// Copyright (c) 2018 Nuclear Furnace
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use super::{
    distributor::{configure_distributor, Distributor},
    hasher::{configure_hasher, KeyHasher},
};
use crate::{
    backend::{processor::Processor, Backend},
    common::{EnqueuedRequests, Message, Responses, GenericError},
    conf::PoolConfiguration,
    errors::CreationError,
    service::DrivenService,
    util::IntegerMappedVec,
};
use futures::prelude::*;
use futures::stream::FuturesOrdered;
use metrics_runtime::Sink as MetricSink;
use std::{
    collections::HashMap,
    task::{Context, Poll},
};

pub struct BackendPool<P>
where
    P: Processor + Clone + Send + Sync,
    P::Message: Message + Send + Sync,
{
    distributor: Box<dyn Distributor>,
    key_hasher: Box<dyn KeyHasher>,
    backends: Vec<Backend<P>>,
    noreply: bool,
    epoch: u64,
    sink: MetricSink,
}

impl<P> BackendPool<P>
where
    P: Processor + Clone + Send + Sync,
    P::Message: Message + Send + Sync,
{
    pub fn new(
        backends: Vec<Backend<P>>,
        distributor: Box<dyn Distributor>,
        key_hasher: Box<dyn KeyHasher>,
        noreply: bool,
        sink: MetricSink,
    ) -> BackendPool<P> {
        let mut pool = BackendPool {
            distributor,
            key_hasher,
            backends,
            noreply,
            epoch: 0,
            sink,
        };
        pool.regenerate_distribution();
        pool
    }

    pub fn regenerate_distribution(&mut self) {
        let descriptors = self
            .backends
            .iter_mut()
            .enumerate()
            .map(|(idx, backend)| {
                let mut descriptor = backend.get_descriptor();
                descriptor.idx = idx;
                descriptor
            })
            .filter(|backend| backend.healthy)
            .collect();
        self.distributor.update(descriptors);
        self.sink.record_counter("distribution_updated", 1);
    }
}

impl<P> DrivenService<EnqueuedRequests<P::Message>> for BackendPool<P>
where
    P: Processor + Clone + Send + Sync,
    P::Message: Message + Send + Sync + 'static,
{
    type Error = GenericError;
    type Future = Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync + Unpin>;
    type Response = Responses<P::Message>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut any_ready = false;
        let mut epoch = 0;
        for backend in &mut self.backends {
            match backend.poll_ready(cx) {
                Poll::Ready(Ok(())) => any_ready = true,
                // Catch everything else.  We don't immediately return an error because
                // we need to be able to update the state of the pool.
                //
                // TODO: should we return an error if no backends are ready?
                _ => {},
            }

            epoch += backend.health().epoch();
        }

        if !any_ready {
            return Poll::Pending
        }

        if self.epoch != epoch {
            debug!("regenerating distribution");
            self.regenerate_distribution();
            self.epoch = epoch;
        }

        Poll::Ready(Ok(()))
    }

    fn poll_service(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut any_pending = false;
        for backend in &mut self.backends {
            match backend.poll_service(cx) {
                Poll::Pending => any_pending = true,
                Poll::Ready(Ok(())) => {},
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }

        if any_pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut any_pending = false;
        for backend in &mut self.backends {
            match backend.poll_close(cx) {
                Poll::Pending => any_pending = true,
                Poll::Ready(Ok(())) => {},
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }

        if any_pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, msgs: EnqueuedRequests<P::Message>) -> Self::Future {
        // Dispatch all requests to their respective backends.
        let mut futs = FuturesOrdered::new();
        let mut batches = IntegerMappedVec::new();

        for msg in msgs {
            let msg_key = msg.key();
            let msg_hashed = self.key_hasher.hash(msg_key);
            let backend_idx = self.distributor.choose(msg_hashed);

            batches.push(backend_idx, msg);
        }

        for (backend_idx, batch) in batches {
            let fut = self.backends[backend_idx].call(batch);
            futs.push(fut);
        }

        Box::new(futs.collect::<Vec<_>>().map(|xs| {
            let mut responses = Vec::new();
            for x in xs {
                match x {
                    Ok(resp) => responses.extend(resp),
                    Err(e) => return Err(e),
                }
            }
            Ok(responses)
        }))
    }
}

pub struct BackendPoolBuilder<P>
where
    P: Processor + Clone + Send + Sync,
    P::Message: Message + Send + Sync,
{
    processor: P,
    config: PoolConfiguration,
    noreply: bool,
    sink: MetricSink,
}

impl<P> BackendPoolBuilder<P>
where
    P: Processor + Clone + Send + Sync,
    P::Message: Message + Send + Sync,
{
    pub fn new(name: String, processor: P, config: PoolConfiguration, mut sink: MetricSink) -> BackendPoolBuilder<P> {
        sink.add_default_labels(&[("pool", name)]);

        BackendPoolBuilder {
            processor,
            config,
            noreply: false,
            sink,
        }
    }

    pub fn set_noreply(mut self, noreply: bool) -> Self {
        self.noreply = noreply;
        self
    }

    pub fn build(self) -> Result<BackendPool<P>, CreationError>
    where
        P: Processor + Clone + Send + Sync,
        P::Message: Message + Send + Sync,
    {
        let mut options = self.config.options.unwrap_or_else(HashMap::new);
        let dist_type = options
            .entry("distribution".to_owned())
            .or_insert_with(|| "modulo".to_owned())
            .to_lowercase();
        let distributor = configure_distributor(&dist_type)?;
        debug!("[listener] using distributor '{}'", dist_type);

        let hash_type = options
            .entry("hash".to_owned())
            .or_insert_with(|| "fnv1a_64".to_owned())
            .to_lowercase();
        let hasher = configure_hasher(&hash_type)?;
        debug!("[listener] using hasher '{}'", hash_type);

        // Build all of our backends for this pool.
        let mut backends = Vec::new();
        for address in &self.config.addresses {
            let backend = Backend::new(
                address.address,
                address.identifier.clone(),
                self.processor.clone(),
                options.clone(),
                self.noreply,
                self.sink.clone(),
            )?;
            backends.push(backend);
        }

        Ok(BackendPool::new(backends, distributor, hasher, self.noreply, self.sink))
    }
}
