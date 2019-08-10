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
    backend::{processor::Processor, Backend, PoolError},
    common::{EnqueuedRequests, Message},
    conf::PoolConfiguration,
    errors::CreationError,
    util::IntegerMappedVec,
    service::DrivenService,
};
use futures::prelude::*;
use std::task::Poll;
use metrics_runtime::Sink as MetricSink;
use std::collections::{HashMap, VecDeque};
use async_trait::async_trait;

pub struct BackendPool<P>
where
    P: Processor + Clone + Send,
    P::Message: Message + Send,
{
    distributor: Box<dyn Distributor>,
    key_hasher: Box<dyn KeyHasher>,
    backends: Vec<Backend<P>>,
    pending_req: VecDeque<EnqueuedRequests<P::Message>>,
    noreply: bool,
    epoch: u64,
    sink: MetricSink,
}

impl<P> BackendPool<P>
where
    P: Processor + Clone + Send,
    P::Message: Message + Send,
{
    pub fn new(
        backends: Vec<Backend<P>>, distributor: Box<dyn Distributor>, key_hasher: Box<dyn KeyHasher>, noreply: bool,
        sink: MetricSink,
    ) -> BackendPool<P> {
        let mut pool = BackendPool {
            distributor,
            key_hasher,
            backends,
            pending_req: VecDeque::new(),
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

#[async_trait]
impl<P> DrivenService<EnqueuedRequests<P::Message>> for BackendPool<P>
where
    P: Processor + Clone + Send,
    P::Message: Message + Send,
{
    type Response = AssignedResponses<P::Message>;
    type Error = PoolError;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    async fn ready(&mut self) -> Result<(), Self::Error> {
        let mut any_ready = false;
        let mut epoch = 0;
        for backend in &mut self.backends {
            match poll!(backend.ready()) {
                Poll::Ready(Ok(())) => any_ready = true,
                Poll::Pending => {},
            }

            epoch += backend.health().epoch();
        }

        if !any_ready {
            pending!();
        }

        if self.epoch != epoch {
            debug!("regenerating distribution");
            self.regenerate_distribution();
            self.epoch = epoch;
        }

        Ok(())
    }

    async fn drive(&mut self) -> Result<(), Self::Error> {
        // Drive each backend to actually start processing.
        for backend in &mut self.backends {
            backend.drive().await?;
        }

        Ok(())
    }

    fn call(&mut self, req: EnqueuedRequests<P::Message>) -> Self::Future {
        // Dispatch all requests to their respective backends.
        let mut futs = FuturesUnordered::new();

        for req in self.pending_req {
            let mut batches = IntegerMappedVec::new();

            for msg in req {
                let msg_key = msg.key();
                let msg_hashed = self.key_hasher.hash(msg_key);
                let backend_idx = self.distributor.choose(msg_hashed);

                batches.push(backend_idx, msg);
            }

            for (backend_idx, batch) in batches {
                let fut = self.backends[backend_idx].call(batch);
                futs.push(fut);
            }
        }

        futs.collect::<Vec<_>>()
            .map(|xs| xs.flatten())
    }
}

pub struct BackendPoolBuilder<P>
where
    P: Processor + Clone + Send,
    P::Message: Message + Send,
{
    processor: P,
    config: PoolConfiguration,
    noreply: bool,
    sink: MetricSink,
}

impl<P> BackendPoolBuilder<P>
where
    P: Processor + Clone + Send,
    P::Message: Message + Send,
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
        P: Processor + Clone + Send,
        P::Message: Message + Send,
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
