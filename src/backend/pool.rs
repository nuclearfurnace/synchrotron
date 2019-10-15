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
    service::{DrivenService, Facade},
    util::IntegerMappedVec,
};
use std::marker::PhantomData;
use futures::prelude::*;
use futures::stream::FuturesOrdered;
use metrics_runtime::Sink as MetricSink;
use tokio_executor::DefaultExecutor;
use tokio::io::{AsyncRead, AsyncWrite};
use std::{
    collections::HashMap,
    task::{Context, Poll},
};

type BufferedPool<P, RW, T> = Facade<BackendPool<P, RW>, EnqueuedRequests<T>>;

pub struct BackendPool<P, RW>
where
    P: Processor<RW> + Clone + Send + Sync,
    P::Message: Message + Send,
    RW: AsyncRead + AsyncWrite + Send + 'static,
{
    distributor: Box<dyn Distributor>,
    key_hasher: Box<dyn KeyHasher>,
    backends: Vec<Backend<P, RW>>,
    epoch: u64,
    sink: MetricSink,
}

impl<P, RW> BackendPool<P, RW>
where
    P: Processor<RW> + Clone + Send + Sync,
    P::Message: Message + Send,
    RW: AsyncRead + AsyncWrite + Send + 'static,
{
    pub fn new(
        backends: Vec<Backend<P, RW>>,
        distributor: Box<dyn Distributor>,
        key_hasher: Box<dyn KeyHasher>,
        sink: MetricSink,
    ) -> BackendPool<P, RW> {
        BackendPool {
            distributor,
            key_hasher,
            backends,
            epoch: 0,
            sink,
        }
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
            .collect::<Vec<_>>();

        let descriptors_len = descriptors.len();
        self.distributor.update(descriptors);

        tracing::debug!(message = "regenerating distribution", node_count = descriptors_len);
        self.sink.record_counter("distribution_updated", 1);
    }
}

impl<P, RW> DrivenService<EnqueuedRequests<P::Message>> for BackendPool<P, RW>
where
    P: Processor<RW> + Clone + Send + Sync,
    P::Message: Message + Send + 'static,
    RW: AsyncRead + AsyncWrite + Send + 'static,
{
    type Error = GenericError;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;
    //type Future = Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Unpin>;
    type Response = Responses<P::Message>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut any_ready = false;
        let mut epoch = 0;
        for backend in &mut self.backends {
            let name = backend.get_descriptor().identifier;

            // We don't bother with the other states because we're effectively shunting errors.
            if let Poll::Ready(Ok(())) = backend.poll_ready(cx) {
                tracing::debug!(message = "backend is ready", %name);
                any_ready = true
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
        tracing::debug!("calling backends for fragmented requests");

        // Dispatch all requests to their respective backends.
        let mut futs = FuturesOrdered::new();
        let mut batches = IntegerMappedVec::new();

        for msg in msgs {
            let msg_key = msg.key();
            let msg_hashed = self.key_hasher.hash(msg_key);
            let backend_idx = self.distributor.choose(msg_hashed);
            tracing::debug!(message = "assigning to batch", backend_idx, ?msg_key);

            batches.push(backend_idx, msg);
        }

        for (backend_idx, batch) in batches {
            let fut = self.backends[backend_idx].call(batch);
            futs.push(fut);
        }

        futs
            .collect::<Vec<_>>()
            .map(|xs| {
                let mut responses = Vec::new();
                for x in xs {
                    responses.extend(x?);
                }
                Ok(responses)
            })
    }
}

pub struct BackendPoolBuilder<P, RW> {
    name: String,
    processor: P,
    _rw: PhantomData<RW>,
    config: PoolConfiguration,
    noreply: bool,
    sink: MetricSink,
}

impl<P, RW> Clone for BackendPoolBuilder<P, RW>
where
    P: Clone,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            processor: self.processor.clone(),
            _rw: PhantomData,
            config: self.config.clone(),
            noreply: self.noreply,
            sink: self.sink.clone(),
        }
    }
}

impl<P, RW> BackendPoolBuilder<P, RW>
where
    P: Processor<RW> + Clone + Send + Sync + 'static,
    P::Message: Message + Send + 'static,
    RW: AsyncRead + AsyncWrite + Send + 'static,
{
    pub fn new(name: String, processor: P, config: PoolConfiguration, mut sink: MetricSink) -> BackendPoolBuilder<P, RW> {
        sink.add_default_labels(&[("pool", name.clone())]);

        BackendPoolBuilder {
            name,
            processor,
            _rw: PhantomData,
            config,
            noreply: false,
            sink,
        }
    }

    pub fn set_noreply(mut self, noreply: bool) -> Self {
        self.noreply = noreply;
        self
    }

    pub fn build(self) -> Result<BackendPool<P, RW>, CreationError> {
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

        let sink = self.sink;
        let backend_len = backends.len();
        tracing::debug!(message = "pool created", backend_len);

        Ok(BackendPool::new(backends, distributor, hasher, sink))
    }

    pub fn build_buffered(self) -> Result<BufferedPool<P, RW, P::Message>, CreationError> {
        let name = self.name.clone();
        let pool = self.build()?;
        Facade::new(pool, DefaultExecutor::current(), 32).map_err(|_| {
            CreationError::InvalidResource(format!(
                "error while building pool '{}': failed to spawn task",
                name
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::BackendPoolBuilder;
    use crate::conf::{BackendAddress, PoolConfiguration};
    use crate::common::EnqueuedRequest;
    use crate::backend::processor::MockProcessor;
    use crate::protocol::mock::MockMessage;
    use crate::util::MockStream;
    use crate::service::DrivenService;
    use std::net::SocketAddr;
    use metrics_runtime::Receiver;
    use tokio_test::{clock::mock as clock_mock, task::MockTask};
    use std::sync::Arc;

    #[test]
    #[should_panic]
    fn test_backend_pool_panics_without_poll_ready() {
        let processor = MockProcessor::<MockStream>::new();
        let processor = Arc::new(processor);

        let address = SocketAddr::new([127, 0, 0, 1].into(), 12345);
        let config = PoolConfiguration {
            addresses: vec![BackendAddress { address, identifier: "backend".to_owned() }],
            options: None,
        };
        let receiver = Receiver::builder().build().unwrap();
        let sink = receiver.get_sink();

        let builder = BackendPoolBuilder::new("mock".to_owned(), processor, config, sink);
        let mut pool = builder.build().unwrap();

        let er = EnqueuedRequest::new(MockMessage::Empty);

        let _ = pool.call(vec![er]);
    }

    #[test]
    fn test_backend_pool() {
        let processor = MockProcessor::<MockStream>::new();
        let processor = Arc::new(processor);

        let address = SocketAddr::new([127, 0, 0, 1].into(), 12345);
        let config = PoolConfiguration {
            addresses: vec![BackendAddress { address, identifier: "backend".to_owned() }],
            options: None,
        };
        let receiver = Receiver::builder().build().unwrap();
        let sink = receiver.get_sink();

        let builder = BackendPoolBuilder::new("mock".to_owned(), processor, config, sink);
        let mut pool = builder.build().unwrap();

        let req = MockMessage::from_data(&b"set"[..], &b"foo"[..], Some(&b"bar"[..]));
        let ereq = EnqueuedRequest::new(req);

        clock_mock(|handle| {
            let mut task = MockTask::new();
            let result = task.enter(|cx| pool.poll_ready(cx));
            assert!(result.is_ready());
        });

        let _ = pool.call(vec![ereq]);
    }
}
