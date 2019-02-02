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
use super::{distributor::{Distributor, configure_distributor}, hasher::{KeyHasher, configure_hasher}};
use backend::{processor::Processor, PoolError, Backend, ResponseFuture};
use common::{AssignedRequests, AssignedResponses, Message};
use futures::prelude::*;
use tower_direct_service::DirectService;
use conf::PoolConfiguration;
use errors::CreationError;
use std::collections::HashMap;
use hotmic::Sink as MetricSink;

type DistributorFutureSafe = Box<Distributor + Send + 'static>;
type KeyHasherFutureSafe = Box<KeyHasher + Send + 'static>;

pub struct BackendPool<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    distributor: DistributorFutureSafe,
    key_hasher: KeyHasherFutureSafe,
    backends: Vec<Backend<P>>,
    sink: MetricSink<&'static str>,
}

impl<P> BackendPool<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    pub fn new(backends: Vec<Backend<P>>, distributor: DistributorFutureSafe, key_hasher: KeyHasherFutureSafe, sink: MetricSink<&'static str>) -> BackendPool<P> {
        let pool = BackendPool {
            distributor,
            key_hasher,
            backends,
            sink,
        };
        pool.regenerate_distribution();
        pool
    }

    pub fn regenerate_distribution(&mut self) {
        let descriptors = self.backends.iter()
            .enumerate()
            .map(|(idx, backend)| {
                let descriptor = backend.get_descriptor();
                descriptor.idx = idx;
                descriptor
            })
            .collect();
        self.distributor.update(descriptors);
    }
}

impl<P> DirectService<AssignedRequests<P::Message>> for BackendPool<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    type Error = PoolError;
    type Future = ResponseFuture<P, Self::Error>;
    type Response = AssignedResponses<P::Message>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        // Not every backend will be ready all the time, especially if they're knocked out of the
        // pool temporarily, but as long as one is ready, then we're ready.  If any of them are in
        // a bad enough state to throw an error, though, then something is very wrong and we need
        // to bubble that up.
        let mut any_ready = false;
        let mut any_not_ready = false;
        for backend in self.backends {
            match backend.poll_ready() {
                Ok(Async::Ready(_)) => any_ready = true,
                Ok(Async::NotReady) => any_not_ready = true,
                Err(e) => return Err(PoolError::Backend(e)),
            }
        }

        // all backends are not ready, so fail fast
        if any_not_ready && !any_ready {
            return Ok(Async::NotReady)
        }

        // We have some backends that are ready and some that are not, which likely means our
        // distribution needs to change.  Do that now.
        // TODO: this is going to regenerate the distribution every single time our poll_ready
        // status is mixed, which could happen _a lot_ while any given machine is down, so we
        // should explore some sort of epoch-based approach... track the highest observed value of
        // a global counter, or something, and when we're mixed mode _and_ the counter differs from
        // the last time we regenerated the distribution, only then regenerate it again.
        if any_not_ready {
            self.regenerate_distribution();
        }

        Ok(Async::Ready(()))
    }

    fn poll_service(&mut self) -> Poll<(), Self::Error> {
        for backend in self.backends {
            // not clear if it actually makes sense to pre-emptively return notready without
            // driving all services.. poll_ready should cover the "am i knocked out of the pool
            // temporarily?" case but would this ever actually return notready when driving the
            // underlying service? unclear
            let _ = try_ready!(backend.poll_service());
        }

        Ok(Async::Ready(()))
    }

    fn poll_close(&mut self) -> Poll<(), Self::Error> {
        for backend in self.backends {
            let _ = try_ready!(backend.poll_close());
        }

        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: AssignedRequests<P::Message>) -> Self::Future {
        let mut futs = Vec::new();

        // can we do better than a normal hashmap here? maybe an indexmap
        let mut batches = HashMap::new();

        for (id, msg) in req {
            let msg_key = msg.key();
            let msg_hashed = self.key_hasher.hash(msg_key);
            let backend_idx = self.distributor.choose(msg_hashed);

            let batch = batches.entry(backend_idx).or_insert_with(Vec::new);
            batch.push(msg);
        }

        // make the batch calls to each relevant backend, and collect them
        for (backend_idx, batch) in batches {
            let fut = self.backends[backend_idx].call(batch);
            futs.extend(fut);
        }

        ResponseFuture::new(futs)
    }
}

pub struct BackendPoolBuilder<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    processor: P,
    config: PoolConfiguration,
    noreply: bool,
    sink: MetricSink<&'static str>,
}

impl<P> BackendPoolBuilder<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    pub fn new(name: String, processor: P, config: PoolConfiguration, sink: MetricSink<&'static str>) -> BackendPoolBuilder<P> {
        let sink = sink.scoped(&["pools", &name]);

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
        P: Processor + Clone + Send + 'static,
        P::Message: Message + Send + 'static,
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
            let backend = Backend::new(*address, self.processor.clone(), self.config.clone(), self.noreply)?;
            backends.push(backend);
        }

        Ok(BackendPool::new(backends, distributor, hasher, self.sink))
    }
}
