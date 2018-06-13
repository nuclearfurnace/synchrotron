use super::distributor::{BackendDescriptor, Distributor};
use super::hasher::KeyHasher;
use backend::sync::RequestTransformer;
use backend::sync::{TaskBackend, TaskBackendParticipant};
use std::net::SocketAddr;
use tokio::runtime::TaskExecutor;
use futures::prelude::*;
use tokio::net::TcpStream;
use std::io::Error;

pub struct BackendPool<T>
where
    T: RequestTransformer,
{
    distributor: Box<Distributor + Send + Sync>,
    hasher: Box<KeyHasher + Send + Sync>,
    backends: Vec<TaskBackend<T>>,
}

impl<T> BackendPool<T>
where
    T: RequestTransformer + Clone + Send + 'static,
    T::Request: Send + 'static,
    T::Response: Send + 'static,
    T::Executor: Future<Item = (TcpStream, T::Response), Error = Error> + Send + 'static,
{
    pub fn new(
        executor: TaskExecutor,
        addresses: Vec<SocketAddr>,
        transformer: T,
        mut dist: Box<Distributor + Send + Sync>,
        hasher: Box<KeyHasher + Send + Sync>,
    ) -> BackendPool<T> {
        // Assemble the list of backends and backend descriptors.
        let mut backends = vec![];
        let mut descriptors = vec![];
        for address in &addresses {
            let (backend, runner) = TaskBackend::new(executor.clone(), address.clone(), transformer.clone(), 1);
            backends.push(backend);

            // eventually, we'll populate this with weight, etc, so that
            // we can actually do weighted things.
            let descriptor = BackendDescriptor::new();
            descriptors.push(descriptor);

            // Spawn our backend runner.
            executor.spawn(runner);
        }

        // Seed the distributor.
        dist.seed(descriptors);

        BackendPool {
            backends: backends,
            distributor: dist,
            hasher: hasher,
        }
    }

    pub fn get_backend_index(&self, key: &[u8]) -> usize {
        let key_id = self.hasher.hash(key);
        self.distributor.choose(key_id)
    }

    pub fn get_backend_by_index(&self, idx: usize) -> TaskBackendParticipant<T> {
        match self.backends.get(idx) {
            Some(backend) => backend.subscribe(),
            None => unreachable!("incorrect backend idx"),
        }
    }
}
