use super::distributor::{BackendDescriptor, Distributor};
use super::hasher::Hasher;
use futures::prelude::*;
use futures::future::{ok, result};
use std::io::Error;
use std::net::SocketAddr;
use tokio;
use tokio::net::TcpStream;
use super::sync::{
    MutexBackend, MutexBackendParticipant, MutexBackendConnection,
    TaskBackend, TaskBackendParticipant, TaskBackendConnection,
};

pub struct BackendPool<D, H, R, V> {
    distributor: D,
    hasher: H,
    backends: Vec<TaskBackend<R, V>>,
}

impl<D, H, R, R2, V> BackendPool<D, H, R, V>
    where D: Distributor,
          H: Hasher,
          R: FnOnce(TcpStream) -> R2,
          R2: Future<Item = (TcpStream, V), Error = Error> + 'static
{
    pub fn new(addresses: Vec<SocketAddr>, mut dist: D, hasher: H) -> BackendPool<D, H, R, V> {
        // Assemble the list of backends and backend descriptors.
        let mut backends = vec![];
        let mut descriptors = vec![];
        for address in &addresses {
            let (backend, runner) = TaskBackend::new(address.clone(), 1);
            backends.push(backend);

            // eventually, we'll populate this with weight, etc, so that
            // we can actually do weighted things.
            let descriptor = BackendDescriptor::new();
            descriptors.push(descriptor);

            // Spawn our backend runner.
            tokio::spawn(runner);
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

    pub fn get_backend_by_index(&self, idx: usize) -> TaskBackendParticipant<R, V> {
        match self.backends.get(idx) {
            Some(backend) => backend.subscribe(),
            None => unreachable!("incorrect backend idx"),
        }
    }
}

pub fn run_operation_on_mutex_backend<F, F2, U>(
    backend: MutexBackendParticipant,
    f: F,
) -> impl Future<Item = U, Error = Error>
where
    F: FnOnce(TcpStream) -> F2,
    F2: Future<Item = (TcpStream, U), Error = Error> + 'static,
{
    let (backend_tx, backend_rx) = backend.split();
    backend_rx
        .into_future()
        .map_err(|(err, _)| err)
        .and_then(|(server, _)| server.unwrap())
        .and_then(move |server| f(server))
        .then(|result| match result {
            Ok((server, x)) => ok((MutexBackendConnection::Alive(server), Ok(x))),
            Err(e) => ok((MutexBackendConnection::Error, Err(e))),
        })
        .and_then(move |(backend_result, op_result)| {
            backend_tx
                .send(backend_result)
                .join(result(op_result))
                .map(|(_, result)| result)
        })
}

pub fn run_operation_on_task_backend<R, R2, V>(
    backend: TaskBackendParticipant<R, V>,
    f: R,
) -> impl Future<Item = V, Error = Error>
where
    R: FnOnce(TcpStream) -> R2,
    R2: Future<Item = (TcpStream, V), Error = Error> + 'static,
{
    backend.submit(f)
        .map(|r| result(r))
}
