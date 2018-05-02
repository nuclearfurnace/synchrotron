use std::net::SocketAddr;
use std::sync::Arc;
use rand::{thread_rng, RngCore};
use futures::prelude::*;
use tokio::net::TcpStream;
use multiqueue::{mpmc_queue, MPMCSender, MPMCReceiver};
use std::sync::mpsc::{TrySendError, TryRecvError};

pub struct BackendDescriptor;

impl BackendDescriptor {
    pub fn new() -> BackendDescriptor {
        BackendDescriptor {
        }
    }
}

pub struct Backend {
    address: SocketAddr,
    conns_tx: Arc<MPMCSender<TcpStream>>,
    conns_rx: Arc<MPMCReceiver<TcpStream>>,
}

impl Backend {
    pub fn new(addr: SocketAddr) -> Backend {
        let (conns_tx, conns_rx) = mpmc_queue(1);

        Backend {
            address: addr,
            conns_tx: Arc::new(conns_tx),
            conns_rx: Arc::new(conns_rx),
        }
    }

    pub fn subscribe(&self) -> BackendParticipant {
        BackendParticipant {
            conns_tx: self.conns_tx.clone(),
            conns_rx: self.conns_rx.clone(),
        }
    }
}

/// A placeholder for a caller interested in getting a connection from a backend.
///
/// This wraps the underlying MPMC queue that we use for shuttling connections in and out of the
/// Backend itself in a Stream/Sink compatible footprint.
pub struct BackendParticipant {
    conns_tx: Arc<MPMCSender<TcpStream>>,
    conns_rx: Arc<MPMCReceiver<TcpStream>>,
}

impl Stream for BackendParticipant {
    type Item = TcpStream;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // TODO: how do we signal demand (to create new streams, or replenish when old streams die)
        // when we have no coordination point with the parent `Backend`?
        match self.conns_rx.try_recv() {
            Ok(conn) => Ok(Async::Ready(Some(conn))),
            Err(TryRecvError::Empty) => Ok(Async::NotReady),
            Err(TryRecvError::Disconnected) => panic!("backend conn sender disconnected!"),
        }
    }
}

impl Sink for BackendParticipant {
    type SinkItem = TcpStream;
    type SinkError = ();

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self.conns_tx.try_send(item) {
            Ok(_) => Ok(AsyncSink::Ready),
            Err(TrySendError::Full(msg)) => Ok(AsyncSink::NotReady(msg)),
            Err(TrySendError::Disconnected(_)) => panic!("backend conn receiver disconnected!"),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}

pub trait Distributor {
    fn seed(&mut self, backends: Vec<BackendDescriptor>);
    fn choose(&mut self, point: u64) -> usize;
}

pub struct RandomDistributor {
    backends: Vec<BackendDescriptor>,
}

impl RandomDistributor {
    pub fn new() -> RandomDistributor {
        RandomDistributor {
            backends: vec![],
        }
    }
}

impl Distributor for RandomDistributor {
    fn seed(&mut self, backends: Vec<BackendDescriptor>) {
        self.backends = backends;
    }

    fn choose(&mut self, _point: u64) -> usize {
        let mut rng = thread_rng();
        rng.next_u64() as usize % self.backends.len()
    }
}

pub struct BackendPool<D>
    where D: Distributor
{
    addresses: Vec<SocketAddr>,
    distributor: D,
    backends: Vec<Arc<Backend>>,
}

impl<D> BackendPool<D>
    where D: Distributor
{
    pub fn new(addresses: Vec<SocketAddr>, mut dist: D) -> BackendPool<D> {
        // Assemble the list of backends and backend descriptors.
        let mut backends = vec![];
        let mut descriptors = vec![];
        for address in &addresses {
            let backend = Backend::new(address.clone());
            backends.push(Arc::new(backend));

            // eventually, we'll populate this with weight, etc, so that
            // we can actually do weighted things.
            let descriptor = BackendDescriptor::new();
            descriptors.push(descriptor);
        }

        // Seed the distributor.
        dist.seed(descriptors);

        BackendPool {
            addresses: addresses,
            backends: backends,
            distributor: dist,
        }
    }

    pub fn get(&mut self) -> BackendParticipant {
        let backend_idx = self.distributor.choose(1);
        match self.backends.get(backend_idx) {
            Some(backend) => backend.subscribe(),
            None => unreachable!("incorrect backend idx"),
        }
    }
}
