use std::io::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use rand::{thread_rng, RngCore};
use futures::prelude::*;
use futures::future::Either;
use tokio::net::{TcpStream, ConnectFuture};
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
    conns_tx: Arc<Mutex<MPMCSender<TcpConnection>>>,
    conns_rx: Arc<Mutex<MPMCReceiver<TcpConnection>>>,
    conn_count: Arc<AtomicUsize>,
    conn_limit: usize,
}

impl Backend {
    pub fn new(addr: SocketAddr, conn_limit: usize) -> Backend {
        let (conns_tx, conns_rx) = mpmc_queue(conn_limit as u64);

        Backend {
            address: addr,
            conns_tx: Arc::new(Mutex::new(conns_tx)),
            conns_rx: Arc::new(Mutex::new(conns_rx)),
            conn_count: Arc::new(AtomicUsize::new(0)),
            conn_limit: conn_limit,
        }
    }

    pub fn subscribe(&self) -> BackendParticipant {
        BackendParticipant {
            address: self.address.clone(),
            conns_tx: self.conns_tx.clone(),
            conns_rx: self.conns_rx.clone(),
            conn_count: self.conn_count.clone(),
            conn_limit: self.conn_limit,
        }
    }
}

type TcpConnection = Either<ExistingTcpStream, ConnectFuture>;

pub struct ExistingTcpStream {
    stream: Option<TcpStream>,
}

impl ExistingTcpStream {
    pub fn from_stream(stream: TcpStream) -> ExistingTcpStream {
        ExistingTcpStream { stream: Some(stream) }
    }
}

impl Future for ExistingTcpStream {
    type Item = TcpStream;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(self.stream.take().unwrap()))
    }
}

/// A placeholder for a caller interested in getting a connection from a backend.
///
/// This wraps the underlying MPMC queue that we use for shuttling connections in and out of the
/// Backend itself in a Stream/Sink compatible footprint.
pub struct BackendParticipant {
    address: SocketAddr,
    conns_tx: Arc<Mutex<MPMCSender<TcpConnection>>>,
    conns_rx: Arc<Mutex<MPMCReceiver<TcpConnection>>>,
    conn_count: Arc<AtomicUsize>,
    conn_limit: usize,
}

impl Stream for BackendParticipant {
    type Item = TcpConnection;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut rx = self.conns_rx.lock().unwrap();
        match rx.try_recv() {
            Ok(conn) => Ok(Async::Ready(Some(conn))),
            Err(TryRecvError::Empty) => {
                let current_conns = self.conn_count.load(Ordering::SeqCst);
                if current_conns < self.conn_limit {
                    let old = self.conn_count.compare_and_swap(current_conns, current_conns + 1, Ordering::SeqCst);
                    if old == current_conns {
                        debug!("[backend] creating new connection to {}", &self.address);
                        return Ok(Async::Ready(Some(Either::B(TcpStream::connect(&self.address)))))
                    }
                }

                Ok(Async::NotReady)
            },
            Err(TryRecvError::Disconnected) => panic!("backend conn sender disconnected!"),
        }
    }
}

impl Sink for BackendParticipant {
    type SinkItem = TcpStream;
    type SinkError = ();

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let mut tx = self.conns_tx.lock().unwrap();
        match tx.try_send(Either::A(ExistingTcpStream::from_stream(item))) {
            Ok(_) => Ok(AsyncSink::Ready),
            Err(TrySendError::Full(_)) => panic!("backend conn queue should not be full via participant!"),
            Err(TrySendError::Disconnected(_)) => panic!("backend conn receiver disconnected!"),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}

pub trait Distributor {
    fn seed(&mut self, backends: Vec<BackendDescriptor>);
    fn choose(&self, point: u64) -> usize;
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

    fn choose(&self, _point: u64) -> usize {
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
            let backend = Backend::new(address.clone(), 4);
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

    pub fn get(&self) -> BackendParticipant {
        let backend_idx = self.distributor.choose(1);
        match self.backends.get(backend_idx) {
            Some(backend) => backend.subscribe(),
            None => unreachable!("incorrect backend idx"),
        }
    }
}
