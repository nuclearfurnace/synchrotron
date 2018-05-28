use std::io::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use rand::{thread_rng, RngCore};
use futures::prelude::*;
use futures::future::Either;
use tokio::net::{TcpStream, ConnectFuture};
use crypto::md5::Md5;
use crypto::digest::Digest;

pub struct BackendDescriptor;

impl BackendDescriptor {
    pub fn new() -> BackendDescriptor {
        BackendDescriptor {
        }
    }
}

pub struct Backend {
    address: SocketAddr,
    conns: Arc<Mutex<Vec<TcpConnection>>>,
    conn_count: Arc<AtomicUsize>,
    conn_limit: usize,
}

impl Backend {
    pub fn new(addr: SocketAddr, conn_limit: usize) -> Backend {
        Backend {
            address: addr,
            conns: Arc::new(Mutex::new(Vec::new())),
            conn_count: Arc::new(AtomicUsize::new(0)),
            conn_limit: conn_limit,
        }
    }

    pub fn subscribe(&self) -> BackendParticipant {
        BackendParticipant {
            address: self.address.clone(),
            conns: self.conns.clone(),
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
    conns: Arc<Mutex<Vec<TcpConnection>>>,
    conn_count: Arc<AtomicUsize>,
    conn_limit: usize,
}

impl Stream for BackendParticipant {
    type Item = TcpConnection;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut conns = self.conns.lock().unwrap();
        match conns.pop() {
            Some(conn) => Ok(Async::Ready(Some(conn))),
            None => {
                let current_conns = self.conn_count.load(Ordering::SeqCst);
                if current_conns < self.conn_limit {
                    let old = self.conn_count.compare_and_swap(current_conns, current_conns + 1, Ordering::SeqCst);
                    if old == current_conns {
                        debug!("[backend] creating new connection to {}", &self.address);
                        return Ok(Async::Ready(Some(Either::B(TcpStream::connect(&self.address)))))
                    }
                }

                Ok(Async::NotReady)
            }
        }
    }
}

impl Sink for BackendParticipant {
    type SinkItem = TcpStream;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let mut conns = self.conns.lock().unwrap();
        conns.push(Either::A(ExistingTcpStream::from_stream(item)));
        Ok(AsyncSink::Ready)
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

pub trait Hasher {
    fn hash(&self, buf: &[u8]) -> u64;
}

pub struct MD5Hasher;

impl MD5Hasher {
    pub fn new() -> MD5Hasher {
        MD5Hasher {
        }
    }
}

impl Hasher for MD5Hasher {
    fn hash(&self, buf: &[u8]) -> u64 {
        let mut hasher = Md5::new();
        hasher.input(buf);

        let mut result = [0; 16];
        hasher.result(&mut result);

        (((result[3] as u32) << 24) + ((result[2] as u32) << 16) + ((result[1] as u32) << 8) + result[0] as u32) as u64
    }
}

pub struct BackendPool<D, H>
    where D: Distributor, H: Hasher
{
    distributor: D,
    hasher: H,
    backends: Vec<Arc<Backend>>,
}

impl<D, H> BackendPool<D, H>
    where D: Distributor, H: Hasher
{
    pub fn new(addresses: Vec<SocketAddr>, mut dist: D, hasher: H) -> BackendPool<D, H> {
        // Assemble the list of backends and backend descriptors.
        let mut backends = vec![];
        let mut descriptors = vec![];
        for address in &addresses {
            let backend = Backend::new(address.clone(), 1);
            backends.push(Arc::new(backend));

            // eventually, we'll populate this with weight, etc, so that
            // we can actually do weighted things.
            let descriptor = BackendDescriptor::new();
            descriptors.push(descriptor);
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

    pub fn get_backend_by_index(&self, idx: usize) -> BackendParticipant {
        match self.backends.get(idx) {
            Some(backend) => backend.subscribe(),
            None => unreachable!("incorrect backend idx"),
        }
    }
}
