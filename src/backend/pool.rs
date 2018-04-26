use std::net::SocketAddr;
use rand::{thread_rng, RngCore};
use futures::future;
use futures::future::{Future, Either};
use tokio::net::TcpStream;
use tokio::io::Error;

pub struct BackendDescriptor;

impl BackendDescriptor {
    pub fn new() -> BackendDescriptor {
        BackendDescriptor {
        }
    }
}

pub struct Backend {
    address: SocketAddr,
    conns: Vec<TcpStream>,
}

impl Backend {
    pub fn new(addr: SocketAddr) -> Backend {
        Backend {
            address: addr,
            conns: Vec::new(),
        }
    }

    pub fn get(&mut self) -> impl Future<Item=TcpStream, Error=Error> {
        match self.conns.pop() {
            Some(conn) => Either::B(future::ok::<TcpStream, Error>(conn)),
            None => Either::A(TcpStream::connect(&self.address)),
        }
    }

    pub fn put(&mut self, stream: TcpStream) {
        self.conns.push(stream);
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
    backends: Vec<Backend>,
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
            backends.push(backend);

            // eventually, we'll populate this with weight, etc, so that
            // so can... do weighted things.
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

    pub fn get(&mut self) -> impl Future<Item=TcpStream, Error=Error> {
        let backend_idx = self.distributor.choose(1);
        match self.backends.get_mut(backend_idx) {
            Some(backend) => backend.get(),
            None => unreachable!("incorrect backend idx"),
        }
    }
}
