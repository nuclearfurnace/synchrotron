use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::TryRecvError;
use std::marker::PhantomData;
use rand::{thread_rng, RngCore};
use futures::future;
use futures::future::{Future, Either};
use tokio::net::{TcpStream, ConnectFuture};
use tokio::io::{Error, ErrorKind};
use multiqueue::{mpmc_queue, MPMCSender, MPMCReceiver};

pub struct Backend {
    address: SocketAddr,
    conns_tx: MPMCSender<TcpStream>,
    conns_rx: MPMCReceiver<TcpStream>,
    current_conns: AtomicUsize,
    target_conns: usize,
}

impl Backend {
    pub fn new(conn_limit: usize, addr: SocketAddr) -> Backend {
        let (connections_tx, connections_rx) = mpmc_queue(conn_limit as u64);

        Backend {
            address: addr,
            conns_tx: connections_tx,
            conns_rx: connections_rx,
            current_conns: AtomicUsize::new(0),
            target_conns: conn_limit,
        }
    }

    pub fn get(&mut self) -> impl Future<Item=TcpStream, Error=Error> {
        match self.conns_rx.try_recv() {
            // fast path, no blocking required
            Ok(conn) => Either::B(future::ok::<TcpStream, Error>(conn)),
            Err(val) => match val {
                TryRecvError::Empty => {
                    // buffer is empty, let's check if all the connections have been created yet
                    let current_conns = self.current_conns.load(Ordering::SeqCst);
                    if current_conns < self.target_conns {
                        // try to bump up `self.current_conns`.  if we "win" the incr, it means
                        // we're responsible for creating the connection and adding it to the queue
                        let old_value = self.current_conns.compare_and_swap(current_conns, current_conns + 1, Ordering::SeqCst);
                        if old_value == current_conns {
                            // we won, make the connection, and send it back to the caller.
                            return Either::A(TcpStream::connect(&self.address))
                        }
                    }

                    // slow path, wait for receive
                    let result = self.conns_rx.recv()
                        .map_err(|_| Error::new(ErrorKind::Other, "socket pool connection buffer disconnected!"));
                    Either::B(future::result::<TcpStream, Error>(result))
                },
                TryRecvError::Disconnected => Either::B(future::err::<TcpStream, Error>(Error::new(ErrorKind::Other, "socket pool connection buffer disconnected!"))),
            }
        }
    }

    pub fn put(&mut self, stream: TcpStream) {
        self.conns_tx.try_send(stream).unwrap();
    }
}

pub trait Distributor<'a> {
    fn seed(&mut self, backends: &[&'a Backend]);
    fn choose(&mut self, point: u64) -> &Backend;
}

pub struct RandomDistributor<'a> {
    backends: Vec<&'a Backend>,
}

impl<'a> RandomDistributor<'a> {
    pub fn new() -> RandomDistributor<'a> {
        RandomDistributor {
            backends: vec![],
        }
    }
}

impl<'a> Distributor<'a> for RandomDistributor<'a> {
    fn seed(&mut self, backends: &[&'a Backend]) {
        self.backends.extend_from_slice(backends);
    }

    fn choose(&mut self, point: u64) -> &Backend {
        let mut rng = thread_rng();
        self.backends[rng.next_u64() as usize % self.backends.len()]
    }
}

pub struct BackendPool<'a, D: 'a>
    where D: Distributor<'a>
{
    addresses: Vec<SocketAddr>,
    distributor: D,
    phantom: PhantomData<&'a D>,
    backends: Vec<Backend>,
}

impl<'a, D: 'a> BackendPool<'a, D>
    where D: Distributor<'a>
{
    pub fn new(addresses: Vec<SocketAddr>, dist: D) -> BackendPool<'a, D> {
        let mut backends = vec![];
        for address in &addresses {
            let backend = Backend::new(1, address.clone());
            backends.push(backend);
        }

        BackendPool {
            addresses: addresses,
            backends: backends,
            distributor: dist,
            phantom: PhantomData,
        }
    }
}
