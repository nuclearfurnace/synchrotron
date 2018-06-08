use std::collections::VecDeque;
use std::io::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use futures::future::Either;
use futures::task;
use futures::task::Task;
use futures::prelude::*;
use tokio::net::{TcpStream, ConnectFuture};

pub struct MutexBackendState {
    conns: Vec<MutexTcpConnection>,
    waiters: VecDeque<Task>,
}

impl MutexBackendState {
    pub fn new() -> MutexBackendState {
        MutexBackendState {
            conns: Vec::new(),
            waiters: VecDeque::new(),
        }
    }
}

pub enum MutexBackendConnection {
    Alive(TcpStream),
    Error,
}

/// Managed connections to a backend server.
///
/// This will maintain a specific number of connections to the backend, giving them out and
/// reclaiming them during normal operation.
pub struct MutexBackend {
    address: SocketAddr,
    state: Arc<Mutex<MutexBackendState>>,
    conn_count: Arc<AtomicUsize>,
    conn_limit: usize,
}

impl MutexBackend {
    pub fn new(addr: SocketAddr, conn_limit: usize) -> MutexBackend {
        MutexBackend {
            address: addr,
            state: Arc::new(Mutex::new(MutexBackendState::new())),
            conn_count: Arc::new(AtomicUsize::new(0)),
            conn_limit: conn_limit,
        }
    }

    pub fn subscribe(&self) -> MutexBackendParticipant {
        MutexBackendParticipant {
            address: self.address.clone(),
            state: self.state.clone(),
            conn_count: self.conn_count.clone(),
            conn_limit: self.conn_limit,
        }
    }
}

type MutexTcpConnection = Either<MutexExistingTcpStream, ConnectFuture>;

pub struct MutexExistingTcpStream {
    stream: Option<TcpStream>,
}

impl MutexExistingTcpStream {
    pub fn from_stream(stream: TcpStream) -> MutexExistingTcpStream {
        MutexExistingTcpStream {
            stream: Some(stream),
        }
    }
}

impl Future for MutexExistingTcpStream {
    type Item = TcpStream;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(self.stream.take().unwrap()))
    }
}

/// A placeholder for a caller interested in getting a connection from a backend.
///
/// This provides a Stream/Sink interface to the underlying queue of backend connections so that
/// getting a connection, and returning it, can be easily composed.
pub struct MutexBackendParticipant {
    address: SocketAddr,
    state: Arc<Mutex<MutexBackendState>>,
    conn_count: Arc<AtomicUsize>,
    conn_limit: usize,
}

impl Stream for MutexBackendParticipant {
    type Item = MutexTcpConnection;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        debug!("[mutex backend] checking queue for existing connections");
        let mut state = self.state.lock().unwrap();
        match state.conns.pop() {
            Some(conn) => {
                debug!("[mutex backend] got connection from queue directly");
                Ok(Async::Ready(Some(conn)))
            }
            None => {
                let current_conns = self.conn_count.load(Ordering::SeqCst);
                if current_conns < self.conn_limit {
                    debug!("[mutex backend] open connection slot, trying to reserve it...");
                    let old = self.conn_count.compare_and_swap(
                        current_conns,
                        current_conns + 1,
                        Ordering::SeqCst,
                    );
                    if old == current_conns {
                        debug!("[mutex backend] creating new connection to {}", &self.address);
                        return Ok(Async::Ready(Some(Either::B(TcpStream::connect(
                            &self.address,
                        )))));
                    }
                }

                // If we have no connection to give, store the task so we can notify them when we
                // are given back a connection.
                debug!("[mutex backend] not ready to give connection");
                let waiter = task::current();
                state.waiters.push_back(waiter);

                Ok(Async::NotReady)
            }
        }
    }
}

impl Sink for MutexBackendParticipant {
    type SinkItem = MutexBackendConnection;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let mut state = self.state.lock().unwrap();

        match item {
            MutexBackendConnection::Alive(stream) => {
                debug!("[backend] sending connection back into queue");
                state
                    .conns
                    .push(Either::A(MutexExistingTcpStream::from_stream(stream)));
            }
            MutexBackendConnection::Error => {
                debug!("[backend] error with connection; freeing slot in queue");
                self.conn_count.fetch_sub(1, Ordering::SeqCst);
            }
        };

        if state.waiters.len() > 0 {
            match state.waiters.pop_front() {
                Some(waiter) => waiter.notify(),
                None => {},
            }
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}
