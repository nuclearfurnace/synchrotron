use backend::sync::RequestTransformer;
use futures::prelude::*;
use futures::sync::{mpsc, oneshot};
use futures::future::{ok, Either};
use std::io::Error;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::runtime::TaskExecutor;

pub enum TaskBackendConnection {
    Alive(TcpStream),
    Error,
}

/// A state machine that drives the pooling of backend connections and the requests that require
/// them.
///
/// Rather than using explicit synchronization, all connections and work requests flow to this
/// state machine via channels, and this future must be launched as an independent task when a new
/// backend is created.
///
/// There is an implicit requirement that a backend be created with a sibling state machine, and
/// each given the appropriate tx/rx sides of a channel that allows them to communicate with each
/// other.
pub struct TaskBackendStateMachine<T>
where
    T: RequestTransformer,
{
    executor: TaskExecutor,

    transformer: T,
    requests_rx: mpsc::UnboundedReceiver<(T::Request, oneshot::Sender<Result<T::Response, Error>>)>,

    address: SocketAddr,
    conns: Vec<TcpStream>,
    conns_rx: mpsc::UnboundedReceiver<TaskBackendConnection>,
    conns_tx: mpsc::UnboundedSender<TaskBackendConnection>,
    conn_count: usize,
    conn_limit: usize,
}

impl<T> Future for TaskBackendStateMachine<T>
where
    T: RequestTransformer,
    T::Request: Send + 'static,
    T::Response: Send + 'static,
    T::Executor: Future<Item = (TcpStream, T::Response), Error = Error> + Send + 'static,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            // See if we have a connection to recover or replace.
            //
            match self.conns_rx.poll() {
                Ok(Async::Ready(Some(conn))) => match conn {
                    TaskBackendConnection::Alive(conn) => self.conns.push(conn),
                    TaskBackendConnection::Error => {
                        self.conn_count -= 1;
                    }
                },
                Ok(Async::NotReady) => {},
                _ => return Err(()),
            }

            // Go through every request we have pending to us, and spin them up.
            match self.requests_rx.poll() {
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                }
                Ok(Async::Ready(Some((request, response_tx)))) => {
                    // If all connections are currently busy, we can't do anything else.
                    if self.conns.len() == 0 && self.conn_count == self.conn_limit {
                        return Ok(Async::NotReady);
                    }

                    // We have a new request.  Grab a connection and attempt to service it.
                    let connection = match self.conns.len() {
                        0 => {
                            self.conn_count += 1;
                            Either::B(TcpStream::connect(&self.address))
                        }
                        _ => self
                            .conns
                            .pop()
                            .map(|x| Either::A(ok(x)))
                            .unwrap(),
                    };

                    let conns_tx = self.conns_tx.clone();
                    let inner_work = self.transformer.transform(request, connection);
                    let work = inner_work
                        .then(|result| match result {
                            Ok((conn, x)) => ok((TaskBackendConnection::Alive(conn), Ok(x))),
                            Err(e) => ok((TaskBackendConnection::Error, Err(e))),
                        })
                        .and_then(move |(backend_result, op_result)| {
                            conns_tx.send(backend_result).map(|_| op_result)
                        })
                        .and_then(move |result| ok(response_tx.send(result)))
                        .map_err(|_| ())
                        .map(|_| ());

                    self.executor.spawn(work);
                }
                _ => return Err(()),
            }
        }
    }
}

fn new_state_machine<T>(
    executor: TaskExecutor,
    addr: SocketAddr,
    transformer: T,
    rx: mpsc::UnboundedReceiver<(T::Request, oneshot::Sender<Result<T::Response, Error>>)>,
    conn_limit: usize,
) -> TaskBackendStateMachine<T>
where
    T: RequestTransformer,
{
    let (conns_tx, conns_rx) = mpsc::unbounded();

    TaskBackendStateMachine {
        executor: executor,

        transformer: transformer,
        requests_rx: rx,

        address: addr,
        conns: Vec::new(),
        conns_rx: conns_rx,
        conns_tx: conns_tx,
        conn_count: 0,
        conn_limit: conn_limit,
    }
}

/// Managed connections to a backend server.
///
/// This backend is serviced by a Tokio task, which processes all work requests to backend servers,
/// and the connections that constitute this backend server.
///
/// Backends are, in essence, proxy objects to their respective Tokio task, which is doing the
/// actual heavy lifting.  They exist purely to hand out participant placeholders, which allow
/// owned copies of senders for communicating with the backend task.
///
/// Backends maintain a given number of connections to their underlying service, and track error
/// states, recycling connections and pausing work when required.
pub struct TaskBackend<T>
where
    T: RequestTransformer,
{
    requests_tx: mpsc::UnboundedSender<(T::Request, oneshot::Sender<Result<T::Response, Error>>)>,
}

impl<T> TaskBackend<T>
where
    T: RequestTransformer,
{
    pub fn new(
        executor: TaskExecutor,
        addr: SocketAddr,
        transformer: T,
        conn_limit: usize,
    ) -> (TaskBackend<T>, TaskBackendStateMachine<T>) {
        let (tx, rx) = mpsc::unbounded();

        let backend = TaskBackend {
            requests_tx: tx,
        };

        let runner = new_state_machine(executor, addr, transformer, rx, conn_limit);

        (backend, runner)
    }

    pub fn subscribe(&self) -> TaskBackendParticipant<T> {
        TaskBackendParticipant {
            requests_tx: self.requests_tx.clone(),
        }
    }
}

/// A placeholder for a caller interested in getting a connection from a backend.
///
/// This provides a Stream/Sink interface to the underlying queue of backend connections so that
/// getting a connection, and returning it, can be easily composed.
pub struct TaskBackendParticipant<T>
where
    T: RequestTransformer,
{
    requests_tx: mpsc::UnboundedSender<(T::Request, oneshot::Sender<Result<T::Response, Error>>)>,
}

impl<T> TaskBackendParticipant<T>
where
    T: RequestTransformer,
{
    pub fn submit(&mut self, request: T::Request) -> oneshot::Receiver<Result<T::Response, Error>> {
        let (tx, rx) = oneshot::channel();
        self.requests_tx
            .unbounded_send((request, tx))
            .expect("unbounded task send failed");
        rx
    }
}
