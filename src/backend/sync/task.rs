use std::io::Error;
use std::net::SocketAddr;
use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::prelude::*;
use futures::future::{ok, result};
use tokio::net::{TcpStream, ConnectFuture};

type TaskTcpConnection = Either<TaskExistingTcpStream, ConnectFuture>;

pub struct TaskExistingTcpStream {
    stream: Option<TcpStream>,
}

impl TaskExistingTcpStream {
    pub fn from_stream(stream: TcpStream) -> TaskExistingTcpStream {
        TaskExistingTcpStream {
            stream: Some(stream),
        }
    }
}

impl Future for TaskExistingTcpStream {
    type Item = TcpStream;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(self.stream.take().unwrap()))
    }
}

/// Wraps a request future as a work item so we can statically type the work futures queue.
struct WorkItemFuture<F>
    where F: Future,
{
    inner: F,
}

impl<F> WorkItemFuture<F>
    where F: Future,
{
    pub fn new(inner: F) -> WorkItemFuture<F>
        where F: Future<Item = (), Error = ()>
    {
        WorkItemFuture { inner: inner }
    }
}

impl<F> Future for WorkItemFuture<F>
    where F: Future
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

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
pub struct TaskBackendStateMachine<R, V> {
    requests_rx: mpsc::UnboundedReceiver<(R, oneshot::Sender<Result<V, Error>>)>,

    address: SocketAddr,
    conns: Vec<TcpStream>,
    conns_rx: mpsc::UnboundedReceiver<TaskBackendConnection>,
    conns_tx: mpsc::UnboundedSender<TaskBackendConnection>,
    conn_count: usize,
    conn_limit: usize,

    work: FuturesUnordered<WorkItemFuture<Future<Item = (), Error = ()>>>,
}

impl<R, R2, V> Future for TaskBackendStateMachine<R, V>
    where R: FnOnce(TcpStream) -> R2,
          R2: Future<Item = (TcpStream, V), Error = Error> + 'static
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // See if we've had any connections returned to us.
        loop {
            match self.conns_rx.poll() {
                Ok(Async::Ready(Some(conn))) => match conn {
                    TaskBackendConnection::Alive(conn) => self.conns.push(conn),
                    TaskBackendConnection::Error => {
                        self.conn_count -= 1;
                    },
                },
                Ok(Async::NotReady) => break,
                x => return Err(()),
            }
        }

        // If all connections are currently busy, just poll our in-flight work in the hopes that
        // something is going to finish and trigger us again.
        if self.conns.len() == 0 && self.conn_count == self.conn_limit {
            let _ = self.work.poll();
            return Ok(Async::NotReady);
        }

        loop {
        match self.requests_rx.poll() {
            Ok(Async::NotReady) => {
                // Open connections doesn't mean we don't have work, so do a quick poll and then
                // bounce out of here.
                let _ = self.work.poll();
                return Ok(Async::NotReady);
            },
            Ok(Async::Ready(Some((f, tx)))) => {
                // We have a new request.  Grab a connection and attempt to service it.
                let connection = match self.conns.len() {
                    0 => {
                        self.conn_count += 1;
                        Either::B(TcpStream::connect(&self.address))
                    },
                    _ => self.conns.pop().map(|x| Either::A(TaskExistingTcpStream::from_stream(x))).unwrap(),
                };

                let conns_tx = self.conns_tx.clone();
                let work = connection
                    .and_then(move |conn| f(conn))
                    .then(|result| match result {
                        Ok((conn, x)) => ok((TaskBackendConnection::Alive(conn), Ok(x))),
                        Err(e) => ok((TaskBackendConnection::Error, Err(e))),
                    })
                    .and_then(move |(backend_result, op_result)| {
                        conns_tx
                            .send(backend_result)
                            .map(|_| op_result)
                    })
                    .and_then(move |result| tx.send(result))
                    .map_err(|_| ())
                    .map(|_| ());

                self.work.push(work);
            },
            _ => Err(()),
        }
        }
    }
}

fn new_state_machine<R, R2, V>(addr: SocketAddr, rx: mpsc::UnboundedReceiver<(R, oneshot::Sender<V>)>, conn_limit: usize)
    -> TaskBackendStateMachine<R, V>
    where R: FnOnce(TcpStream) -> R2,
          R2: Future<Item = (TcpStream, V), Error = Error>,
{
    let (conns_tx, conns_rx) = mpsc::unbounded();

    TaskBackendStateMachine {
        requests_rx: rx,

        address: addr,
        conns: Vec::new(),
        conns_rx: conns_rx,
        conns_tx: conns_tx,
        conn_count: 0,
        conn_limit: conn_limit,

        work: FuturesUnordered::new(),
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
pub struct TaskBackend<R, V> {
    address: SocketAddr,
    requests_tx: mpsc::UnboundedSender<(R, oneshot::Sender<Result<V, Error>>)>,
}

impl<R, R2, V> TaskBackend<R, V>
    where R: FnOnce(TcpStream) -> R2,
          R2: Future<Item = (TcpStream, V), Error = Error> + 'static
{
    pub fn new(addr: SocketAddr, conn_limit: usize) -> (TaskBackend<R, V>, TaskBackendStateMachine<R, V>)
    {
        let (tx, rx) = mpsc::unbounded();

        let backend = TaskBackend {
            address: addr,
            requests_tx: tx,
        };

        let runner = new_state_machine(addr, rx, conn_limit);

        (backend, runner)
    }

    pub fn subscribe(&self) -> TaskBackendParticipant<R, V> {
        TaskBackendParticipant {
            requests_tx: self.requests_tx.clone(),
        }
    }
}

/// A placeholder for a caller interested in getting a connection from a backend.
///
/// This provides a Stream/Sink interface to the underlying queue of backend connections so that
/// getting a connection, and returning it, can be easily composed.
pub struct TaskBackendParticipant<R, V> {
    requests_tx: mpsc::UnboundedSender<(R, oneshot::Sender<Result<V, Error>>)>,
}

impl<R, R2, V> TaskBackendParticipant<R, V>
    where R: FnOnce(TcpStream) -> R2,
          R2: Future<Item = (TcpStream, V), Error = Error>
{
    pub fn submit(&mut self, f: R) -> oneshot::Receiver<Result<V, Error>> {
        let (tx, rx) = oneshot::channel();
        self.requests_tx.unbounded_send((f, tx)).expect("unbounded task send failed");
        rx
    }
}
