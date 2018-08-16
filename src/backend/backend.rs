// Copyright (c) 2018 Nuclear Furnace
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use backend::{message_queue::QueuedMessage, processor::RequestProcessor};
use futures::{
    future::{ok, Either, Shared},
    prelude::*,
    sync::mpsc,
};
use futures_turnstyle::Waiter;
use protocol::errors::ProtocolError;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use util::{WorkQueue, Worker};

pub enum BackendCommand {
    Error,
}

struct BackendConnection<P>
where
    P: RequestProcessor,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    processor: P,
    worker: Worker<Vec<QueuedMessage<P::Message>>>,
    command_tx: mpsc::UnboundedSender<BackendCommand>,
    address: SocketAddr,

    socket: Option<TcpStream>,
    current: Option<P::Future>,
}

impl<P> Future for BackendConnection<P>
where
    P: RequestProcessor,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            // First, check if we have an operation running.  If we do, poll it to drive it towards
            // completion.  If it's done, we'll reclaim the socket and then fallthrough to trying to
            // find another piece of work to run.
            if let Some(task) = self.current.as_mut() {
                match task.poll() {
                    Ok(Async::Ready(socket)) => {
                        // The operation finished, and gave us the connection back.
                        self.socket = Some(socket);
                        self.current = None;
                    },
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(_) => {
                        // On error, we kill ourselves but notify the supervisor first so it can
                        // replace us down the line.
                        let _ = self.command_tx.unbounded_send(BackendCommand::Error);
                        return Err(());
                    },
                }
            }

            // If we're here, we have no current operation to drive, so see if anything is in our work
            // queue that we can grab.
            match self.worker.poll() {
                Ok(Async::Ready(Some(batch))) => {
                    let socket = match self.socket.take() {
                        Some(socket) => Either::A(ok(socket)),
                        None => Either::B(TcpStream::connect(&self.address)),
                    };

                    let work = self.processor.process(batch, socket);
                    self.current = Some(work);
                },
                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => {
                    let _ = self.command_tx.unbounded_send(BackendCommand::Error);
                    return Err(e);
                },
            }
        }
    }
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
pub struct BackendSupervisor<P>
where
    P: RequestProcessor + Clone + Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    processor: P,
    worker: Worker<Vec<QueuedMessage<P::Message>>>,
    command_rx: mpsc::UnboundedReceiver<BackendCommand>,
    command_tx: mpsc::UnboundedSender<BackendCommand>,

    address: SocketAddr,
    conn_count: usize,
    conn_limit: usize,

    close: Shared<Waiter>,
}

impl<P> Future for BackendSupervisor<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // If we're supposed to close, do it now.
        if let Ok(Async::Ready(_)) = self.close.poll() {
            return Ok(Async::Ready(()));
        }

        // Process any commands.
        loop {
            match self.command_rx.poll() {
                Ok(Async::Ready(Some(cmd))) => {
                    match cmd {
                        BackendCommand::Error => {
                            self.conn_count -= 1;
                        },
                    }
                },
                Ok(Async::NotReady) => break,
                _ => return Err(()),
            }
        }

        // Make sure all connections have been spawned.
        while self.conn_count < self.conn_limit {
            let connection = BackendConnection {
                processor: self.processor.clone(),
                worker: self.worker.clone(),
                address: self.address,
                command_tx: self.command_tx.clone(),
                current: None,
                socket: None,
            };

            tokio::spawn(connection);

            self.conn_count += 1;
        }

        Ok(Async::NotReady)
    }
}

fn new_supervisor<P>(
    addr: SocketAddr, processor: P, worker: Worker<Vec<QueuedMessage<P::Message>>>, conn_limit: usize,
    close: Shared<Waiter>,
) -> BackendSupervisor<P>
where
    P: RequestProcessor + Clone + Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    let (command_tx, command_rx) = mpsc::unbounded();

    BackendSupervisor {
        processor,
        worker,

        command_rx,
        command_tx,

        address: addr,
        conn_count: 0,
        conn_limit,

        close,
    }
}

/// Managed connections to a backend server.
///
/// This backend is serviced by a Tokio task, which processes all work requests to backend servers,
/// and the connections that constitute this backend server.
///
/// Backends are, in essence, proxy objects to their respective Tokio task, which is doing the
/// actual heavy lifting.  They exist purely as a facade to the underlying channels which shuttle
/// work back and forth between the backend connections and client connections.
///
/// Backends maintain a given number of connections to their underlying service, and track error
/// states, recycling connections and pausing work when required.
pub struct Backend<P>
where
    P: RequestProcessor + Clone + Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    work_queue: WorkQueue<Vec<QueuedMessage<P::Message>>>,
}

impl<P> Backend<P>
where
    P: RequestProcessor + Clone + Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    pub fn new(
        addr: SocketAddr, processor: P, conn_limit: usize, close: Shared<Waiter>,
    ) -> (Backend<P>, BackendSupervisor<P>) {
        let work_queue = WorkQueue::new();
        let worker = work_queue.worker();
        let backend = Backend { work_queue };
        let runner = new_supervisor(addr, processor, worker, conn_limit, close);

        (backend, runner)
    }

    pub fn submit(&self, batch: Vec<QueuedMessage<P::Message>>) { self.work_queue.send(batch) }
}
