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
use backend::{processor::RequestProcessor, BackendError};
use common::{Keyed, OrderedMessages};
use futures::{
    future::{ok, Either},
    prelude::*,
    sync::{mpsc, oneshot},
};
use metrics::{get_sink, MetricSink, Metrics};
use std::{io, net::SocketAddr};
use tokio::net::TcpStream;

pub enum BackendConnection {
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
pub struct BackendStateMachine<T>
where
    T: RequestProcessor,
{
    sink: MetricSink,

    processor: T,
    requests_rx: mpsc::UnboundedReceiver<(
        OrderedMessages<T::Message>,
        oneshot::Sender<Result<OrderedMessages<T::Message>, BackendError>>,
    )>,

    address: SocketAddr,
    conns: Vec<TcpStream>,
    conns_rx: mpsc::UnboundedReceiver<BackendConnection>,
    conns_tx: mpsc::UnboundedSender<BackendConnection>,
    conn_count: usize,
    conn_limit: usize,
}

impl<T> Future for BackendStateMachine<T>
where
    T: RequestProcessor,
    T::Message: Keyed + Send + 'static,
    T::Future: Future<Item = (TcpStream, OrderedMessages<T::Message>), Error = io::Error> + Send + 'static,
{
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            // See if we have a connection to recover or replace.
            match self.conns_rx.poll() {
                Ok(Async::Ready(Some(conn))) => {
                    match conn {
                        BackendConnection::Alive(conn) => self.conns.push(conn),
                        BackendConnection::Error => {
                            self.conn_count -= 1;
                        },
                    }
                },
                Ok(Async::NotReady) => {},
                _ => return Err(()),
            }

            // If all connections are currently busy, we can't do anything else.  By waiting until
            // we've exhausted our connections channel to check here, we're making sure we set
            // ourselves up to be notified when a connection is recovered, or fails.
            if self.conns.len() == 0 && self.conn_count == self.conn_limit {
                return Ok(Async::NotReady);
            }

            // See if we have a request waiting to be processed.
            match self.requests_rx.poll() {
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(Some((request, response_tx)))) => {
                    // We have a new request.  Grab a connection and attempt to service it.
                    let connection = match self.conns.len() {
                        0 => {
                            self.conn_count += 1;
                            self.sink.increment(Metrics::BackendNewConnections);
                            Either::B(TcpStream::connect(&self.address))
                        },
                        _ => self.conns.pop().map(|x| Either::A(ok(x))).unwrap(),
                    };

                    let conns_tx = self.conns_tx.clone();
                    let inner_work = self.processor.process(request, connection);
                    let work = inner_work
                        .then(|result| {
                            match result {
                                Ok((conn, x)) => ok((BackendConnection::Alive(conn), Ok(x))),
                                Err(e) => ok((BackendConnection::Error, Err(e.into()))),
                            }
                        }).and_then(move |(br, or)| conns_tx.send(br).map(|_| or))
                        .and_then(move |result| ok(response_tx.send(result)))
                        .map_err(|_| ())
                        .map(|_| ());

                    tokio::spawn(work);
                },
                _ => return Err(()),
            }
        }
    }
}

fn new_state_machine<T>(
    addr: SocketAddr, processor: T,
    rx: mpsc::UnboundedReceiver<(
        OrderedMessages<T::Message>,
        oneshot::Sender<Result<OrderedMessages<T::Message>, BackendError>>,
    )>,
    conn_limit: usize,
) -> BackendStateMachine<T>
where
    T: RequestProcessor,
{
    let (conns_tx, conns_rx) = mpsc::unbounded();

    BackendStateMachine {
        sink: get_sink(),

        processor,
        requests_rx: rx,

        address: addr,
        conns: Vec::new(),
        conns_rx,
        conns_tx,
        conn_count: 0,
        conn_limit,
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
pub struct Backend<T>
where
    T: RequestProcessor,
{
    requests_tx: mpsc::UnboundedSender<(
        OrderedMessages<T::Message>,
        oneshot::Sender<Result<OrderedMessages<T::Message>, BackendError>>,
    )>,
}

impl<T> Backend<T>
where
    T: RequestProcessor,
{
    pub fn new(addr: SocketAddr, processor: T, conn_limit: usize) -> (Backend<T>, BackendStateMachine<T>) {
        let (tx, rx) = mpsc::unbounded();
        let backend = Backend { requests_tx: tx };
        let runner = new_state_machine(addr, processor, rx, conn_limit);

        (backend, runner)
    }

    pub fn submit(
        &self, request: OrderedMessages<T::Message>,
    ) -> oneshot::Receiver<Result<OrderedMessages<T::Message>, BackendError>> {
        let (tx, rx) = oneshot::channel();
        self.requests_tx
            .unbounded_send((request, tx))
            .expect("unbounded task send failed");
        rx
    }
}
