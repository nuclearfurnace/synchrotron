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
pub mod distributor;
mod errors;
pub mod hasher;
mod health;
pub mod pool;
pub mod processor;
pub mod redis;

pub use self::errors::{BackendError, PoolError};

use crate::{
    backend::{distributor::BackendDescriptor, health::BackendHealth, processor::Processor},
    common::{EnqueuedRequests, Message, ResponseFuture, Responses, GenericError, ConnectionFuture},
    errors::CreationError,
    protocol::errors::ProtocolError,
    service::DrivenService,
};
use std::future::Future;
use std::task::{Context, Poll};
use std::pin::Pin;
use futures::{ready, stream::FuturesOrdered, future::FutureExt};
use metrics_runtime::{data::Counter, Sink as MetricSink};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    str::FromStr,
    time::Duration,
};
use tokio::net::tcp::TcpStream;
use tokio::future::FutureExt as TokioFutureExt;

// Don't talk to me about this, I don't wanna talk about it.
const LONG_ASS_TIME: u64 = 5 * 365 * 24 * 60 * 60;

enum ConnectionState {
    Disconnected,
    Connecting(ConnectionFuture),
    Connected(Option<TcpStream>),
    Processing(Pin<Box<dyn Future<Output = Result<TcpStream, BackendError>> + Send>>),
}

/// A backend connection.
///
/// This represents a one-to-one mapping with a TCP connection to the given backend server.  This
/// connection will independently poll the work queue for the backend and run requests when
/// available.
pub struct BackendConnection<P>
where
    P: Processor + Send + Sync,
    P::Message: Message + Clone + Send + Sync,
{
    processor: P,
    address: SocketAddr,
    timeout_ms: u64,
    noreply: bool,

    state: ConnectionState,
    pending: VecDeque<EnqueuedRequests<P::Message>>,

    connects: Counter,
}

impl<P> BackendConnection<P>
where
    P: Processor + Send + Sync,
    P::Message: Message + Clone + Send + Sync,
{
    pub fn new(
        address: SocketAddr,
        processor: P,
        timeout_ms: u64,
        noreply: bool,
        mut sink: MetricSink,
    ) -> BackendConnection<P> {
        BackendConnection {
            processor,
            address,
            timeout_ms,
            noreply,
            state: ConnectionState::Disconnected,
            pending: VecDeque::new(),
            connects: sink.counter("connects"),
        }
    }
}

impl<P> DrivenService<EnqueuedRequests<P::Message>> for BackendConnection<P>
where
    P: Processor + Send + Sync,
    P::Message: Message + Clone + Send + Sync,
{
    type Error = GenericError;
    type Future = ResponseFuture<P::Message>;
    type Response = Responses<P::Message>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_service(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let span = tracing::trace_span!("poll service", backend.addr = ?self.address);
        let _guard = span.enter();

        loop {
            let (state, err) = match &mut self.state {
                ConnectionState::Disconnected => {
                    // If we're disconnected, we only need to both connecting if we have requests.
                    if self.pending.is_empty() {
                        tracing::trace!("no requests to process; sleeping");
                        return Poll::Ready(Ok(()));
                    }

                    tracing::trace!("pending requests, trying to connect to backend");
                    // We have items to process, so we need to start by actually connecting.
                    (ConnectionState::Connecting(self.processor.preconnect(self.address, self.noreply)), None)
                },
                ConnectionState::Connecting(ref mut f) => match Pin::new(f).poll(cx) {
                    Poll::Pending => {
                        debug!("connection to backend pending");
                        return Poll::Pending
                    },
                    Poll::Ready(Ok(conn)) => {
                        tracing::trace!("successfully connected");
                        self.connects.increment();
                        (ConnectionState::Connected(Some(conn)), None)
                    },
                    Poll::Ready(Err(e)) => {
                        tracing::error!("error while connecting to backend; resetting state");

                        // This is a very deliberate action.
                        //
                        // Normally, we'd catch errors while running a request, and these errors
                        // would usually be indicative of a problem with _that_ request.  The issue
                        // here is that if we have many pending responses, and we get an error even
                        // connecting to the backend -- be it for authentication or networking
                        // reasons -- then we likely can't service any requests on this backend for
                        // a short period of time, and further, we don't want to keep retrying
                        // every single request up until the backend starts working again.... or
                        // else we may end up with clients stacking their requests, forcing a
                        // client-side timeout to occur, as they wait their turn in line to have
                        // their message tried and failed for every poll in the "connecting" state,
                        // which includes waiting for each message failure plus a likely cooloff
                        // period.
                        //
                        // By clearing all pending messages, we fast fail ourselves.  If continual
                        // traffic is coming in, it will still quickly trigger enough errors for
                        // the cooloff feature to kick in, propetly downing the backend within the
                        // pool, but we can get back to clients much faster on the fact that the
                        // backend is experiencing issues.
                        self.pending.clear();

                        (ConnectionState::Disconnected, Some(BackendError::Protocol(e)))
                    },
                },
                ConnectionState::Connected(ref mut conn) => {
                    if !self.pending.is_empty() {
                        let next = self.pending.pop_front().expect("self.pending should not be empty");
                        let timeout_ms = if self.timeout_ms == 0 {
                            LONG_ASS_TIME
                        } else {
                            self.timeout_ms
                        };

                        let conn = conn.take().expect("conn should have been existing connection");
                        let process = self.processor
                            .process(next, conn)
                            .timeout(Duration::from_millis(timeout_ms))
                            .map(|r| {
                                match r {
                                    Ok(v) => match v {
                                        Ok(conn) => Ok(conn),
                                        Err(e) => Err(BackendError::Protocol(e)),
                                    },
                                    Err(_) => Err(BackendError::TimedOut),
                                }
                            })
                            .boxed();

                        tracing::trace!("took pending request, trying to process");

                        (ConnectionState::Processing(process), None)
                    } else {
                        tracing::trace!("connected but no requests to process; sleeping");
                        // We're connected but have no items to process; we're done for now.
                        return Poll::Ready(Ok(()));
                    }
                },
                ConnectionState::Processing(ref mut f) => match ready!(Pin::new(f).poll(cx)) {
                    // We processed the request just fine, so reset ourselves by taking back the
                    // connection and seeing if we have any more requests to process.
                    Ok(conn) => {
                        tracing::trace!("successfully processed request; resetting state");
                        (ConnectionState::Connected(Some(conn)), None)
                    },
                    // We encountered an error while processing, which is _likely_ going to mean
                    // the connection itself is dirty, so drop it and force a new connection.
                    Err(e) => {
                        tracing::error!("encountered error during processing; resetting state");
                        (ConnectionState::Disconnected, Some(e))
                    },
                },
            };

            self.state = state;
            if let Some(e) = err {
                return Poll::Ready(Err(e.into()))
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tracing::trace!(message = "poll close", backend.addr = ?self.address);
        self.poll_service(cx)
    }

    fn call(&mut self, mut batch: EnqueuedRequests<P::Message>) -> Self::Future {
        let fut = batch
            .as_mut_slice()
            .iter_mut()
            .map(|x| x.get_response_rx())
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .fold(FuturesOrdered::new(), |mut xs, x| {
                xs.push(x);
                xs
            });


        tracing::trace!(message = "call", backend.addr = ?self.address, count = batch.len(), pending = self.pending.len());
        self.pending.push_back(batch);

        ResponseFuture::new(fut)
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
    P: Processor + Clone + Send + Sync,
    P::Message: Message + Clone + Send + Sync,
{
    identifier: String,
    health: BackendHealth,
    conns: Vec<BackendConnection<P>>,
    conns_index: usize,
    sink: MetricSink,
}

impl<P> Backend<P>
where
    P: Processor + Clone + Send + Sync,
    P::Message: Message + Clone + Send + Sync,
{
    pub fn new(
        address: SocketAddr,
        identifier: String,
        processor: P,
        mut options: HashMap<String, String>,
        noreply: bool,
        sink: MetricSink,
    ) -> Result<Backend<P>, CreationError>
    where
        P: Processor + Clone + Send + Sync,
        P::Message: Message + Clone + Send + Sync,
    {
        let sink = sink.scoped("backend");

        let conn_limit_raw = options.entry("conns".to_owned()).or_insert_with(|| "1".to_owned());
        let conn_limit = usize::from_str(conn_limit_raw.as_str())
            .map_err(|_| CreationError::InvalidParameter("options.conns".to_string()))?;
        debug!("[listener] using connection limit of '{}'", conn_limit);

        let cooloff_enabled_raw = options
            .entry("cooloff_enabled".to_owned())
            .or_insert_with(|| "true".to_owned());
        let cooloff_enabled = bool::from_str(cooloff_enabled_raw.as_str())
            .map_err(|_| CreationError::InvalidParameter("options.cooloff_enabled".to_string()))?;

        let cooloff_timeout_ms_raw = options
            .entry("cooloff_timeout_ms".to_owned())
            .or_insert_with(|| "10000".to_owned());
        let cooloff_timeout_ms = u64::from_str(cooloff_timeout_ms_raw.as_str())
            .map_err(|_| CreationError::InvalidParameter("options.cooloff_timeout_ms".to_string()))?;

        let cooloff_error_limit_raw = options
            .entry("cooloff_error_limit".to_owned())
            .or_insert_with(|| "5".to_owned());
        let cooloff_error_limit = usize::from_str(cooloff_error_limit_raw.as_str())
            .map_err(|_| CreationError::InvalidParameter("options.cooloff_error_limit".to_string()))?;

        let health = BackendHealth::new(cooloff_enabled, cooloff_timeout_ms, cooloff_error_limit);

        // TODO: where the hell did the actual backend timeout value go? can't hard-code this
        let conns = (0..conn_limit)
            .map(|_| BackendConnection::new(address, processor.clone(), 500, noreply, sink.clone()))
            .collect();

        Ok(Backend {
            identifier,
            health,
            conns,
            conns_index: 0,
            sink,
        })
    }

    pub fn health(&self) -> &BackendHealth { &self.health }

    pub fn get_descriptor(&mut self) -> BackendDescriptor {
        BackendDescriptor {
            idx: 0,
            identifier: self.identifier.clone(),
            healthy: self.health.is_healthy(),
        }
    }
}

impl<P> DrivenService<EnqueuedRequests<P::Message>> for Backend<P>
where
    P: Processor + Clone + Send + Sync,
    P::Message: Message + Clone + Send + Sync,
{
    type Error = GenericError;
    type Future = ResponseFuture<P::Message>;
    type Response = Responses<P::Message>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Make this give us back a brand new future, maybe?  Gotta figure that out.
        self.health.poll_health(cx)
            .map(|_| Ok(()))
    }

    fn poll_service(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO: we may want to precisely examine the protocol error and behave differently
        // for i/o errors vs actual protocol errors, but not sure yet....
        //
        // TODO 2: we also may want to enqueue all the batches at the backend level, and pass one
        // in to BackendConnection::drive every time we call it.  we might want to do something
        // like one future per conn (or less if not enough batches) to drive it, stored in a
        // futuresunoredered, and then await on that to try and drive them all pseudo simultaenously
        // that way hopefully we can avoid a slow connection blocking other i/o and keep latency
        // down
        let mut any_pending = false;
        for conn in &mut self.conns {
            match conn.poll_service(cx) {
                Poll::Pending => any_pending = true,
                Poll::Ready(Ok(())) => {},
                Poll::Ready(Err(_)) => self.health.increment_error(),
            }
        }

        if any_pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut any_pending = false;
        for conn in &mut self.conns {
            match conn.poll_close(cx) {
                Poll::Pending => any_pending = true,
                Poll::Ready(Ok(())) => {},
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }

        if any_pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, req: EnqueuedRequests<P::Message>) -> Self::Future {
        let fut = self.conns[self.conns_index].call(req);

        self.conns_index += 1;
        self.conns_index %= self.conns.len();
        fut
    }
}
