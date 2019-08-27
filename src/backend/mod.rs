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
    common::{EnqueuedRequests, Message, ResponseFuture, Responses, GenericError},
    errors::CreationError,
    service::DrivenService,
};
use async_trait::async_trait;
use futures::stream::FuturesOrdered;
use metrics_runtime::{data::Counter, Sink as MetricSink};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    str::FromStr,
    time::Duration,
};
use tokio::net::tcp::TcpStream;
use tokio::future::FutureExt;

// Don't talk to me about this, I don't wanna talk about it.
const LONG_ASS_TIME: u64 = 5 * 365 * 24 * 60 * 60;

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

    conn: Option<TcpStream>,
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
            conn: None,
            pending: VecDeque::new(),
            connects: sink.counter("connects"),
        }
    }
}

#[async_trait]
impl<P> DrivenService<EnqueuedRequests<P::Message>> for BackendConnection<P>
where
    P: Processor + Send + Sync,
    P::Message: Message + Clone + Send + Sync,
{
    type Error = GenericError;
    type Future = ResponseFuture<P::Message>;
    type Response = Responses<P::Message>;

    async fn ready(&mut self) -> Result<(), Self::Error> { Ok(()) }

    async fn drive(&mut self) -> Result<(), Self::Error> {
        debug!("trying to drive ourself as backend conn");

        while !self.pending.is_empty() {
            debug!("conn still has msgs, getting connection");

            // Get the underlying connection to the backend.
            let mut conn = match self.conn.take() {
                // We still have an existing connection, so reuse it.
                Some(conn) => conn,
                // We haven't connected yet, or the old connection was faulty, so reconnect.
                None => {
                    let conn = self.processor.preconnect(&self.address, self.noreply).await?;
                    self.connects.increment();
                    conn
                },
            };

            debug!("got backend connection");

            // Extract a batch.
            let batch = self.pending.pop_front().expect("self.pending should not be empty");

            // Actually process this batch and then give back the connection.
            let timeout_ms = if self.timeout_ms == 0 {
                LONG_ASS_TIME
            } else {
                self.timeout_ms
            };

            debug!("about to run request on backend connection");

            // Run the actual request, and bail out if we get an error.  This ensures that
            // we don't give back the connection (since it's likely tainted) and that it
            // will be recreated.
            let _ = self.processor
                .process(batch, &mut conn)
                .timeout(Duration::from_millis(timeout_ms))
                .await?;

            debug!("done with request; yielding connection back to loop");

            self.conn = Some(conn);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Self::Error> { self.drive().await }

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


        debug!("backend call'd -> took in {} msgs", batch.len());
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

#[async_trait]
impl<P> DrivenService<EnqueuedRequests<P::Message>> for Backend<P>
where
    P: Processor + Clone + Send + Sync,
    P::Message: Message + Clone + Send + Sync,
{
    type Error = GenericError;
    type Future = ResponseFuture<P::Message>;
    type Response = Responses<P::Message>;

    async fn ready(&mut self) -> Result<(), Self::Error> {
        self.health.healthy().await;
        Ok(())
    }

    async fn drive(&mut self) -> Result<(), Self::Error> {
        // TODO: we may want to precisely examine the protocol error and behave differently
        // for i/o errors vs actual protocol errors, but not sure yet....
        //
        // TODO 2: we also may want to enqueue all the batches at the backend level, and pass one
        // in to BackendConnection::drive every time we call it.  we might want to do something
        // like one future per conn (or less if not enough batches) to drive it, stored in a
        // futuresunoredered, and then await on that to try and drive them all pseudo simultaenously
        // that way hopefully we can avoid a slow connection blocking other i/o and keep latency
        // down
        for conn in &mut self.conns {
            debug!("driving individual backend connection");
            if let Err(_) = conn.drive().await {
                self.health.increment_error();
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        for conn in &mut self.conns {
            conn.drive().await?;
        }

        Ok(())
    }

    fn call(&mut self, req: EnqueuedRequests<P::Message>) -> Self::Future {
        let fut = self.conns[self.conns_index].call(req);

        self.conns_index += 1;
        self.conns_index %= self.conns.len();
        fut
    }
}
