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
use crate::{
    backend::{
        pool::{BackendPool, BackendPoolBuilder},
        processor::Processor,
        redis::RedisProcessor,
    },
    common::{EnqueuedRequests, Message, Response, GenericError},
    conf::ListenerConfiguration,
    errors::CreationError,
    protocol::errors::ProtocolError,
    routing::{FixedRouter, ShadowRouter},
    service::{Service, Facade, Pipeline, Fragment, PipelineError},
};
use futures::{
    future::{Shared, FutureExt},
    sink::Sink,
    stream::{Stream, StreamExt},
};
use futures_turnstyle::Waiter;
use metrics_runtime::Sink as MetricSink;
use net2::TcpBuilder;
use std::{collections::HashMap, future::Future, task::Poll, net::SocketAddr};
use tokio::{io, net::TcpListener};
use tokio_net::driver::Handle;
use tokio_evacuate::{Evacuate, Warden};
use tokio_executor::DefaultExecutor;
use std::pin::Pin;

type GenericRuntimeFuture = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;
type BufferedPool<P, T> = Facade<BackendPool<P>, EnqueuedRequests<T>>;

/// Creates a listener from the given configuration.
///
/// The listener will spawn a socket for accepting client connections, and when a client connects,
/// spawn a task to process all of the messages from that client until the client disconnects or
/// there is an unrecoverable connection/protocol error.
pub fn from_config(
    version: usize,
    name: String,
    config: ListenerConfiguration,
    close: Shared<Waiter>,
    sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError> {
    // Create the actual listener proper.
    let listen_address = config.address.clone();
    let listener = get_listener(&listen_address).expect("failed to create the TCP listener");

    // Now build our handler: this is what's actually going to do the real work.
    let protocol = config.protocol.to_lowercase();
    let handler = match protocol.as_str() {
        "redis" => routing_from_config(name, config, listener, close.clone(), RedisProcessor::new(), sink),
        s => Err(CreationError::InvalidResource(format!("unknown cache protocol: {}", s))),
    }?;

    // Make sure our handlers close out when told.
    let task = async move {
        info!("[listener] starting listener '{}' (v{})", listen_address, version);
        handler.await;
        info!("[listener] shutting down listener '{}' (v{})", listen_address, version);
    };

    Ok(Box::pin(task))
}

fn routing_from_config<P, C>(
    name: String,
    config: ListenerConfiguration,
    listener: TcpListener,
    close: C,
    processor: P,
    sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError>
where
    P: Processor + Clone + Send + Sync + Unpin + 'static,
    P::Message: Message + Clone + Send + Sync + Unpin + 'static,
    P::Transport: Sink<P::Message> + Stream<Item = Result<P::Message, ProtocolError>> + Send + Sync + Unpin,
    <P::Transport as Sink<P::Message>>::Error: std::error::Error,
    C: Future + Clone + Send + Sync + Unpin + 'static,
{
    let reload_timeout_ms = config.reload_timeout_ms.unwrap_or_else(|| 5000);

    // Build our evacuator and wrap it as shared.  This lets us soft close everything.
    let (warden, evacuate, runner) = Evacuate::new(close, reload_timeout_ms);
    tokio::spawn(runner);
    let closer = evacuate.shared();

    // Get our scoped metric sink.
    let mut sink = sink.clone();
    sink.add_default_labels(&[("listener", name)]);

    // Extract all the configured pools and build a backend pool for them.
    let mut pools = HashMap::new();
    let pool_configs = config.pools.clone();
    for (pool_name, pool_config) in pool_configs {
        debug!(
            "[listener] configuring backend pool '{}' for address '{}'",
            &pool_name,
            config.address.clone()
        );

        let pool = BackendPoolBuilder::new(pool_name.clone(), processor.clone(), pool_config, sink.clone()).build()?;
        let buffered_pool = Facade::new(pool, DefaultExecutor::current(), 32).map_err(|_| {
            CreationError::InvalidResource(format!(
                "error while building pool '{}': failed to spawn task",
                pool_name
            ))
        })?;
        pools.insert(pool_name, buffered_pool);
    }

    // Figure out what sort of routing we're doing so we can grab the right handler.
    let mut routing = config.routing;
    let route_type = routing
        .entry("type".to_owned())
        .or_insert_with(|| "fixed".to_owned())
        .to_lowercase();
    match route_type.as_str() {
        "fixed" => get_fixed_router(listener, pools, processor, warden, closer, sink),
        "shadow" => get_shadow_router(listener, pools, processor, warden, closer, sink),
        x => Err(CreationError::InvalidResource(format!("unknown route type '{}'", x))),
    }
}

fn get_fixed_router<P, C>(
    listener: TcpListener,
    pools: HashMap<String, BufferedPool<P, P::Message>>,
    processor: P,
    warden: Warden,
    close: C,
    sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError>
where
    P: Processor + Clone + Send + Sync + Unpin + 'static,
    P::Message: Message + Clone + Send + Sync + Unpin + 'static,
    P::Transport: Sink<P::Message> + Stream<Item = Result<P::Message, ProtocolError>> + Send + Sync + Unpin,
    <P::Transport as Sink<P::Message>>::Error: std::error::Error,
    C: Future + Clone + Send + Sync + Unpin + 'static,
{
    // Construct an instance of our router.
    let default_pool = pools
        .get("default")
        .cloned()
        .ok_or_else(|| CreationError::InvalidResource("no default pool configured for fixed router".to_string()))?;
    let router = FixedRouter::new(processor.clone(), default_pool);

    build_router_chain(listener, processor, router, warden, close, sink)
}

fn get_shadow_router<P, C>(
    listener: TcpListener,
    pools: HashMap<String, BufferedPool<P, P::Message>>,
    processor: P,
    warden: Warden,
    close: C,
    sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError>
where
    P: Processor + Clone + Send + Sync + Unpin + 'static,
    P::Message: Message + Clone + Send + Sync + Unpin + 'static,
    P::Transport: Sink<P::Message> + Stream<Item = Result<P::Message, ProtocolError>> + Send + Sync + Unpin,
    <P::Transport as Sink<P::Message>>::Error: std::error::Error,
    C: Future + Clone + Send + Sync + Unpin + 'static,
{
    // Construct an instance of our router.
    let default_pool = pools
        .get("default")
        .cloned()
        .ok_or_else(|| CreationError::InvalidResource("no default pool configured for shadow router".to_string()))?;

    let shadow_pool = pools
        .get("shadow")
        .cloned()
        .ok_or_else(|| CreationError::InvalidResource("no shadow pool configured for shadow router".to_string()))?;

    let router = ShadowRouter::new(processor.clone(), default_pool, shadow_pool);

    build_router_chain(listener, processor, router, warden, close, sink)
}

fn build_router_chain<P, R, C>(
    listener: TcpListener,
    processor: P,
    router: R,
    warden: Warden,
    mut close: C,
    mut sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError>
where
    P: Processor + Clone + Unpin + 'static,
    P::Message: Message + Clone + Unpin + 'static,
    P::Transport: Sink<P::Message> + Stream<Item = Result<P::Message, ProtocolError>> + Send + Unpin,
    <P::Transport as Sink<P::Message>>::Error: std::error::Error,
    R: Service<Vec<P::Message>> + Clone + Send + Sync + 'static,
    R::Error: Into<GenericError> + Send + Sync,
    R::Response: IntoIterator<Item = Response<P::Message>> + Send,
    R::Future: Future + Send + Unpin,
    C: Future + Clone + Send + Sync + Unpin + 'static,
{
    let task = async move {
        let close2 = close.clone();
        let mut close = close.fuse();
        let mut incoming = listener.incoming().fuse();

        loop {
            // Stop ourselves if need be.
            select! {
                _ = close => {
                    debug!("listener should close, breaking");
                    break
                },
                r = incoming.next() => match r {
                    // Our listener is gone, so we're done here.
                    None => break,
                    Some(conn) => match conn {
                        Err(e) => {
                            debug!("[client] error while accepting connection: {}", e);
                        },
                        Ok(client) => {
                            warden.increment();
                            sink.record_counter("clients_connected", 1);

                            let router = router.clone();
                            let processor = processor.clone();
                            let close = close2.clone().fuse();
                            let warden = warden.clone();
                            let mut sink2 = sink.clone();
                            let client_addr = client.peer_addr().unwrap();
                            debug!("[client] {} connected", client_addr);

                            let transport = processor.get_transport(client);
                            let fragment = Fragment::new(router, processor);
                            let mut pipeline = Pipeline::new(transport, fragment);

                            tokio::spawn(async move {
                                let mut pf = pipeline.boxed().fuse();
                                select! {
                                    r = pf => if let Err(e) = r {
                                        match e {
                                            PipelineError::TransportReceive(ie) => {
                                                if !ie.client_closed() {
                                                    sink2.record_counter("client_errors", 1);
                                                    error!("[client] transport error from {}: {}", client_addr, ie);
                                                }
                                            },
                                            e => error!("[client] error from {}: {}", client_addr, e),
                                        }
                                    },
                                    _ = close.fuse() => {
                                    },
                                };

                                debug!("[client] {} disconnected", client_addr);
                                warden.decrement();
                            });
                        },
                    }
                }
            }
        }
    };

    Ok(Box::pin(task))
}

fn get_listener(addr_str: &str) -> io::Result<TcpListener> {
    let addr = addr_str.parse().unwrap();
    let builder = match addr {
        SocketAddr::V4(_) => TcpBuilder::new_v4()?,
        SocketAddr::V6(_) => TcpBuilder::new_v6()?,
    };
    configure_builder(&builder)?;
    builder.reuse_address(true)?;
    builder.bind(addr)?;
    builder
        .listen(1024)
        .and_then(|l| TcpListener::from_std(l, &Handle::default()))
}

#[cfg(unix)]
fn configure_builder(builder: &TcpBuilder) -> io::Result<()> {
    use net2::unix::*;

    builder.reuse_port(true)?;
    Ok(())
}

#[cfg(windows)]
fn configure_builder(_builder: &TcpBuilder) -> io::Result<()> { Ok(()) }
