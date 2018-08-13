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
use backend::{
    distributor, hasher,
    pool::BackendPool,
    processor::{MessageSink, RequestProcessor},
    redis::RedisRequestProcessor,
};
use common::{Keyed, OrderedMessages};
use conf::ListenerConfiguration;
use futures::{
    future::{lazy, ok, Shared},
    prelude::*,
};
use futures_turnstyle::Waiter;
use metrics::{self, Metrics};
use net2::TcpBuilder;
use routing::{FixedRouter, Router, RouterError};
use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};
use tokio::{
    io,
    net::{TcpListener, TcpStream},
    reactor,
};
use util::StreamExt;

type GenericRuntimeFuture = Box<Future<Item = (), Error = ()> + Sync + Send + 'static>;

/// Creates a listener from the given configuration.
///
/// The listener will spawn a socket for accepting client connections, and when a client connects,
/// spawn a task to process all of the messages from that client until the client disconnects or
/// there is an unrecoverable connection/protocol error.
pub fn from_config(config: ListenerConfiguration, close: Shared<Waiter>) -> Result<GenericRuntimeFuture, Error> {
    // Create the actual listener proper.
    let listen_address = config.address.clone();
    let listener = get_listener(&listen_address).expect("failed to create the TCP listener");

    // Get the correct handler based on protocol.
    let protocol = config.protocol.to_lowercase();
    let handler = match protocol.as_str() {
        "redis" => routing_from_config(config, listener, RedisRequestProcessor::new())?,
        s => panic!("unknown protocol type: {}", s),
    };

    // Make sure our handlers close out when told.
    let listen_address2 = listen_address.clone();
    let wrapped = lazy(move || {
        info!("[listener] starting listener '{}'...", listen_address);
        ok(())
    }).and_then(|_| handler)
    .select2(close)
    .then(move |_| {
        info!("[pool] shutting down listener '{}'", listen_address2);
        ok(())
    });
    Ok(Box::new(wrapped))
}

fn routing_from_config<T>(
    config: ListenerConfiguration, listener: TcpListener, processor: T,
) -> Result<GenericRuntimeFuture, Error>
where
    T: RequestProcessor + Clone + Sync + Send + 'static,
    T::Message: Keyed + Clone + Send + 'static,
    T::ClientReader: Stream<Item = T::Message, Error = Error> + Send + 'static,
    T::ClientWriter: MessageSink<Message = T::Message> + Send + 'static,
    T::Future: Future<Item = (TcpStream, OrderedMessages<T::Message>), Error = Error> + Send + 'static,
{
    let mut pools = HashMap::new();
    let pool_configs = config.pools.clone();
    for (pool_name, pool_config) in pool_configs {
        debug!(
            "[listener] configuring backend pool '{}' for address '{}'",
            &pool_name,
            config.address.clone()
        );

        let mut opts = pool_config.options.unwrap_or(HashMap::new());

        let dist_type = opts
            .entry("distribution".to_owned())
            .or_insert("modulo".to_owned())
            .to_lowercase();
        let distributor = distributor::configure_distributor(&dist_type);
        debug!("[listener] using distributor '{}'", dist_type);

        let hash_type = opts
            .entry("hash".to_owned())
            .or_insert("fnv1a_64".to_owned())
            .to_lowercase();
        let hasher = hasher::configure_hasher(&hash_type);
        debug!("[listener] using hasher '{}'", hash_type);

        let pool = Arc::new(BackendPool::new(
            pool_config.addresses,
            processor.clone(),
            distributor,
            hasher,
        ));
        pools.insert(pool_name, pool);
    }

    // Figure out what sort of routing we're doing so we can grab the right handler.
    let mut routing = config.routing;
    let route_type = routing
        .entry("type".to_owned())
        .or_insert("fixed".to_owned())
        .to_lowercase();
    match route_type.as_str() {
        "fixed" => get_fixed_router(listener, pools, processor),
        x => panic!("unknown route type '{}'", x),
    }
}

fn get_fixed_router<T>(
    listener: TcpListener, pools: HashMap<String, Arc<BackendPool<T>>>, processor: T,
) -> Result<GenericRuntimeFuture, Error>
where
    T: RequestProcessor + Clone + Sync + Send + 'static,
    T::Message: Keyed + Clone + Send + 'static,
    T::ClientReader: Stream<Item = T::Message, Error = Error> + Send + 'static,
    T::ClientWriter: MessageSink<Message = T::Message> + Send + 'static,
    T::Future: Future<Item = (TcpStream, OrderedMessages<T::Message>), Error = Error> + Send + 'static,
{
    // Construct an instance of our router.
    let default_pool = pools.get("default").ok_or(Error::new(
        ErrorKind::Other,
        "no default pool configured for fixed router",
    ))?;
    let router = FixedRouter::new(processor.clone(), default_pool.clone());

    build_router_chain(listener, processor, router)
}

fn build_router_chain<T, R>(listener: TcpListener, processor: T, router: R) -> Result<GenericRuntimeFuture, Error>
where
    T: RequestProcessor + Clone + Sync + Send + 'static,
    T::Message: Keyed + Clone + Send + 'static,
    T::ClientReader: Stream<Item = T::Message, Error = Error> + Send + 'static,
    T::ClientWriter: MessageSink<Message = T::Message> + Send + 'static,
    T::Future: Future<Item = (TcpStream, OrderedMessages<T::Message>), Error = Error> + Send + 'static,
    R: Router<T> + Clone + Sync + Send + 'static,
    R::Future: Future<Item = OrderedMessages<T::Message>, Error = RouterError> + Send + 'static,
{
    let task = listener
        .incoming()
        .for_each(move |client| {
            let mut metrics = metrics::get_sink();
            metrics.increment(Metrics::ClientsConnected);

            let router = router.clone();
            let (client_rx, client_tx) = processor.get_client_streams(client);
            let client_proto = client_rx
                .map_err(|e| {
                    match e.kind() {
                        // We really only care about the "other" type which we comandeer for
                        // custom messages.  Connection resets, etc, are a fact of life and
                        // often occur when clients disconnect.  No point in logging those.
                        io::ErrorKind::Other => error!("[client] caught error while reading from client: {:?}", e),
                        _ => (),
                    }
                }).ordered_batch(128)
                .fold((router, client_tx, metrics), |(router, tx, mut ms), req| {
                    ms.update_count(Metrics::ServerMessagesReceived, req.len() as i64);

                    let batch_start = Instant::now();
                    router
                        .route(req)
                        .into_future()
                        .and_then(|res| res)
                        .map_err(|e| e.into())
                        .and_then(|res| tx.send(res))
                        .map_err(|e| {
                            match e.kind() {
                                // We really only care about the "other" type which we comandeer for
                                // custom messages.  Connection resets, etc, are a fact of life and
                                // often occur when clients disconnect.  No point in logging those.
                                io::ErrorKind::Other => error!("[client] caught error while handling request: {:?}", e),
                                _ => (),
                            }
                        }).map(move |(mc, nw, tx)| {
                            let batch_end = Instant::now();
                            ms.update_latency(Metrics::ClientMessageBatchServiced, batch_start, batch_end);
                            ms.update_count(Metrics::ServerMessagesSent, mc as i64);
                            ms.update_count(Metrics::ServerBytesSent, nw as i64);

                            (router, tx, ms)
                        })
                }).and_then(|(_, _, mut ms)| {
                    ms.decrement(Metrics::ClientsConnected);
                    ok(())
                }).map(|_| ());

            tokio::spawn(client_proto);

            ok(())
        }).map_err(|e| error!("[listener] caught error while accepting connections: {:?}", e));

    Ok(Box::new(task))
}

fn get_listener(addr_str: &String) -> io::Result<TcpListener> {
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
        .and_then(|l| TcpListener::from_std(l, &reactor::Handle::current()))
}

#[cfg(unix)]
fn configure_builder(builder: &TcpBuilder) -> io::Result<()> {
    use net2::unix::*;

    builder.reuse_port(true)?;
    Ok(())
}

#[cfg(windows)]
fn configure_builder(_builder: &TcpBuilder) -> io::Result<()> { Ok(()) }
