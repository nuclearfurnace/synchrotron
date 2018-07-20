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
    distributor, hasher, pool::BackendPool, redis::{generate_batched_redis_writes, RedisRequestTransformer},
};
use conf::ListenerConfiguration;
use futures::{
    future::{join_all, lazy, ok, Shared}, prelude::*,
};
use futures_turnstyle::Waiter;
use metrics;
use metrics::Metrics;
use net2::TcpBuilder;
use protocol::redis;
use std::{
    collections::HashMap, io::{Error, ErrorKind}, net::SocketAddr, sync::Arc,
    time::Instant,
};
use tokio::{io, net::TcpListener, reactor::Handle, runtime::TaskExecutor};
use tokio_io::AsyncRead;
use util::{flatten_ordered_messages, get_batch_size, StreamExt};

type GenericRuntimeFuture = Box<Future<Item = (), Error = ()> + Sync + Send + 'static>;

/// Creates a listener from the given configuration.
///
/// The listener will spawn a socket for accepting client connections, and when a client connects,
/// spawn a task to process all of the messages from that client until the client disconnects or
/// there is an unrecoverable connection/protocol error.
pub fn from_config(
    reactor: Handle, executor: TaskExecutor, config: ListenerConfiguration, close: Shared<Waiter>,
) -> Result<GenericRuntimeFuture, Error> {
    // Create the actual listener proper.
    let listen_address = config.address.clone();
    let listener = get_listener(&listen_address, &reactor).expect("failed to create the TCP listener");

    // Get the correct handler based on protocol.
    let protocol = config.protocol.to_lowercase();
    let handler = match protocol.as_str() {
        "redis" => redis_from_config(executor, config, listener)?,
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

fn redis_from_config(
    executor: TaskExecutor, config: ListenerConfiguration, listener: TcpListener,
) -> Result<GenericRuntimeFuture, Error> {
    // Gather up all of the backend pools.
    let mut pools = HashMap::new();
    let pool_configs = config.pools.clone();
    for (pool_name, mut pool_config) in pool_configs {
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

        let transformer = RedisRequestTransformer::new();

        let pool = Arc::new(BackendPool::new(
            executor.clone(),
            pool_config.addresses,
            transformer,
            distributor,
            hasher,
        ));
        pools.insert(pool_name, pool);
    }

    // Figure out what sort of routing we're doing so we can grab the right handler.
    let routing_type = config.routing.to_lowercase();
    match routing_type.as_str() {
        "warmup" => redis_warmup_handler(executor, listener, pools),
        _ => redis_normal_handler(executor, listener, pools),
    }
}

fn redis_warmup_handler(
    executor: TaskExecutor, listener: TcpListener, pools: HashMap<String, Arc<BackendPool<RedisRequestTransformer>>>,
) -> Result<GenericRuntimeFuture, Error> {
    let warm_pool = pools
        .get("warm")
        .ok_or(Error::new(
            ErrorKind::Other,
            "redis warmup handler has no 'warm' pool configured!",
        ))?
        .clone();

    let cold_pool = pools
        .get("cold")
        .ok_or(Error::new(
            ErrorKind::Other,
            "redis warmup handler has no 'cold' pool configured!",
        ))?
        .clone();

    let handler = listener
        .incoming()
        .map_err(|e| error!("[pool] accept failed: {:?}", e))
        .for_each(move |socket| {
            let client_addr = socket.peer_addr().unwrap();
            debug!("[client] connection established -> {:?}", client_addr);

            let cold = cold_pool.clone();
            let warm = warm_pool.clone();
            let executor2 = executor.clone();

            let (client_rx, client_tx) = socket.split();
            let client_proto = redis::read_messages_stream(client_rx)
                .map_err(|e| {
                    error!("[client] caught error while reading from client: {:?}", e);
                })
                .batch(128)
                .fold(client_tx, move |tx, msgs| {
                    trace!("[client] got batch of {} messages!", msgs.len());

                    // Fire off our cold pool operations asynchronously so that we don't influence
                    // the normal client path.
                    let cold_msgs = msgs.clone();
                    let cold_batches = generate_batched_redis_writes(&cold, cold_msgs);
                    let cold_handler = join_all(cold_batches)
                        .map_err(|err| error!("[client] error while sending warming ops to cold pool: {:?}", err))
                        .map(|_| ());

                    executor2.spawn(cold_handler);

                    // Now run our normal writes.
                    let warm_handler = join_all(generate_batched_redis_writes(&warm, msgs))
                        .and_then(|results| ok(flatten_ordered_messages(results)))
                        .map_err(|_| Error::new(ErrorKind::Other, "internal synchrotron error (WRJE)"))
                        .and_then(move |items| redis::write_messages(tx, items))
                        .map(|(w, _n)| w)
                        .map_err(|err| error!("[client] caught error while handling request: {:?}", err));

                    warm_handler
                })
                .map(|_| ());

            executor.spawn(client_proto);
            ok(())
        });

    Ok(Box::new(handler))
}

fn redis_normal_handler(
    executor: TaskExecutor, listener: TcpListener, pools: HashMap<String, Arc<BackendPool<RedisRequestTransformer>>>,
) -> Result<GenericRuntimeFuture, Error> {
    let default_pool = pools
        .get("default")
        .ok_or(Error::new(
            ErrorKind::Other,
            "redis normal handler has no 'default' pool configured!",
        ))?
        .clone();

    let handler = listener
        .incoming()
        .map_err(|e| error!("[pool] accept failed: {:?}", e))
        .for_each(move |socket| {
            let mut metrics = metrics::get_sink();

            let client_addr = socket.peer_addr().unwrap();
            debug!("[client] connection established -> {:?}", client_addr);
            metrics.increment(Metrics::ClientsConnected);

            let default = default_pool.clone();

            let (client_rx, client_tx) = socket.split();
            let client_proto = redis::read_messages_stream(client_rx)
                .map_err(|e| {
                    let mut ms = metrics::get_sink();
                    ms.decrement(Metrics::ClientsConnected);
                    error!("[client] caught error while reading from client: {:?}", e);
                })
                .batch(128)
                .fold((client_tx, client_addr, metrics, 0), move |(tx, addr, mut ms, msg_count), msgs| {
                    let batch_start = Instant::now();
                    trace!("[client] [{:?}] got batch of {} messages!", addr, msgs.len());

                    let batch_size_bytes = get_batch_size(&msgs);
                    ms.update_count(Metrics::ServerBytesReceived, batch_size_bytes as i64);

                    let new_msg_count = msg_count + msgs.len();
                    ms.update_count(Metrics::ServerMessagesReceived, msgs.len() as i64);

                    join_all(generate_batched_redis_writes(&default, msgs))
                        .and_then(|results| ok(flatten_ordered_messages(results)))
                        .map_err(|_| Error::new(ErrorKind::Other, "internal synchrotron error (NRJE)"))
                        .and_then(move |items| {
                            ms.update_count(Metrics::ServerMessagesSent, items.len() as i64);

                            redis::write_messages(tx, items)
                                .map(|(w, n)| (w, n, ms))
                        })
                        .map(move |(w, n, mut ms)| {
                            let batch_finished = Instant::now();

                            debug!(
                                "[client] [{:?}] sent batch of responses to client",
                                addr,
                            );

                            ms.update_count(Metrics::ServerBytesSent, n as i64);
                            ms.update_latency(Metrics::ClientMessageBatchServiced, batch_start, batch_finished);

                            (w, addr, ms, new_msg_count)
                        })
                        .map_err(|err| error!("[client] caught error while handling request: {:?}", err))
                })
                .map(|(_, addr, mut ms, msg_count)| {
                    ms.decrement(Metrics::ClientsConnected);
                    debug!("[client] connection complete -> {:?}; processed {} messages", addr, msg_count);
                    ()
                });

            executor.spawn(client_proto);
            ok(())
        });

    Ok(Box::new(handler))
}

fn get_listener(addr_str: &String, handle: &Handle) -> io::Result<TcpListener> {
    let addr = addr_str.parse().unwrap();
    let builder = match addr {
        SocketAddr::V4(_) => TcpBuilder::new_v4()?,
        SocketAddr::V6(_) => TcpBuilder::new_v6()?,
    };
    configure_builder(&builder)?;
    builder.reuse_address(true)?;
    builder.bind(addr)?;
    builder.listen(1024).and_then(|l| TcpListener::from_std(l, handle))
}

#[cfg(unix)]
fn configure_builder(builder: &TcpBuilder) -> io::Result<()> {
    use net2::unix::*;

    builder.reuse_port(true)?;
    Ok(())
}

#[cfg(windows)]
fn configure_builder(_builder: &TcpBuilder) -> io::Result<()> { Ok(()) }
