use backend::pool::BackendPool;
use backend::redis::generate_batched_writes;
use backend::{distributor, hasher};
use conf::PoolConfiguration;
use futures::future::{join_all, ok};
use futures::prelude::*;
use listener;
use protocol::redis;
use rs_futures_spmc::Receiver;
use std::sync::Arc;
use tokio;
use tokio::reactor::Handle;
use tokio_io::AsyncRead;
use util::{flatten_ordered_messages, StreamExt};

/// Creates a listener from the given configuration.
///
/// The listener will spawn a socket for accepting client connections, and when a client connects,
/// spawn a task to process all of the messages from that client until the client disconnects or
/// there is an unrecoverable connection/protocol error.
pub fn from_config(
    reactor: Handle,
    config: PoolConfiguration,
    close: Receiver<()>,
) -> impl Future<Item = (), Error = ()> {
    let protocol = config.protocol.to_lowercase();
    match protocol.as_str() {
        "redis" => redis_from_config(reactor, config, close),
        s => panic!("unknown protocol type: {}", s),
    }
}

fn redis_from_config(
    reactor: Handle,
    mut config: PoolConfiguration,
    close: Receiver<()>,
) -> impl Future<Item = (), Error = ()> {
    let listen_address = config.address.clone();
    let backend_addresses = config.backends.clone();

    let dist_type = config
        .options
        .entry("distribution".to_owned())
        .or_insert("random".to_owned())
        .to_lowercase();
    let distributor = distributor::configure_distributor(dist_type);

    let hash_type = config
        .options
        .entry("hash".to_owned())
        .or_insert("md5".to_owned())
        .to_lowercase();
    let hasher = hasher::configure_hasher(hash_type);

    let backend_pool = Arc::new(BackendPool::new(backend_addresses, distributor, hasher));

    let listener = listener::get_listener(&listen_address, &reactor).unwrap();
    listener
        .incoming()
        .map_err(|e| error!("[pool] accept failed: {:?}", e))
        .for_each(move |socket| {
            let client_addr = socket.peer_addr().unwrap();
            debug!("[client] connection established -> {:?}", client_addr);

            let pool = backend_pool.clone();
            let (client_rx, client_tx) = socket.split();
            let client_proto = redis::read_messages_stream(client_rx)
                .map_err(|e| {
                    error!("[client] caught error while reading from client: {:?}", e);
                })
                .batch(128)
                .fold(client_tx, move |tx, msgs| {
                    debug!("[client] got batch of {} messages!", msgs.len());

                    join_all(generate_batched_writes(&pool, msgs))
                        .and_then(|results| ok(flatten_ordered_messages(results)))
                        .and_then(move |items| redis::write_messages(tx, items))
                        .map(|(w, _n)| w)
                        .map_err(|err| {
                            error!("[client] caught error while handling request: {:?}", err)
                        })
                })
                .map(|_| ());

            tokio::spawn(client_proto)
        })
        .select2(close.into_future())
        .then(move |_| {
            info!("[pool] shutting down listener '{}'", listen_address);
            ok(())
        })
}
