use backend::pool::BackendPool;
use backend::redis::generate_batched_writes;
use backend::{distributor, hasher};
use conf::PoolConfiguration;
use futures::future::{join_all, ok};
use futures::prelude::*;
use net2::TcpBuilder;
use protocol::redis;
use rs_futures_spmc::Receiver;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio;
use tokio::io;
use tokio::net::TcpListener;
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

    let listener = get_listener(&listen_address, &reactor).unwrap();
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

fn get_listener(addr_str: &String, handle: &Handle) -> io::Result<TcpListener> {
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
        .and_then(|l| TcpListener::from_std(l, handle))
}

#[cfg(unix)]
fn configure_builder(builder: &TcpBuilder) -> io::Result<()> {
    use net2::unix::*;

    builder.reuse_port(true)?;
    Ok(())
}

#[cfg(windows)]
fn configure_builder(_builder: &TcpBuilder) -> io::Result<()> {
    Ok(())
}
