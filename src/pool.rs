use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio;
use tokio::reactor::Handle;
use tokio_io::AsyncRead;
use futures::prelude::*;
use futures::future::join_all;
use rs_futures_spmc::Receiver;

use listener;
use protocol::redis;
use conf::PoolConfiguration;
use backend::pool::{BackendPool, RandomDistributor, MD5Hasher};
use backend::redis::RedisBackendQueue;
use util::ext::StreamExt;

pub fn from_config(reactor: Handle, config: PoolConfiguration, close: Receiver<()>) -> impl Future<Item=(), Error=()> {
    let listen_address = config.address.clone();
    let backend_addresses = config.backends.clone();
    let distributor = RandomDistributor::new();
    let hasher = MD5Hasher::new();
    let backend_pool = Arc::new(BackendPool::new(backend_addresses, distributor, hasher));

    let listener = listener::get_listener(&listen_address, &reactor).unwrap();
    listener.incoming()
        .map_err(|e| error!("[pool] accept failed: {:?}", e))
        .for_each(move |socket| {
            let client_addr = socket.peer_addr().unwrap();
            debug!("[client] connection established -> {:?}", client_addr);

            let pool = backend_pool.clone();
            let (client_rx, client_tx) = socket.split();
            let client_proto = redis::read_client_messages(client_rx)
                .map_err(|e| { error!("[client] caught error while reading from client: {:?}", e); })
                .batch(128)
                .fold(client_tx, move |tx, msgs| {
                    let queues = RedisBackendQueue::to_queues(&pool, msgs);
                    join_all(queues)
                        .and_then(|results| {
                            let mut items = results.into_iter()
                                .flatten()
                                .collect::<Vec<_>>();

                            items.sort_by(|(idx, _msg)| idx);
                            items
                        })
                        .and_then(move |items| redis::write_messages(tx, items))
                        .map_err(|err| error!("[client] caught error while handling request: {:?}", err))
                })
                .map(|_| ());

            tokio::spawn(client_proto)
        })
        .select2(close.into_future())
        .then(move |_| {
            info!("[pool] shutting down listener '{}'", listen_address);
            future::ok(())
        })
}
