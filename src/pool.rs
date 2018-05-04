use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio;
use tokio::reactor::Handle;
use tokio_io::AsyncRead;
use futures::prelude::*;
use futures::future;
use rs_futures_spmc::Receiver;

use listener;
use protocol::redis;
use config::PoolConfiguration;
use backend::pool::{RandomDistributor, BackendPool};

pub fn from_config(reactor: Handle, config: PoolConfiguration, close: Receiver<()>) -> impl Future<Item=(), Error=()> {
    let listen_address = config.address.clone();
    let backend_addresses = config.backends.clone();
    let distributor = RandomDistributor::new();
    let backend_pool = Arc::new(BackendPool::new(backend_addresses, distributor));

    let listener = listener::get_listener(&listen_address, &reactor).unwrap();
    listener.incoming()
        .map_err(|e| error!("[pool] accept failed: {:?}", e))
        .for_each(move |socket| {
            let client_addr = socket.peer_addr().unwrap();
            debug!("[client] connection established -> {:?}", client_addr);

            let pool = backend_pool.clone();
            let (client_rx, client_tx) = socket.split();
            let client_proto = redis::read_client_commands(client_rx)
                .map_err(|e| { error!("[client] caught error while reading from client: {:?}", e); })
                .fold(client_tx, move |tx, cmd| {
                    let subscription = pool.get();
                    let (conn_tx, conn_rx) = subscription.split();

                    conn_rx.take(1).into_future()
                        .map_err(|(err, _)| err)
                        .and_then(|(conn, _)| conn.unwrap())
                        .and_then(move |server| redis::write_resp(server, cmd))
                        .and_then(|(server, _)| redis::read_resp(server))
                        .and_then(move |(server, res)| {
                            redis::write_resp(tx, res)
                                .join(conn_tx.send(server)
                                      .map_err(|_| Error::new(ErrorKind::Other, "failed to return backend connection to pool")))
                        })
                        .map(|((w, n), _)| w)
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
