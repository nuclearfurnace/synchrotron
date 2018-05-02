use std::io;
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
            info!("[client] connection established -> {:?}", client_addr);

            let pool = backend_pool.clone();
            let (client_rx, client_tx) = socket.split();
            let client_proto = redis::read_client_commands(client_rx)
                .map_err(|e| { error!("[client] caught error while reading from client: {:?}", e); })
                .fold(client_tx, move |tx, cmd| {
                    info!("got command");

                    // Backend is a stream that we split so we can "receive" a connection
                    // from it and then "send" it back.
                    let (conn_tx, conn_rx) = pool.get().split();

                    conn_rx.take(1) // grab a connection
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                        .and_then(move |server| redis::write_resp(server, cmd)) // write client command to backend
                        .and_then(|(server, _)| redis::read_resp(server)) // wait until we get a response from backend
                        .and_then(move |(server, res)| {
                            redis::write_resp(tx, res) // write that response to client
                                .join(conn_tx.send(server)) // and also send back the connection
                        })
                        .map(|((w, n), _)| {
                            info!("wrote {} bytes from server to client", n);
                            w
                        })
                        .map_err(|err| error!("IO error {:?}", err))
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
