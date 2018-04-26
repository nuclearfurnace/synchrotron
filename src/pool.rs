use std::io::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio;
use tokio::io;
use tokio::reactor::Handle;
use tokio::net::TcpStream;
use tokio_io::AsyncRead;
use tokio_io::codec::{Framed, BytesCodec};
use futures::{future, Stream};
use futures::future::Future;
use rs_futures_spmc::Receiver;

use listener;
use protocol;
use config::PoolConfiguration;
use backend::pool::{RandomDistributor, BackendPool};

pub fn from_config(reactor: Handle, config: PoolConfiguration, close: Receiver<()>) -> impl Future<Item=(), Error=()> {
    let listen_address = config.address.clone();
    let backend_addresses = config.backends.clone();
    let distributor = RandomDistributor::new();
    let backend_pool = Arc::new(Mutex::new(BackendPool::new(backend_addresses, distributor)));

    let listener = listener::get_listener(&listen_address, &reactor).unwrap();
    listener.incoming()
        .map_err(|e| error!("[pool] accept failed: {:?}", e))
        .for_each(move |socket| {
            let clone_pool = backend_pool.clone();
            let (client_rx, client_tx) = socket.split();
            let proto = protocol::RedisClientProtocol::new(client_rx)
                .map_err(|e| { error!("[client] caught error while reading from client: {:?}", e); })
                .for_each(move |cmd| {
                    let mut pool = clone_pool.lock().unwrap();
                    let backend = (*pool).get();
                    let process = backend
                        .and_then(move |server| io::write_all(server, cmd.buf()))
                        .and_then(|(server, _)| io::copy(server, client_tx))
                        .map(|(n, _, _)| info!("wrote {} bytes from server to client", n))
                        .map_err(|err| error!("IO error {:?}", err));

                    tokio::spawn(process)
                });

            tokio::spawn(proto)
        })
        .select2(close.into_future())
        .then(move |_| {
            info!("[pool] shutting down listener '{}'", listen_address);
            future::ok(())
        })
}

fn framed_tcp_connect(addr: SocketAddr) -> impl Future<Item=Framed<TcpStream, BytesCodec>, Error=Error> {
    TcpStream::connect(&addr)
        .map(|sock| sock.framed(BytesCodec::new()))
}
