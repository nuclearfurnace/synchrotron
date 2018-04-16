use std::io::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio;
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
        .for_each(|socket| {
            let clone_pool = backend_pool.clone();
            let proto = protocol::RedisClientProtocol::new(socket)
                .map_err(|e| { error!("[client] caught error while reading from client: {:?}", e); })
                .for_each(|cmd| {
                    info!("got command: {:?}", cmd);
                    let mut pool = backend_pool.lock().unwrap();
                    let mut stream = pool.get();

                    Ok(())
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
