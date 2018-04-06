use tokio;
use tokio::io::*;
use tokio::reactor::Handle;
use futures::{future, Stream};
use futures::future::Future;
use rs_futures_spmc::Receiver;

use listener;
use config::PoolConfiguration;

pub fn from_config(reactor: Handle, config: PoolConfiguration, close: Receiver<()>) -> impl Future<Item=(), Error=()> {
    let address = config.address.clone();
    let listener = listener::get_listener(&address, &reactor).unwrap();
    listener.incoming()
        .map_err(|e| error!("accept failed = {:?}", e))
        .for_each(|sock| {
            let (reader, writer) = sock.split();
            let bytes_copied = copy(reader, writer);
            let handle_conn = bytes_copied.map(|amt| {
                info!("wrote {:?} bytes", amt)
            }).map_err(|e| {
                error!("IO error {:?}", e)
            });

            tokio::spawn(handle_conn)
        })
        .select2(close.into_future())
        .then(move |_| {
            info!("[pool] shutting down listener '{}'", address);
            future::ok(())
        })
}
