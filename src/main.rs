#![recursion_limit = "1024"]

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

extern crate chan;
extern crate chan_signal;

use chan_signal::Signal;

extern crate tokio;
extern crate futures;
extern crate rs_futures_spmc;
extern crate net2;

use tokio::prelude::*;
use tokio::io::copy;
use tokio::reactor::Handle;
use rs_futures_spmc::channel;

#[macro_use]
extern crate log;
#[macro_use(slog_o, slog_kv)]
extern crate slog;
extern crate slog_stdlog;
extern crate slog_scope;
extern crate slog_term;
extern crate slog_async;

use slog::Drain;

use std::thread;

mod config;
mod listener;

use config::Configuration;

fn main() {
    // Due to the way signal masking apparently works, or works with this library, we
    // must initialize our signal handling code before *any* threads are spun up by
    // the process, otherwise we don't seem to get them delivered to us.
    //
    // We also have this accessory thread because trying to wrap the channel as a stream
    // was fraught with pain and this is much simpler.  C'est la vie.
    let signals = chan_signal::notify(&[Signal::USR1, Signal::INT]);
    let (close_tx, close_rx) = channel::<()>(1);
    thread::spawn(move || {
        loop {
            let signal = signals.recv().unwrap();
            debug!("[core] signal received: {:?}", signal);

            match signal {
                Signal::USR1 => {
                    // signal to spawn new process
                },
                Signal::INT => {
                    // signal to close this process
                    let _ = close_tx.send(()).wait();
                    break;
                },
                _ => {
                    // we don't care about the rest
                }
            }
        }
    });

    // Configure our logging.  This gives us fully asynchronous logging to the terminal
    // which is also level filtered.  As well, we've replaced the global std logger
    // and pulled in helper macros that correspond to the various logging levels.
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(
        slog::LevelFilter::new(drain, slog::Level::Debug).fuse(),
        slog_o!("version" => env!("CARGO_PKG_VERSION")));

    let _scope_guard = slog_scope::set_global_logger(logger);
    let _log_guard = slog_stdlog::init().unwrap();
    info!("[core] logging configured");

    tokio::run(future::lazy(move || {
        // set up our listeners
        let configuration = Configuration::from_path("synchrotron.json")
            .unwrap();

        for pool_config in configuration.pools {
            let close = close_rx.clone();
            let pool_address = pool_config.pool_address.clone();
            let reactor = Handle::current();
            let listener = listener::get_listener(&pool_address, &reactor).unwrap();
            let server = listener.incoming()
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
                .then(|_| {
                    info!("[pool] shutting down listener");
                    future::ok(())
                });

            tokio::spawn(server);

            info!("[pool] listening on {}...", pool_address);
        }

        Ok(())
    }));
}
