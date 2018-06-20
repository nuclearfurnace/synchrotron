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
#![feature(test)]
#![feature(iterator_flatten)]
#![feature(associated_type_defaults)]
#![feature(duration_as_u128)]
#![recursion_limit = "1024"]

extern crate config;
extern crate crypto;
extern crate pruefung;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

extern crate chan;
extern crate chan_signal;

use chan_signal::Signal;

extern crate tokio;
extern crate tokio_io;
#[macro_use]
extern crate futures;
extern crate net2;
extern crate rs_futures_spmc;

use rs_futures_spmc::channel;
use std::{error::Error, process, thread};
use tokio::{executor::thread_pool, prelude::*, runtime};

#[macro_use]
extern crate log;
#[macro_use(slog_o, slog_kv)]
extern crate slog;
extern crate slog_async;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

use slog::Drain;

extern crate btoi;
extern crate bytes;
extern crate itoa;
extern crate rand;

#[cfg(test)]
extern crate test;

#[cfg(test)]
extern crate spectral;

mod backend;
mod conf;
mod listener;
mod protocol;
mod util;

use conf::{Configuration, LevelExt};

fn run() -> i32 {
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
            info!("[core] signal received: {:?}", signal);

            match signal {
                Signal::USR1 => {}, // signal to spawn new process
                Signal::INT => {
                    // signal to close this process
                    let _ = close_tx.send(()).wait();
                    break;
                },
                _ => {}, // we don't care about the rest
            }
        }
    });

    let configuration = Configuration::new().expect("failed to parse configuration");

    // Configure our logging.  This gives us fully asynchronous logging to the terminal
    // which is also level filtered.  As well, we've replaced the global std logger
    // and pulled in helper macros that correspond to the various logging levels.
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(
        slog::LevelFilter::new(drain, slog::Level::from_str(&configuration.logging.level)).fuse(),
        slog_o!(),
    );

    let _scope_guard = slog_scope::set_global_logger(logger);
    let _log_guard = slog_stdlog::init().unwrap();
    info!("[core] logging configured");

    // Now run.
    let mut threadpool_builder = thread_pool::Builder::new();
    threadpool_builder.name_prefix("my-runtime-worker-").pool_size(4);

    let mut runtime = runtime::Builder::new()
        .threadpool_builder(threadpool_builder)
        .build()
        .expect("failed to create tokio runtime");

    let reactor = runtime.reactor().clone();
    let executor = runtime.executor().clone();

    let listeners = configuration
        .listeners
        .into_iter()
        .map(|x| {
            let close = close_rx.clone();
            let config = x.clone();
            let reactor2 = reactor.clone();
            let executor2 = executor.clone();

            listener::from_config(reactor2, executor2, config, close)
        })
        .collect::<Vec<_>>();

    let mut errors = Vec::new();
    for listener in &listeners {
        let result = listener.as_ref();
        if result.is_err() {
            let error = result.err().unwrap();
            errors.push(error.description().to_owned());
        }
    }

    if errors.len() > 0 {
        error!("[core] encountered errors while spawning listeners:");

        for error in errors {
            error!("[core] - {}", error);
        }

        return 1;
    }

    // Launch all these listeners into the runtime.
    for listener in listeners {
        runtime.spawn(listener.unwrap());
    }

    info!("[core] synchrotron running");

    runtime.shutdown_on_idle().wait().unwrap();
    return 0;
}

fn main() {
    let code = run();
    process::exit(code);
}
