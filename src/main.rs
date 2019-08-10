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
#![feature(nll)]
#![feature(never_type)]
#![feature(proc_macro_hygiene)]
#![feature(async_await)]
#![feature(type_alias_impl_trait)]
#![recursion_limit = "1024"]

#[macro_use]
extern crate derivative;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate futures;

use futures_turnstyle::{Turnstyle, Waiter};
use signal_hook::iterator::Signals;
use libc::{SIGINT, SIGUSR1};
use std::thread;

extern crate tokio;
use tokio::{
    prelude::*,
    sync::{mpsc, oneshot},
};

#[macro_use]
extern crate log;
#[macro_use(slog_o)]
extern crate slog;
#[macro_use]
extern crate metrics;

use slog::Drain;

#[cfg(test)]
extern crate test;

mod backend;
mod common;
mod conf;
mod errors;
mod listener;
mod protocol;
mod routing;
mod service;
mod util;

use crate::{
    conf::{Configuration, LevelExt},
    errors::CreationError,
};
use metrics_runtime::{
    exporters::HttpExporter, observers::PrometheusBuilder, Controller, Receiver, Sink as MetricSink,
};

enum SupervisorCommand {
    Launch,
    Reload,
    Shutdown,
}

#[tokio::main]
async fn main() {
    // Set up our signal handling before anything else.
    let (mut supervisor_tx, supervisor_rx) = mpsc::unbounded_channel();
    let signals = Signals::new(&[SIGINT, SIGUSR1]).expect("failed to register signal handlers");
    thread::spawn(move || {
        // Do an initial send of the launch command to trigger actually spawning the listeners at
        // startup.
        let _ = supervisor_tx.try_send(SupervisorCommand::Launch);

        for signal in signals.forever() {
            info!("[core] signal received: {:?}", signal);

            match signal {
                libc::SIGUSR1 => {
                    let _ = supervisor_tx.try_send(SupervisorCommand::Reload);
                },
                libc::SIGINT => {
                    let _ = supervisor_tx.try_send(SupervisorCommand::Shutdown);
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
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(
        slog::LevelFilter::new(drain, slog::Level::from_str(&configuration.logging.level)).fuse(),
        slog_o!("version" => env!("GIT_HASH")),
    );

    let _scope_guard = slog_scope::set_global_logger(logger);
    slog_stdlog::init().unwrap();
    info!("[core] logging configured");

    // Configure our metrics.  We want to do this pretty early on before anything actually tries to
    // record any metrics.
    let receiver = Receiver::builder().build().expect("failed to build metrics receiver");
    let controller = receiver.get_controller();
    let sink = receiver.get_sink();
    receiver.install();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    launch_metrics(configuration.stats_addr, controller, shutdown_rx);
    launch_supervisor(supervisor_rx, shutdown_tx, sink);

    info!("[core] synchrotron running");
}

fn launch_supervisor(
    supervisor_rx: mpsc::UnboundedReceiver<SupervisorCommand>, shutdown_tx: oneshot::Sender<()>, sink: MetricSink,
) {
    let turnstyle = Turnstyle::new();
    let supervisor = supervisor_rx
        .map_err(|_| CreationError::ListenerSpawnFailed)
        .fold(turnstyle, move |ts, command| {
            match command {
                SupervisorCommand::Launch => {
                    let (version, waiter) = ts.join();
                    launch_listeners(version, waiter, sink.clone())?;
                    counter!("supervisor.configuration_loads", 1);
                },
                SupervisorCommand::Reload => {
                    let (version, waiter) = ts.join();
                    launch_listeners(version, waiter, sink.clone())?;
                    ts.turn();
                    counter!("supervisor.configuration_loads", 1);
                },
                SupervisorCommand::Shutdown => {
                    ts.turn();
                },
            }

            Ok(ts)
        })
        .then(move |result| {
            if let Err(e) = result {
                error!("[core supervisor] caught an error during launch/reload: {}", e);
            }

            shutdown_tx.send(())
        })
        .untyped();

    tokio::spawn(supervisor);
}

fn launch_listeners(version: usize, close: Waiter, sink: MetricSink) -> Result<(), CreationError> {
    let configuration = Configuration::new().expect("failed to parse configuration");
    let closer = close.shared();
    let listeners = configuration
        .listeners
        .into_iter()
        .map(|(name, config)| {
            let close = closer.clone();

            listener::from_config(version, name, config, close, sink.clone())
        })
        .collect::<Vec<_>>();

    let mut errors = Vec::new();
    for listener in &listeners {
        let result = listener.as_ref();
        if result.is_err() {
            let error = result.err().unwrap();
            errors.push(error.to_string());
        }
    }

    if !errors.is_empty() {
        error!("[core] encountered errors while spawning listeners:");
        for error in errors {
            error!("[core] - {}", error);
        }

        return Err(CreationError::ListenerSpawnFailed);
    }

    // Launch all these listeners into the runtime.
    for listener in listeners {
        tokio::spawn(listener.unwrap());
    }

    Ok(())
}

fn launch_metrics(stats_addr: String, controller: Controller, shutdown_rx: impl Future) {
    let addr = stats_addr.parse().expect("failed to parse metrics listen address");
    let exporter = HttpExporter::new(controller, PrometheusBuilder::new(), addr);
    let task = exporter.into_future().select2(shutdown_rx).untyped();
    tokio::spawn(task);
}
