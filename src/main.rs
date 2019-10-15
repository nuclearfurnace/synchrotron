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
#![feature(async_closure)]
#![feature(type_alias_impl_trait)]
#![recursion_limit = "1024"]

#[macro_use]
extern crate derivative;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tracing;
#[macro_use]
extern crate metrics;

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

use futures_turnstyle::{Turnstyle, Waiter};
use libc::{SIGINT, SIGUSR1};
use signal_hook::iterator::Signals;
use std::thread;

use tokio::{
    prelude::*,
    sync::{mpsc, oneshot},
};

use crate::{
    conf::Configuration,
    errors::CreationError,
};
use futures::select;
use metrics_runtime::{
    exporters::HttpExporter, observers::PrometheusBuilder, Controller, Receiver, Sink as MetricSink,
};

use tracing::Dispatch;
use tracing_fmt::FmtSubscriber;

#[derive(Debug)]
enum SupervisorCommand {
    Launch,
    Reload,
    Shutdown,
}

fn main() {
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

    // Configure tracing and the default stdout subscriber.
    let fmt = FmtSubscriber::new();
    let dispatch = Dispatch::new(fmt);
    tracing::dispatcher::set_global_default(dispatch).expect("failed to set tracing subscriber");
    info!("logging configured");

    // Configure our metrics.  We want to do this pretty early on before anything actually tries to
    // record any metrics.
    let receiver = Receiver::builder().build().expect("failed to build metrics receiver");
    let controller = receiver.get_controller();
    let sink = receiver.get_sink();
    receiver.install();

    // Build our runtime, and spawn our metrics and our supervisor on to it.  The supervisor will
    // actually spawn the configured listeners when it runs.
    //let runtime = Builder::new()
    //    .name_prefix("synchrotron-thread-pool-")
    //    .build()
    //    .unwrap();

    let mut runtime = tokio::runtime::current_thread::Runtime::new().unwrap();
    runtime.spawn(async move {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        launch_metrics(configuration.stats_addr, controller, shutdown_rx);
        launch_supervisor(supervisor_rx, shutdown_tx, sink);
    });
    //runtime.shutdown_on_idle();
    runtime.run().unwrap();
}

fn launch_supervisor(
    mut supervisor_rx: mpsc::UnboundedReceiver<SupervisorCommand>,
    shutdown_tx: oneshot::Sender<()>,
    sink: MetricSink,
) {
    let turnstyle = Turnstyle::new();
    tokio::spawn(async move {
        info!("supervisor running");
        while let Some(command) = supervisor_rx.next().await {
            match command {
                SupervisorCommand::Launch => {
                    let (version, waiter) = turnstyle.join();
                    if let Err(e) = launch_listeners(version, waiter, sink.clone()) {
                        error!("caught an error during launch/reload: {}", e);
                        break
                    }
                    counter!("supervisor.configuration_loads", 1);
                },
                SupervisorCommand::Reload => {
                    let (version, waiter) = turnstyle.join();
                    if let Err(e) = launch_listeners(version, waiter, sink.clone()) {
                        error!("caught an error during launch/reload: {}", e);
                        break
                    }
                    turnstyle.turn();
                    counter!("supervisor.configuration_loads", 1);
                },
                SupervisorCommand::Shutdown => {
                    turnstyle.turn();
                },
            }
        }

       let _ = shutdown_tx.send(());
    });
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
        for err in errors {
            error!(message = "failed to spawn listener", error = ?err);
        }

        return Err(CreationError::ListenerSpawnFailed);
    }

    // Launch all these listeners into the runtime.
    for listener in listeners {
        tokio::spawn(listener.unwrap());
    }

    Ok(())
}

fn launch_metrics(stats_addr: String, controller: Controller, shutdown_rx: impl Future + Send + Unpin + 'static) {
    let addr = stats_addr.parse().expect("failed to parse metrics listen address");
    let exporter = HttpExporter::new(controller, PrometheusBuilder::new(), addr);

    tokio::spawn(async move {
        let mut server = exporter.into_future().fuse();
        let mut shutdown = shutdown_rx.fuse();

        select! {
            _ = server => {},
            _ = shutdown => {},
        }
    });
}
