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
use hotmic::{Controller, Facet, Receiver, Sample, Sink, Snapshot};
use std::{fmt, io, thread, time::Instant};

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Metrics {
    ClientsConnected,
    ServerBytesReceived,
    ServerBytesSent,
    ServerMessagesReceived,
    ServerMessagesSent,
    ClientMessageBatchServiced,
    GenerateBatchedRedisWrite,
    BackendNewConnections,
    ClientTxErrors,
}

impl fmt::Display for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Metrics::ClientsConnected => write!(f, "clients_connected"),
            Metrics::ServerBytesReceived => write!(f, "server_bytes_recv"),
            Metrics::ServerBytesSent => write!(f, "server_bytes_sent"),
            Metrics::ServerMessagesReceived => write!(f, "server_msgs_recv"),
            Metrics::ServerMessagesSent => write!(f, "server_msgs_sent"),
            Metrics::ClientMessageBatchServiced => write!(f, "client_msg_batch_serviced"),
            Metrics::GenerateBatchedRedisWrite => write!(f, "generate_batched_redis_write"),
            Metrics::BackendNewConnections => write!(f, "backend_new_connections"),
            Metrics::ClientTxErrors => write!(f, "client_tx_errors"),
        }
    }
}

lazy_static! {
    static ref METRICS: MetricsFacade = {
        let receiver = Receiver::builder()
            .batch_size(1)
            .build();

        let facade = MetricsFacade::new(&receiver);

        // Spawn our actual processing loop.
        thread::spawn(move || run_metrics_loop(receiver));

        facade
    };
}

pub fn get_facade() -> &'static MetricsFacade { &METRICS }

pub fn get_sink() -> MetricSink {
    let facade = get_facade();
    facade.get_sink()
}

pub struct MetricsFacade {
    sink: Sink<Metrics>,
    controller: Controller<Metrics>,
}

impl MetricsFacade {
    pub fn new(receiver: &Receiver<Metrics>) -> MetricsFacade {
        MetricsFacade {
            sink: receiver.get_sink(),
            controller: receiver.get_controller(),
        }
    }

    pub fn get_snapshot(&self) -> Result<Snapshot<Metrics>, io::Error> { self.controller.get_snapshot() }

    pub fn get_sink(&self) -> MetricSink {
        MetricSink {
            sink: self.sink.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetricSink {
    sink: Sink<Metrics>,
}

impl MetricSink {
    pub fn update_latency(&mut self, key: Metrics, start: Instant, stop: Instant) {
        self.sink.send(Sample::Timing(key, start, stop, 1)).unwrap()
    }

    pub fn update_count(&mut self, key: Metrics, value: i64) { self.sink.send(Sample::Count(key, value)).unwrap() }

    pub fn increment(&mut self, key: Metrics) { self.update_count(key, 1) }

    pub fn decrement(&mut self, key: Metrics) { self.update_count(key, -1) }
}

fn run_metrics_loop(mut receiver: Receiver<Metrics>) {
    // Register our facets.  There should be a better place for this.  Until then...
    receiver.add_facet(Facet::Count(Metrics::ClientsConnected));
    receiver.add_facet(Facet::Count(Metrics::ServerMessagesSent));
    receiver.add_facet(Facet::Count(Metrics::ServerMessagesReceived));
    receiver.add_facet(Facet::Count(Metrics::ServerBytesSent));
    receiver.add_facet(Facet::Count(Metrics::ServerBytesReceived));
    receiver.add_facet(Facet::TimingPercentile(Metrics::ClientMessageBatchServiced));
    receiver.add_facet(Facet::Count(Metrics::ClientMessageBatchServiced));
    receiver.add_facet(Facet::TimingPercentile(Metrics::GenerateBatchedRedisWrite));
    receiver.add_facet(Facet::Count(Metrics::GenerateBatchedRedisWrite));
    receiver.add_facet(Facet::Count(Metrics::BackendNewConnections));
    receiver.add_facet(Facet::Count(Metrics::ClientTxErrors));

    receiver.run();
}
