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
use crate::{
    backend::{message_queue::MessageQueue, processor::Processor},
    common::{EnqueuedRequests, PendingResponse, Message},
    service::PipelineError,
    util::{FutureExt, Timed, Sizable},
    protocol::errors::ProtocolError,
};
use bytes::BytesMut;
use futures::prelude::*;
use metrics_runtime::{
    data::{Counter, Histogram},
    Sink as MetricSink,
};
use std::collections::VecDeque;
use tower_service::Service;
use std::task::Poll;
use crate::service::DrivenService;

/// Pipeline-capable service base.
///
/// `Pipeline` can simultaneously drive a `Transport` and an underlying `Service`,
/// opportunistically batching messages from the client transport and handing them off for
/// processing while waiting to send back to the responses.
pub struct Pipeline<T, S, P>
where
    T: Sink<BytesMut> + Stream<Item = Result<P::Message, ProtocolError>> + Unpin,
    S: DrivenService<EnqueuedRequests<P::Message>>,
    P: Processor,
    P::Message: Message + Clone,
{
    responses: VecDeque<Timed<PendingResponse<P::Message>>>,
    transport: T,
    service: S,
    queue: MessageQueue<P>,

    finish: bool,

    sink: MetricSink,
    bytes_sent: Counter,
    bytes_received: Counter,
    messages_sent: Counter,
    messages_received: Counter,
    client_e2e: Histogram,
}

impl<T, S, P> Pipeline<T, S, P>
where
    T: Sink<BytesMut> + Stream<Item = Result<P::Message, ProtocolError>> + Unpin,
    S: DrivenService<EnqueuedRequests<P::Message>>,
    P: Processor,
    P::Message: Message + Clone + Unpin,
{
    /// Creates a new `Pipeline`.
    pub fn new(transport: T, service: S, processor: P, mut sink: MetricSink) -> Self {
        let bytes_sent = sink.counter("bytes_sent");
        let bytes_received = sink.counter("bytes_received");
        let messages_sent = sink.counter("messages_sent");
        let messages_received = sink.counter("messages_received");
        let client_e2e = sink.histogram("client_e2e");

        Pipeline {
            responses: VecDeque::new(),
            transport,
            service,
            queue: MessageQueue::new(processor),
            finish: false,
            sink,
            bytes_sent,
            bytes_received,
            messages_sent,
            messages_received,
            client_e2e,
        }
    }

    pub async fn drive(&mut self) -> Result<(), PipelineError<T, S, P>> {
        loop {
            // The pipeline follows three logical steps: poll the transport for any new requests,
            // process those requests concurrently, and 
            // We follow three logical stages: poll the transport for any requests, process those
            // requests, and return the response.
            //
            // We flip the script, however, and poll for new requests last, which gives us a chance
            // to drive requests to completion and send the response before we pull in new requests
            // to process.
            while let Some(mut f) = self.responses.pop_front() {
                match poll!(f) {
                    Poll::Ready((start, result)) => {
                        let response = result.map_err(PipelineError::internal)?;
                        self.queue.fulfill(response);
                        let end = self.sink.now();
                        self.client_e2e.record_timing(start, end);
                    },
                    Poll::Pending => {
                        self.responses.push_front(f);
                        break;
                    },
                }
            }

            // Now that we've polled and fulfilled any completed batches, try and drain the message
            // queue to send any completed responses back to the client.
            while let Some((buf, msg_count)) = self.queue.get_sendable_buf() {
                let buf_len = buf.len();

                // TODO: do we need to check for n == 0 here for error conditions? :bigthink:
                self.transport.send(buf).await.map_err(PipelineError::send)?;
                self.messages_sent.record(msg_count);
                self.bytes_sent.record(buf_len as u64);
            }

            // Don't try and grab anything else from the transport if we're finished, we just need
            // to flush the rest of our responses and that's it.  If we have no more responses,
            // though, then we're actually done and we can return entirely.
            if self.finish {
                if self.responses.is_empty() {
                    return Ok(());
                }
            }

            // Make sure the underlying service is ready to be called.
            self.service.ready().await.map_err(PipelineError::service)?;

            if let Some(result) = self.transport.next().await {
                let request = result.map_err(PipelineError::receive)?;
                self.messages_received.increment();
                self.bytes_received.record(request.size() as u64);

                // Enqueue the message. This may generate multiple subrequests. We check to see if
                // we actually got anything back, because in the case of commands like PING, we do
                // a sort of request hairpinning maneuver which shoves the response directly into
                // the message queue, but obviously it has no request to send to a backend so we
                // don't get back a request object to actually send off.
                let subrequests = self.queue.enqueue(request).map_err(PipelineError::internal)?;
                if !subrequests.is_empty() {
                    // Split out the response channel from the enqueued requests we got back so we
                    // can store them/drive them.
                    let start = self.sink.now();
                    let responses = subrequests
                        .as_mut_slice()
                        .iter_mut()
                        .map(|x| x.get_response_rx())
                        .filter(|x| x.is_some())
                        .map(|x| x.unwrap().timed(start))
                        .collect::<Vec<_>>();

                    self.service.call(subrequests);
                    self.responses.extend(responses);
                }
            } else {
                // Our transport has signalled no more messages are going to come in, so mark
                // ourselves as finished so we can begin the closing process.
                assert!(!self.finish);
                self.finish = true;
            }
        }
    }
}
