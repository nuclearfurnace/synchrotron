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
    protocol::errors::ProtocolError,
    service::{PipelineError, Service},
};
use std::{
    future::Future,
    task::{Context, Poll},
    pin::Pin,
    collections::VecDeque,
};
use futures::{
    future::FutureExt,
    stream::{Stream, FuturesOrdered},
    sink::Sink,
};
use pin_project::pin_project;

/// Pipeline-capable service base.
///
/// Simultaneously drives a `Transport` and an underlying `Service`, pulling requests off of the
/// transport, submitting them to the service, and sending back the responses.
#[pin_project]
pub struct Pipeline<T, S, Request>
where
    T: Sink<S::Response> + Stream<Item = Result<Request, ProtocolError>> + Unpin,
    S: Service<Request>,
    S::Future: Future<Output = Result<S::Response, S::Error>> + Unpin,
{
    #[pin]
    pending: FuturesOrdered<S::Future>,
    responses: VecDeque<S::Response>,
    #[pin]
    transport: T,
    service: S,
    finish: bool,
}

impl<T, S, Request> Pipeline<T, S, Request>
where
    T: Sink<S::Response> + Stream<Item = Result<Request, ProtocolError>> + Unpin,
    S: Service<Request>,
    S::Future: Future<Output = Result<S::Response, S::Error>> + Unpin,
{
    /// Creates a new `Pipeline`.
    pub fn new(transport: T, service: S) -> Self {
        Pipeline {
            pending: FuturesOrdered::new(),
            responses: VecDeque::new(),
            transport,
            service,
            finish: false,
        }
    }
}

impl<T, S, Request> Future for Pipeline<T, S, Request>
where
    T: Sink<S::Response> + Stream<Item = Result<Request, ProtocolError>> + Unpin,
    S: Service<Request>,
    S::Future: Future<Output = Result<S::Response, S::Error>> + Unpin,
{
    type Output = Result<(), PipelineError<T, S, Request>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        debug!("inside poll for pipeline");
        let mut this = self.project();
        loop {
            // Drive all of our pending responses, collecting any available responses.
            while let Poll::Ready(Some(result)) = this.pending.as_mut().poll_next(cx) {
                match result {
                    Ok(response) => {
                        this.responses.push_back(response);
                        debug!("got response back from pending");
                    },
                    Err(e) => return Poll::Ready(Err(PipelineError::service(e))),
                }
            }

            // Try and push any responses we have into the transport.
            while let Some(response) = this.responses.pop_front() {
                let ready = this.transport.as_mut().poll_ready(cx);
                match ready {
                    Poll::Pending => {
                        debug!("transport not ready to send, push back resp");
                        this.responses.push_front(response);
                        break
                    },
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(PipelineError::send(e))),
                    Poll::Ready(Ok(())) => {
                        if let Err(e) = this.transport.as_mut().start_send(response) {
                            return Poll::Ready(Err(PipelineError::send(e)))
                        }

                        debug!("start send for resp to transport");
                    },
                }
            }

            // Try and flush our transport in case we pushed anything into it just now.
            if let Poll::Ready(Ok(())) = this.transport.as_mut().poll_flush(cx) {
                debug!("transport flushed");
                // We have no more finished/pending responses to worry about, and we've flushed
                // everything, so we're done!
                if *this.finish && this.responses.is_empty() && this.pending.is_empty() {
                    return Poll::Ready(Ok(()))
                }
            }

            // We've sent back all the responses we could during this poll, and nothing else was
            // ready yet, so yield back if we're supposed to be closing up shop.
            if *this.finish {
                return Poll::Pending
            }

            debug!("checking svc readiness");

            // Make sure the underlying service is ready to be called.
            if let Err(e) = ready!(this.service.ready().as_mut().poll(cx)) {
                return Poll::Ready(Err(PipelineError::service(e)))
            }

            debug!("svc is ready, polling transport");

            // Since we're ready, try and see if there's a request from the transport.
            if let Some(result) = ready!(this.transport.as_mut().poll_next(cx)) {
                match result {
                    Ok(request) => {
                        debug!("transport had message, calling svc");
                        let mut span = tracing::span!(tracing::Level::DEBUG, "pipeline request");
                        let _guard = span.enter();
                        let response = this.service.call(request);
                        this.pending.push(response);
                    },
                    Err(e) => return Poll::Ready(Err(PipelineError::receive(e))),
                }
            } else {
                // Our transport has signalled no more messages are going to come in, so mark
                // ourselves as finished so we can begin the closing process.
                assert!(!*this.finish);
                *this.finish = true;
            }
        }
    }
}
