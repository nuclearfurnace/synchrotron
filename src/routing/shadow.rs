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
    backend::processor::Processor,
    common::{EnqueuedRequest, EnqueuedRequests, Message},
};
use futures::stream::{Stream, futures_unordered::FuturesUnordered};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;
use tower::Service;
use pin_project::pin_project;

#[derive(Derivative)]
#[derivative(Clone)]
pub struct ShadowRouter<P, S>
where
    P: Processor,
    S: Service<EnqueuedRequests<P::Message>>,
    S::Future: Send,
{
    processor: P,
    default_inner: S,
    shadow_inner: S,
    noops: mpsc::UnboundedSender<S::Future>,
}

#[pin_project]
struct ShadowWorker<F: Future> {
    #[pin]
    rx: mpsc::UnboundedReceiver<F>,
    finish: bool,
    #[pin]
    inner: FuturesUnordered<F>,
}

impl<F: Future> ShadowWorker<F> {
    pub fn new(rx: mpsc::UnboundedReceiver<F>) -> ShadowWorker<F> {
        ShadowWorker {
            rx,
            finish: false,
            inner: FuturesUnordered::new(),
        }
    }
}

impl<F: Future> Future for ShadowWorker<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if !*this.finish {
            while let Poll::Ready(result) = this.rx.as_mut().poll_next(cx) {
                match result {
                    Some(fut) => {
                        this.inner.push(fut);
                        tracing::debug!("pushed pending response into queue");
                    },
                    None => {
                        *this.finish = true;
                        tracing::debug!("marked as finished");
                        break;
                    },
                }
            }
        }

        // Just drive our inner futures; we don't care about their return value.
        while let Poll::Ready(result) = this.inner.as_mut().poll_next(cx) {
            match result {
                // These are successful results, so we just drop the value and keep on moving on.
                Some(_) => {
                    tracing::debug!("processed a response");
                },
                // If we have no more futures to drive, and we've been instructed to close, it's
                // time to go.
                None => {
                    if *this.finish {
                        tracing::debug!("finished");
                        return Poll::Ready(());
                    } else {
                        break;
                    }
                },
            }
        }

        Poll::Pending
    }
}

impl<P, S> ShadowRouter<P, S>
where
    P: Processor,
    P::Message: Send,
    S: Service<EnqueuedRequests<P::Message>> + Send,
    S::Future: Send + 'static,
{
    pub fn new(processor: P, default_inner: S, shadow_inner: S) -> ShadowRouter<P, S> {
        let (tx, rx) = mpsc::unbounded_channel();

        // Spin off a task that drives all of the shadow responses.
        let shadow = ShadowWorker::new(rx);
        tokio::spawn(shadow);

        ShadowRouter {
            processor,
            default_inner,
            shadow_inner,
            noops: tx,
        }
    }
}

impl<P, S> Service<Vec<P::Message>> for ShadowRouter<P, S>
where
    P: Processor,
    P::Message: Message + Clone + Send,
    S: Service<EnqueuedRequests<P::Message>>,
    S::Future: Send,
{
    type Error = S::Error;
    type Future = S::Future;
    type Response = S::Response;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.default_inner.poll_ready(cx)
    }

    fn call(&mut self, req: Vec<P::Message>) -> Self::Future {
        let shadow_reqs = req
            .clone()
            .into_iter()
            .map(EnqueuedRequest::without_response)
            .collect();

        let default_reqs = req.into_iter()
            .map(EnqueuedRequest::new)
            .collect();

        let noop = self.shadow_inner.call(shadow_reqs);
        let _ = self.noops.try_send(noop);

        self.default_inner.call(default_reqs)
    }
}
