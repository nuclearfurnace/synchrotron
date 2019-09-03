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
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;
use crate::service::Service;
use pin_project::pin_project;

#[derive(Derivative)]
#[derivative(Clone)]
pub struct ShadowRouter<P, S>
where
    P: Processor + Clone + Unpin,
    P::Message: Message + Clone,
    S: Service<EnqueuedRequests<P::Message>> + Clone,
    S::Future: Future + Send,
{
    processor: P,
    default_inner: S,
    shadow_inner: S,
    noops: mpsc::UnboundedSender<S::Future>,
}

#[pin_project]
struct ShadowWorker<S, Request>
where
    S: Service<Request>,
{
    #[pin]
    rx: mpsc::UnboundedReceiver<S::Future>,
    finish: bool,
    #[pin]
    inner: FuturesUnordered<S::Future>,
    _service: PhantomData<S>,
}

impl<S, Request> ShadowWorker<S, Request>
where
    S: Service<Request>,
{
    pub fn new(rx: mpsc::UnboundedReceiver<S::Future>) -> ShadowWorker<S, Request> {
        ShadowWorker {
            rx,
            finish: false,
            inner: FuturesUnordered::new(),
            _service: PhantomData,
        }
    }
}

impl<S, Request> Future for ShadowWorker<S, Request>
where
    S: Service<Request>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if !*this.finish {
            while let Poll::Ready(result) = this.rx.as_mut().poll_next(cx) {
                match result {
                    Some(fut) => this.inner.push(fut),
                    None => {
                        *this.finish = true;
                        break;
                    },
                }
            }
        }

        // Just drive our inner futures; we don't care about their return value.
        while let Poll::Ready(result) = this.inner.as_mut().poll_next(cx) {
            match result {
                // These are successful results, so we just drop the value and keep on moving on.
                Some(_) => {},
                // If we have no more futures to drive, and we've been instructed to close, it's
                // time to go.
                None => {
                    if *this.finish {
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
    P: Processor + Clone + Unpin,
    P::Message: Message + Clone + Send + 'static,
    S: Service<EnqueuedRequests<P::Message>> + Clone + Send + 'static,
    S::Future: Future + Send,
{
    pub fn new(processor: P, default_inner: S, shadow_inner: S) -> ShadowRouter<P, S> {
        let (tx, rx) = mpsc::unbounded_channel();

        // Spin off a task that drives all of the shadow responses.
        let shadow: ShadowWorker<S, EnqueuedRequests<P::Message>> = ShadowWorker::new(rx);
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
    P: Processor + Clone + Unpin,
    P::Message: Message + Clone,
    S: Service<EnqueuedRequests<P::Message>> + Clone + Send,
    S::Future: Future + Send,
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
