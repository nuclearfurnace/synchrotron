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
use crate::common::GenericError;
use crate::service::{DrivenService, Service};
use async_trait::async_trait;
use futures::future::poll_fn;
use futures::stream::Stream;
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio_sync::{mpsc, oneshot};
use tokio_executor::{SpawnError, TypedExecutor};
use pin_project::{pin_project, project};

type InnerTx<R, F> = mpsc::Sender<Message<R, F>>;
type InnerRx<R, F> = mpsc::Receiver<Message<R, F>>;
type OuterTx<F> = oneshot::Sender<Result<F, FacadeError>>;
type OuterRx<F> = oneshot::Receiver<Result<F, FacadeError>>;
type SharedState = Arc<Mutex<State>>;

/// Message sent from the facade to the worker.
pub struct Message<Request, F> {
    pub(crate) req: Request,
    pub(crate) tx: OuterTx<F>,
    pub(crate) span: tracing::Span,
}

/// State of the worker task.
pub enum State {
    Running,
    Failed(FacadeError),
}

pub enum ResponseFuture<F> {
    Failed(Option<GenericError>),
    Rx(OuterRx<F>),
    Poll(F),
}

impl<F> ResponseFuture<F> {
    pub(crate) fn new(rx: OuterRx<F>) -> Self {
        ResponseFuture::Rx(rx)
    }

    pub(crate) fn failed(err: GenericError) -> Self {
        ResponseFuture::Failed(Some(err))
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, E>> + Unpin,
    E: Into<GenericError>,
{
    type Output = Result<T, GenericError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let fut;

            match *self {
                ResponseFuture::Failed(ref mut e) => {
                    return Poll::Ready(Err(e.take().expect("polled after error")));
                },
                ResponseFuture::Rx(ref mut rx) => {
                    let rx = Pin::new(rx);
                    match ready!(rx.poll(cx)) {
                        Ok(r) => match r {
                            Ok(f) => fut = f,
                            Err(e) => return Poll::Ready(Err(e.into())),
                        }
                        Err(_) => return Poll::Ready(Err(Closed::new().into())),
                    }
                },
                ResponseFuture::Poll(ref mut fut) => {
                    let fut = Pin::new(fut);
                    let result = ready!(fut.poll(cx));
                    return Poll::Ready(result.map_err(Into::into));
                },
            }

            self.set(ResponseFuture::Poll(fut));
        }
    }
}

/// Trait to allow specializing a service that is Send or !Send.
pub trait WorkerExecutor<S, Request>: TypedExecutor<Worker<S, Request>>
where
    S: DrivenService<Request>,
{
}

impl<S, Request, E: TypedExecutor<Worker<S, Request>>> WorkerExecutor<S, Request> for E where S: DrivenService<Request> {}

/// User-facing type to access the underlying service.
pub struct Facade<S, Request>
where
    S: DrivenService<Request>,
{
    tx: InnerTx<Request, S::Future>,
    worker: SharedState,
}

impl<S, Request> Facade<S, Request>
where
    S: DrivenService<Request>,
{
    pub fn new<E>(service: S, mut executor: E, buffer: usize) -> Result<Self, SpawnError>
    where
        S: DrivenService<Request>,
        E: WorkerExecutor<S, Request>,
    {
        let (tx, rx) = mpsc::channel(buffer);

        let worker = Worker::new(service, rx, &mut executor)?;
        let facade = Facade { tx, worker };

        Ok(facade)
    }

    fn get_worker_error(&self) -> GenericError {
        let state = self.worker.lock().unwrap();
        match *state {
            State::Running => Closed::new().into(),
            State::Failed(ref err) => err.clone().into(),
        }
    }
}

#[async_trait]
impl<S, Request> Service<Request> for Facade<S, Request>
where
    S: DrivenService<Request>,
    Request: Send + Sync,
    S::Error: Into<GenericError> + Send + Sync,
    S::Future: Send + Sync + Unpin,
{
    type Error = GenericError;
    type Future = ResponseFuture<S::Future>;
    type Response = S::Response;

    async fn ready(&mut self) -> Result<(), Self::Error> {
        // Readiness is based on the underlying worker: either it has room for a request (or not),
        // or the worker has failed permanently and all requests from this point on will fail.
        let result = poll_fn(|cx| self.tx.poll_ready(cx)).await;
        result.map_err(|_| self.get_worker_error())
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let (tx, rx) = oneshot::channel();
        let span = tracing::Span::current();
        tracing::debug!(parent: &span, "sending request to buffer worker");

        // Try to send the request into the worker.
        match self.tx.try_send(Message { req, tx, span }) {
            Err(e) => {
                if e.is_closed() {
                    ResponseFuture::failed(self.get_worker_error())
                } else {
                    panic!("facade buffer full; poll_ready should have reserved slot");
                }
            },
            Ok(_) => ResponseFuture::new(rx),
        }
    }
}

impl<S, Request> Clone for Facade<S, Request>
where
    S: DrivenService<Request>,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            worker: self.worker.clone(),
        }
    }
}

/// Background task driving the underlying service.
#[pin_project]
pub struct Worker<S, Request>
where
    S: DrivenService<Request>,
{
    service: S,
    #[pin]
    rx: InnerRx<Request, S::Future>,
    state: SharedState,
    pending_msg: Option<Message<Request, S::Future>>,
    last_error: Option<FacadeError>,
    finish: bool,
}

impl<S, Request> Worker<S, Request>
where
    S: DrivenService<Request>,
{
    pub(crate) fn new<E>(service: S, rx: InnerRx<Request, S::Future>, executor: &mut E) -> Result<SharedState, SpawnError>
    where
        S: DrivenService<Request>,
        E: WorkerExecutor<S, Request>,
    {
        let state = Arc::new(Mutex::new(State::Running));
        let worker = Worker {
            service,
            rx,
            state: state.clone(),
            pending_msg: None,
            last_error: None,
            finish: false,
        };

        executor.spawn(worker).map(move |_| state)
    }
}

#[project]
impl<S, Request> Worker<S, Request>
where
    S: DrivenService<Request>,
{
    fn poll_next_request(&mut self, cx: &mut Context<'_>) -> Poll<Option<Message<Request, S::Future>>> {
        // If we're done, just return back none.
        if *self.finish {
            return Poll::Ready(None);
        }

        // See if we have a pending request that we tucked away.  If we do, try again
        // to process it.
        if let Some(mut msg) = self.pending_msg.take() {
            // Check if the receiver is closed.  If it's not, then its closure will be "pending",
            // and that means we're good to move forward with processing.
            if msg.tx.poll_closed(cx).is_pending() {
                return Poll::Ready(Some(msg));
            }
        }

        // We didn't have a buffered a request, so go through our normal channel and keep pulling
        // them out until we find one whose receiver is not closed.
        while let Some(mut msg) = ready!(self.rx.as_mut().poll_next(cx)) {
            if msg.tx.poll_closed(cx).is_pending() {
                return Poll::Ready(Some(msg));
            }
        }

        Poll::Ready(None)
    }

    fn mark_failed(&mut self, e: GenericError) {
        // Set our internal state to failed.
        let mut inner = self.state.lock().unwrap();
        if let State::Failed(_) = *inner {
            // We were already marked as failed, so bail out.
            return;
        }

        let e = FacadeError::new(e);
        *inner = State::Failed(e.clone());
        drop(inner);

        // Now close the receive side of the channel, so callers start getting turned away.
        self.rx.close();

        // Now store the error internally so we can send it back to every request we still have
        // pending.
        *self.last_error = Some(e);
    }
}

impl<S, Request> Future for Worker<S, Request>
where
    S: DrivenService<Request>,
    S::Error: Into<GenericError>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut any_outstanding = false;

        let mut this = self.project();
        loop {
            let next = this.poll_next_request(cx);
            match next {
                Poll::Ready(Some(msg)) => {
                    let _guard = msg.span.enter();

                    if let Some(e) = this.last_error {
                        tracing::debug!("notifying caller about worker failure");
                        let _ = msg.tx.send(Err(e.clone()));
                        continue;
                    }

                    // Wait for the service to be ready
                    tracing::debug!(
                        message = "worker received request; waiting for service readiness"
                    );
                    let ready = this.service.ready().as_mut().poll(cx);
                    match ready {
                        Poll::Ready(Ok(())) => {
                            tracing::debug!(service.ready = true, message = "processing request");
                            let response = this.service.call(msg.req);

                            // Send the response future back to the sender.
                            //
                            // An error means the request had been canceled in-between
                            // our calls, the response future will just be dropped.
                            tracing::debug!("returning response future");
                            let _ = msg.tx.send(Ok(response));

                            // Mark that we now have an outstanding request, and continue to feed
                            // calls into the service until we run out of new requests to make
                            // calls for or until the underlying service can't handle any more.
                            any_outstanding = true;
                            continue;
                        },
                        Poll::Ready(Err(e)) => {
                            this.mark_failed(e.into());
                            let err = this.last_error.as_ref().map(|a| a.clone()).unwrap();
                            tracing::debug!({ %err }, "service ready failed");
                            let _ = msg.tx.send(Err(err));
                        },
                        Poll::Pending => {
                            tracing::debug!(service.ready = false, poll.outstanding = any_outstanding, message = "delay");
                            // Put out current message back in its slot.
                            drop(_guard);
                            *this.pending_msg = Some(msg);
                        },
                    }
                },
                Poll::Ready(None) => {
                    // No more more requests _ever_.
                    *this.finish = true;
                },
                Poll::Pending if !any_outstanding => {
                    // If we have no pending requests, and no new requests, then put ourselves to
                    // sleep.
                    return Poll::Pending;
                },
                Poll::Pending => {
                    // We have pending requests that require the service to be driven to process.
                },
            }

            let mut span = tracing::span!(tracing::Level::DEBUG, "worker svc");
            let _guard = span.enter();

            if *this.finish {
                let result = this.service.close().as_mut().poll(cx);
                match result {
                    Poll::Pending => {
                        tracing::debug!(status = "pending", message = "close svc");
                        return Poll::Pending;
                    },
                    Poll::Ready(Ok(())) => {
                        tracing::debug!(status = "closed", message = "close svc");
                        break;
                    },
                    Poll::Ready(Err(e)) => {
                        tracing::debug!(status = "failed", message = "close svc");
                        this.mark_failed(e.into());
                    },
                }
            } else {
                // Since we've buffered the requests we got, we should be processing them all right
                // here, so we can reset our outstanding flag.  If we have no new requests on the
                // next iteration, then we will still have gotten as far as we could driving the
                // underlying service, so we should get a notification when the service can make
                // more progress, or, we'll have freed up space and be able to queue more requests
                // on the next iteration, continuing until we run out of the service can't accept.
                any_outstanding = false;
                let result = this.service.drive().as_mut().poll(cx);
                match result {
                    Poll::Pending => {
                        tracing::debug!(status = "pending", message = "drive svc");
                        return Poll::Pending;
                    },
                    Poll::Ready(Ok(())) => {
                        tracing::debug!(status = "ok", message = "drive svc");
                        return Poll::Pending;
                    },
                    Poll::Ready(Err(e)) => {
                        tracing::debug!(status = "failed", message = "drive svc");
                        this.mark_failed(e.into());
                        continue;
                    },
                }
            }
        }

        // All senders are dropped... the task is no longer needed
        Poll::Ready(())
    }
}

/// An error produced by a `Service` wrapped by a `Facade`
#[derive(Debug)]
pub struct FacadeError {
    inner: Arc<GenericError>,
}

/// An error when the buffer's worker closes unexpectedly.
#[derive(Debug)]
pub struct Closed {
    _p: (),
}

// ===== impl FacadeError =====

impl FacadeError {
    pub(crate) fn new(inner: GenericError) -> FacadeError {
        let inner = Arc::new(inner);
        FacadeError { inner }
    }

    /// Private to avoid exposing `Clone` trait as part of the public API
    pub(crate) fn clone(&self) -> FacadeError {
        FacadeError {
            inner: self.inner.clone(),
        }
    }
}

impl fmt::Display for FacadeError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result { write!(fmt, "facade service failed: {}", self.inner) }
}

impl std::error::Error for FacadeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(&**self.inner) }
}

// ===== impl Closed =====

impl Closed {
    pub(crate) fn new() -> Self { Closed { _p: () } }
}

impl fmt::Display for Closed {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result { fmt.write_str("facade's worker closed unexpectedly") }
}

impl std::error::Error for Closed {}
