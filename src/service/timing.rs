use std::sync::Arc;
use crate::service::Service;
use crate::common::GenericError;
use std::future::Future;
use std::task::{Context, Poll};
use std::pin::Pin;

use futures::ready;
use metrics_runtime::{Sink, data::Histogram};
use quanta::Clock;

pub struct Timing<S> {
    hist: Arc<Histogram>,
    service: S,
    clock: Arc<Clock>,
}

impl<S> Timing<S> {
    pub fn new<K>(service: S, sink: &mut Sink, key: K) -> Self
    where
        K: Into<String>,
    {
        let hist = sink.histogram(key.into());
        let clock = Clock::new();

        Timing {
            service,
            hist: Arc::new(hist),
            clock: Arc::new(clock),
        }
    }
}

impl<S, Request> Service<Request> for Timing<S>
where
    S: Service<Request>,
    S::Error: Into<GenericError>,
{
    type Response = S::Response;
    type Error = GenericError;
    type Future = TimingResponse<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
            .map_err(Into::into)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        TimingResponse::new(self.service.call(req), self.hist.clone(), self.clock.clone())
    }
}

pub struct TimingResponse<F> {
    fut: F,
    hist: Arc<Histogram>,
    clock: Arc<Clock>,
    start: u64,

}

impl<F> TimingResponse<F> {
    pub fn new(fut: F, hist: Arc<Histogram>, clock: Arc<Clock>) -> Self {
        let start = clock.now();
        TimingResponse {
            fut,
            hist,
            clock,
            start,
        }
    }
}

impl<F, T, E> Future for TimingResponse<F>
where
    F: Future<Output = Result<T, E>>,
    E: Into<GenericError>,
{
    type Output = Result<T, GenericError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = unsafe { self.as_mut().map_unchecked_mut(|me| &mut me.fut) };
        let result = ready!(fut.poll(cx));
        let end = self.clock.now();
        self.hist.record_timing(self.start, end);

        Poll::Ready(result.map_err(Into::into))
    }
}
