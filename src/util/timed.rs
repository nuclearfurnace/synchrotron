use std::future::Future;
use std::task::{Context, Poll};
use std::pin::Pin;
use futures::ready;
use futures::prelude::*;

pub struct Timed<F: Future + Unpin> {
    start: u64,
    inner: F,
}

impl<F: Future + Unpin> Timed<F> {
    pub fn new(inner: F, start: u64) -> Self { Timed { inner, start } }
}

impl<F: Future + Unpin> Future for Timed<F> {
    type Output = (u64, F::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(self.inner.poll_unpin(cx));
        Poll::Ready((self.start, result))
    }
}
