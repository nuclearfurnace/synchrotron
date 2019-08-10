use std::future::Future;
use std::task::{Context, Poll};
use std::pin::Pin;
use futures::prelude::*;

pub struct Untyped<F: Future + Unpin> {
    inner: F,
}

impl<F: Future + Unpin> Untyped<F> {
    pub fn new(inner: F) -> Self { Self { inner } }
}

impl<F: Future + Unpin> Future for Untyped<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx).map(|_| ())
    }
}
