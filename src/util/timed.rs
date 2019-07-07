use futures::prelude::*;

pub struct Timed<F: Future> {
    start: u64,
    inner: F,
}

impl<F: Future> Timed<F> {
    pub fn new(inner: F, start: u64) -> Self { Timed { inner, start } }
}

impl<F: Future> Future for Timed<F> {
    type Error = F::Error;
    type Item = (u64, F::Item);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = try_ready!(self.inner.poll());
        Ok(Async::Ready((self.start, result)))
    }
}
