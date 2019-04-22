use futures::prelude::*;

pub struct Untyped<F: Future> {
    inner: F,
}

impl<F: Future> Untyped<F> {
    pub fn new(inner: F) -> Self { Self { inner } }
}

impl<F: Future> Future for Untyped<F> {
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> { self.inner.poll().map(|x| x.map(|_| ())).map_err(|_| ()) }
}
