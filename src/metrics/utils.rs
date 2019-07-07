use futures::prelude::*;

pub struct Timed<F: Future>(pub u64, pub F);

impl<F: Future> Future for Timed<F> {
    type Item = (u64, F::Item);
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = try_ready!(self.1.poll());
        Ok(Async::Ready((self.0, result)))
    }
}
