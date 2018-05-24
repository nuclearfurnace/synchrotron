use std::mem;
use futures::stream::{Stream, Fuse};
use futures::sink::Sink;
use futures::{Async, Poll, StartSend};

#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Batch<S>
    where S: Stream
{
    items: Vec<S::Item>,
    err: Option<S::Error>,
    stream: Fuse<S>
}

pub fn new<S>(s: S, capacity: usize) -> Batch<S>
    where S: Stream
{
    assert!(capacity > 0);

    Batch {
        items: Vec::with_capacity(capacity),
        err: None,
        stream: s.fuse(),
    }
}

impl<S> Sink for Batch<S>
    where S: Sink + Stream
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: S::SinkItem) -> StartSend<S::SinkItem, S::SinkError> {
        self.stream.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), S::SinkError> {
        self.stream.poll_complete()
    }

    fn close(&mut self) -> Poll<(), S::SinkError> {
        self.stream.close()
    }
}

impl<S> Batch<S>
    where S: Stream
{
    fn take(&mut self) -> Vec<S::Item> {
        let cap = self.items.capacity();
        mem::replace(&mut self.items, Vec::with_capacity(cap))
    }

    /// Acquires a reference to the underlying stream that this combinator is
    /// pulling from.
    pub fn get_ref(&self) -> &S {
        self.stream.get_ref()
    }

    /// Acquires a mutable reference to the underlying stream that this
    /// combinator is pulling from.
    ///
    /// Note that care must be taken to avoid tampering with the state of the
    /// stream which may otherwise confuse this combinator.
    pub fn get_mut(&mut self) -> &mut S {
        self.stream.get_mut()
    }

    /// Consumes this combinator, returning the underlying stream.
    ///
    /// Note that this may discard intermediate state of this combinator, so
    /// care should be taken to avoid losing resources when this is called.
    pub fn into_inner(self) -> S {
        self.stream.into_inner()
    }
}

impl<S> Stream for Batch<S>
    where S: Stream
{
    type Item = Vec<<S as Stream>::Item>;
    type Error = <S as Stream>::Error;  

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Some(err) = self.err.take() {
            return Err(err)
        }

        let cap = self.items.capacity();
        loop {
            match self.stream.poll() {
                // If the underlying stream isn't ready any more, and we have items queued up,
                // simply return them to the caller and zero out our internal buffer.  If we have
                // no items, then tell the caller we aren't ready.
                Ok(Async::NotReady) => return match self.items.len() {
                    0 => Ok(Async::NotReady),
                    _ => {
                        let items = mem::replace(&mut self.items, Vec::new());
                        Ok(Some(items).into())
                    },
                },

                // If the underlying stream is ready and has items, buffer them until we hit our
                // capacity.
                //
                // Generally, the capacity should be high enough that we consume every
                // possible item available to us at the time of a given `poll`, maximixing the
                // batching effect.
                Ok(Async::Ready(Some(item))) => {
                    self.items.push(item);
                    if self.items.len() >= cap {
                        return Ok(Some(self.take()).into())
                    }
                },

                // Since the underlying stream ran out of values, return what we have buffered, if
                // we have anything at all.
                Ok(Async::Ready(None)) => {
                    return if self.items.len() > 0 {
                        let items = mem::replace(&mut self.items, Vec::new());
                        Ok(Some(items).into())
                    } else {
                        Ok(Async::Ready(None))
                    }
                },

                // If we've got buffered items be sure to return them first, we'll defer our error
                // for later.
                Err(e) => {
                    if self.items.len() == 0 {
                        return Err(e)
                    } else {
                        self.err = Some(e);
                        return Ok(Some(self.take()).into())
                    }
                },
            }
        }
    }
}
