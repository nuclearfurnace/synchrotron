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
use futures::{
    stream::{Fuse, Stream},
    Async, Poll,
};
use std::mem;

/// An adapter for batching up items in a stream opportunistically.
///
/// On each call to `poll`, the adapter will poll the underlying stream in a loop until the
/// underlying stream reports that it is not ready.  Any items returned during this loop will be
/// stored and forwarded on either when the batch capacity is met or when the underlying stream
/// signals that it has no available items.
///
/// Batches items to match the `OrderedMessages` type.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct OrderedBatch<S>
where
    S: Stream,
{
    items: Vec<(u64, i64, S::Item)>,
    err: Option<S::Error>,
    stream: Fuse<S>,
}

pub fn new<S>(s: S, capacity: usize) -> OrderedBatch<S>
where
    S: Stream,
{
    assert!(capacity > 0);

    OrderedBatch {
        items: Vec::with_capacity(capacity),
        err: None,
        stream: s.fuse(),
    }
}

impl<S: Stream> OrderedBatch<S> {
    fn take(&mut self) -> Vec<(u64, i64, S::Item)> {
        let cap = self.items.capacity();
        mem::replace(&mut self.items, Vec::with_capacity(cap))
    }
}

impl<S> Stream for OrderedBatch<S>
where
    S: Stream,
{
    type Error = <S as Stream>::Error;
    type Item = Vec<(u64, i64, <S as Stream>::Item)>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Some(err) = self.err.take() {
            return Err(err);
        }

        let cap = self.items.capacity();
        loop {
            match self.stream.poll() {
                // If the underlying stream isn't ready any more, and we have items queued up,
                // simply return them to the caller and zero out our internal buffer.  If we have
                // no items, then tell the caller we aren't ready.
                Ok(Async::NotReady) => {
                    return match self.items.len() {
                        0 => Ok(Async::NotReady),
                        _ => Ok(Some(self.take()).into()),
                    }
                },

                // If the underlying stream is ready and has items, buffer them until we hit our
                // capacity.
                //
                // Generally, the capacity should be high enough that we consume every
                // possible item available to us at the time of a given `poll`, maximixing the
                // batching effect.
                Ok(Async::Ready(Some(item))) => {
                    let index = self.items.len() as u64;
                    self.items.push((index, -1, item));
                    if self.items.len() >= cap {
                        return Ok(Some(self.take()).into());
                    }
                },

                // Since the underlying stream ran out of values, return what we have buffered, if
                // we have anything at all.
                Ok(Async::Ready(None)) => {
                    return if self.items.len() > 0 {
                        Ok(Some(self.take()).into())
                    } else {
                        Ok(Async::Ready(None))
                    }
                },

                // If we've got buffered items be sure to return them first, we'll defer our error
                // for later.
                Err(e) => {
                    if self.items.len() == 0 {
                        return Err(e);
                    } else {
                        self.err = Some(e);
                        return Ok(Some(self.take()).into());
                    }
                },
            }
        }
    }
}
