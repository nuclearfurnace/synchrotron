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
use super::Sizable;
use futures::prelude::*;
use futures::stream::Fuse;
use std::task::{Context, Poll};
use std::pin::Pin;
use std::mem;

/// An adapter for batching up items in a stream opportunistically.
///
/// On each call to `poll`, the adapter will poll the underlying stream in a loop until the
/// underlying stream reports that it is not ready.  Any items returned during this loop will be
/// stored and forwarded on either when the batch capacity is met or when the underlying stream
/// signals that it has no available items.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Batch<S>
where
    S: Stream,
    S::Item: Sizable,
{
    items: Vec<S::Item>,
    size: usize,
    inner: Fuse<S>,
}

impl<S> Batch<S>
where
    S: Stream,
    S::Item: Sizable,
{
    pub fn new(s: S, capacity: usize) -> Batch<S> {
        assert!(capacity > 0);

        Batch {
            items: Vec::with_capacity(capacity),
            size: 0,
            inner: s.fuse(),
        }
    }

    fn consume(&mut self) -> (Vec<S::Item>, usize) {
        let cap = self.items.capacity();
        let items = mem::replace(&mut self.items, Vec::with_capacity(cap));
        let size = mem::replace(&mut self.size, 0);

        (items, size)
    }
}

impl<S> Stream for Batch<S>
where
    S: Stream,
    S::Item: Sizable,
{
    type Item = (Vec<S::Item>, usize);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let cap = self.items.capacity();
        loop {
            match self.inner.poll_next(cx) {
                // If the underlying stream isn't ready any more, and we have items queued up,
                // simply return them to the caller and zero out our internal buffer.  If we have
                // no items, then tell the caller we aren't ready.
                Poll::Pending => {
                    return if self.items.is_empty() {
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(self.consume()))
                    };
                },

                // If the underlying stream is ready and has items, buffer them until we hit our
                // capacity.
                //
                // Generally, the capacity should be high enough that we consume every
                // possible item available to us at the time of a given `poll`, maximixing the
                // batching effect.
                Poll::Ready(Some(item)) => {
                    let size = item.size();
                    self.items.push(item);
                    self.size += size;
                    if self.items.len() >= cap {
                        return Poll::Ready(Some(self.consume()));
                    }
                },

                // Since the underlying stream ran out of values, return what we have buffered, if
                // we have anything at all.
                Poll::Ready(None) => {
                    return if !self.items.is_empty() {
                        Poll::Ready(Some(self.consume()))
                    } else {
                        Poll::Ready(None)
                    };
                },
            }
        }
    }
}

impl<S, T> Sink<T> for Batch<S>
where
    S: Sink<T> + Stream,
    <S as Stream>::Item: Sizable,
{
    type Error = S::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.inner.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_close(cx)
    }
}
