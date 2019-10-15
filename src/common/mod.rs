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
use crate::protocol::errors::ProtocolError;
use bytes::BytesMut;
use tokio::{
    net::TcpStream,
    sync::oneshot::{channel, Receiver, Sender}
};
use std::future::Future;
use std::task::{Context, Poll};
use std::pin::Pin;
use futures::{ready, stream::{Stream, FuturesOrdered}};
use pin_project::pin_project;

pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub trait Sizable {
    fn size(&self) -> usize;
}

pub trait Message: Sizable {
    fn key(&self) -> &[u8];

    /// Whether or not a message is "inline", or already processed.  This typically disqualifies a
    /// message from being altered, fragmented, etc, any further after parsing.
    fn is_inline(&self) -> bool;

    /// Whether or not a message needs a reply.
    ///
    /// This allows clients to indicate that while a message should be sent to a backend, that it
    /// does not need a response.  This allows us to respect the wishes of clients who ask for
    /// requests
    fn needs_reply(&self) -> bool;

    /// Converts the message into a raw buffer suitable for sending over the network.
    fn into_buf(self) -> BytesMut;
}

/// Message response types for a queued message.
#[derive(Debug)]
pub enum MessageResponse<T> {
    /// The message ultimately "failed".  This happens if a queued message is dropped before having
    /// a response sent for it, which may happen if an error occurs during the backend read, etc.
    Failed,

    /// The message was processed and a response was received.
    Complete(T),
}

/// Message state of queued messages.
#[derive(Debug, PartialEq)]
pub enum MessageState {
    /// An unfragmented, standalone message.
    ///
    /// A filled variant of this state can be immediately sent off to the client.
    Standalone,

    /// An unfragmented, standalone message that is _also_ immediately available.
    ///
    /// While normal messages have to be processed before a response, these messages are available
    /// to send as soon as they're enqueued.
    Inline,

    /// A fragmented message.
    ///
    /// This represents a discrete fragment of a parent message.  Depending on the underlying
    /// protocol, the optional buffer can be used to pass information to the processor about the
    /// fragmentation.
    ///
    /// For example, in memcached, only multi-key gets are possible, so returning the responses
    /// from the backends, in order, is all that's required.  For Redis, multiple commands can be
    /// fragmented, so this buffer is used to convey the command type so we can interpret and
    /// defragment the responses correctly.
    Fragmented(Option<BytesMut>),
}

// Core types.
//
// These define the transformation between raw messages that come in over the transport and the
// interstitial types as they're batched, fragmented, etc.
pub type Response<T> = MessageResponse<T>;
pub type Responses<T> = Vec<Response<T>>;
pub type PendingResponse<T> = Receiver<Response<T>>;
pub type PendingResponses<T> = Vec<PendingResponse<T>>;
pub type EnqueuedRequests<T> = Vec<EnqueuedRequest<T>>;

pub struct EnqueuedRequest<T: Clone + Message> {
    request: Option<T>,
    has_response: bool,
    done: bool,
    tx: Option<Sender<Response<T>>>,
}

impl<T: Clone + Message> EnqueuedRequest<T> {
    pub fn new(request: T) -> EnqueuedRequest<T> {
        EnqueuedRequest {
            request: Some(request),
            tx: None,
            has_response: true,
            done: false,
        }
    }

    pub fn without_response(request: T) -> EnqueuedRequest<T> {
        EnqueuedRequest {
            request: Some(request),
            tx: None,
            has_response: false,
            done: true,
        }
    }

    pub fn key(&self) -> &[u8] {
        // Pass-through for `Message::key` because we really don't want to expose the
        // entire Message trait over ourselves, as one of the methods allows taking
        // the request by consuming self.
        self.request.as_ref().expect("tried to get key for empty request").key()
    }

    pub fn is_done(&self) -> bool { self.done }

    pub fn consume(&mut self) -> T { self.request.take().unwrap() }

    pub fn fulfill(&mut self, response: T) {
        if self.done {
            return;
        }

        let _ = self.tx
            .take()
            .expect("tried to send response to uninitialized receiver")
            .send(MessageResponse::Complete(response));
        self.done = true;
    }

    pub fn get_response_rx(&mut self) -> Option<PendingResponse<T>> {
        if self.has_response {
            let (tx, rx) = channel();
            self.tx = Some(tx);
            self.done = false;
            self.has_response = false;

            return Some(rx);
        }

        None
    }
}

impl<T: Clone + Message> Drop for EnqueuedRequest<T> {
    fn drop(&mut self) {
        // The drop guard is used to make sure we always send back a response to the upper
        // layers even if a backend has an error that kills an entire batch of requests.
        if !self.done {
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(MessageResponse::Failed);
            }
        }
    }
}

#[pin_project]
pub struct ResponseFuture<T> {
    responses: Option<Vec<Response<T>>>,
    #[pin]
    pending: FuturesOrdered<PendingResponse<T>>,
}

impl<T> ResponseFuture<T> {
    pub fn new(pending: FuturesOrdered<PendingResponse<T>>) -> ResponseFuture<T> {
        ResponseFuture {
            responses: Some(Vec::with_capacity(pending.len())),
            pending,
        }
    }
}

impl<T> Future for ResponseFuture<T> {
    type Output = Result<Responses<T>, GenericError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        while let Some(response) = ready!(this.pending.as_mut().poll_next(cx)) {
            match response {
                Ok(response) => this.responses.as_mut().unwrap().push(response),
                Err(e) => return Poll::Ready(Err(e.into())),
            }
        }

        let responses = this.responses.take().expect("cannot poll future after completion");
        Poll::Ready(Ok(responses))
    }
}

pub struct ConnectionFuture<T = TcpStream> {
    inner: Pin<Box<dyn Future<Output = Result<T, ProtocolError>> + Send>>,
}

impl<T> ConnectionFuture<T> {
    pub fn new(inner: impl Future<Output = Result<T, ProtocolError>> + Send + 'static) -> Self {
        ConnectionFuture {
            inner: Box::pin(inner),
        }
    }
}

impl<T> Future for ConnectionFuture<T> {
    type Output = Result<T, ProtocolError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

#[macro_export]
macro_rules! ready_break {
    ($e:expr $(,)?) => (match $e {
        std::task::Poll::Ready(t) => t,
        std::task::Poll::Pending => break,
    })
}
