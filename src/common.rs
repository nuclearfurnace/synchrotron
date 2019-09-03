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
use crate::{
    protocol::errors::ProtocolError,
    util::Sizable
};
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

pub trait Message: Sizable {
    fn key(&self) -> &[u8];

    /// Whether or not a message is "inline", or already processed.  This typically disqualifies a
    /// message from being altered, fragmented, etc, any further after parsing.
    fn is_inline(&self) -> bool;

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
    /// This represents a discrete fragment of a parent message.  The buffer represents arbitrary
    /// data that is used to identify the parent message.  Given that fragments may not have the
    /// information any longer, we keep track of it in the message state.
    ///
    /// The integers provide the index of the given fragment and the overall count of fragments
    /// within the parent message.
    Fragmented(BytesMut, usize, usize),

    /// A streaming fragmented message.
    ///
    /// This represents a discrete fragment of a parent message.  The key difference is that the
    /// parent message is "streamable."  This is usually the case for get operations, where, as
    /// long as the fragments are in order, they can be streamed back to the client as they're
    /// available.  This is in contrast to some other fragmented messages, where the response must
    /// be generated by the sum of all the parts.
    ///
    /// The optional buffer represents a header that can be sent before the actual fragment.  This
    /// allows sending any response data that is needed to coalesce the fragments into a meaningful
    /// response to the client.
    ///
    /// The boolean marks whether this streaming fragment represents the end of the response to the
    /// client for a given input message.  For example, if the client sent in a multi-get that
    /// asked for 10 keys, the 10th streaming fragment would be the "end" of the response.
    StreamingFragmented(Option<BytesMut>, bool),
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

    pub fn consume(&mut self) -> T { self.request.take().unwrap() }

    pub fn fulfill(&mut self, response: T) {
        if self.done {
            return;
        }

        let _ = self
            .tx
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
            let _ = self.tx.take().unwrap().send(MessageResponse::Failed);
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

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        while let Some(response) = ready!(this.pending.as_mut().poll_next(cx)) {
            match response {
                Ok(response) => this.responses.as_mut().unwrap().push(response),
                Err(e) => return Poll::Ready(Err(e.into())),
            }
        }

        let responses = this.responses.take().expect("cannot poll future after completion");
        return Poll::Ready(Ok(responses))
    }
}

pub struct ConnectionFuture {
    inner: Pin<Box<dyn Future<Output = Result<TcpStream, ProtocolError>> + Send>>,
}

impl ConnectionFuture {
    pub fn new(inner: impl Future<Output = Result<TcpStream, ProtocolError>> + Send + 'static) -> Self {
        ConnectionFuture {
            inner: Box::pin(inner),
        }
    }
}

impl Future for ConnectionFuture {
    type Output = Result<TcpStream, ProtocolError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}
