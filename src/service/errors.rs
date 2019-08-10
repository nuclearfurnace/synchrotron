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
use std::error::Error;
use bytes::BytesMut;
use crate::common::EnqueuedRequests;
use crate::protocol::errors::ProtocolError;
use std::fmt;
use crate::service::DrivenService;
use crate::backend::processor::Processor;
use futures::{Sink, Stream};

/// Error type for `Pipeline`.
pub enum PipelineError<T, S, P>
where
    T: Sink<BytesMut> + Stream<Item = Result<P::Message, ProtocolError>>,
    S: DrivenService<EnqueuedRequests<P::Message>>,
    P: Processor,
{
    /// An error occurred while reading from the transport.
    TransportReceive(ProtocolError),

    /// An error occurred while writing to the transport.
    TransportSend(<T as Sink<BytesMut>>::Error),

    /// The underlying service failed to process a request.
    Service(S::Error),

    /// An internal Synchrotron error occurred while servicing a request.
    Internal(Box<dyn Error + Send + Sync + 'static>),
}

impl<T, S, P> PipelineError<T, S, P>
where
    T: Sink<BytesMut> + Stream<Item = Result<P::Message, ProtocolError>>,
    S: DrivenService<EnqueuedRequests<P::Message>>,
    P: Processor,
{
    pub fn receive(e: ProtocolError) -> PipelineError<T, S, P> {
        PipelineError::TransportReceive(e)
    }

    pub fn send(e: <T as Sink<BytesMut>>::Error) -> PipelineError<T, S, P> {
        PipelineError::TransportSend(e)
    }

    pub fn service(e: S::Error) -> PipelineError<T, S, P> {
        PipelineError::Service(e)
    }

    pub fn internal<E>(e: E) -> PipelineError<T, S, P>
    where
        E: Into<Box<dyn Error + Send + Sync + 'static>>,
    {
        PipelineError::Internal(e.into())
    }
}

impl<T, S, P> fmt::Display for PipelineError<T, S, P>
where
    T: Sink<BytesMut> + Stream<Item = Result<P::Message, ProtocolError>>,
    <T as Sink<BytesMut>>::Error: fmt::Display,
    S: DrivenService<EnqueuedRequests<P::Message>>,
    S::Error: fmt::Display,
    P: Processor,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PipelineError::TransportReceiver(ref se) => fmt::Display::fmt(se, f),
            PipelineError::TransportSend(ref se) => fmt::Display::fmt(se, f),
            PipelineError::Service(ref se) => fmt::Display::fmt(se, f),
            PipelineError::Internal(ref se) => fmt::Display::fmt(se, f),
        }
    }
}

impl<T, S, P> fmt::Debug for PipelineError<T, S, P>
where
    T: Sink<BytesMut> + Stream<Item = Result<P::Message, ProtocolError>>,
    <T as Sink<BytesMut>>::Error: fmt::Debug,
    S: DrivenService<EnqueuedRequests<P::Message>>,
    S::Error: fmt::Debug,
    P: Processor,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PipelineError::TransportReceive(ref se) => write!(f, "TransportReceive({:?})", se),
            PipelineError::TransportSend(ref se) => write!(f, "TransportSend({:?})", se),
            PipelineError::Service(ref se) => write!(f, "Service({:?})", se),
            PipelineError::Internal(ref se) => write!(f, "Internal({:?})", se),
        }
    }
}
