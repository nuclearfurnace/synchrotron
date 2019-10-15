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
use futures::{Sink, Stream};
use std::fmt;
use tower::Service;

/// Error type for `Pipeline`.
pub enum PipelineError<T, S, Request>
where
    T: Sink<S::Response> + Stream<Item = Result<Request, ProtocolError>>,
    S: Service<Request>,
{
    /// An error occurred while reading from the transport.
    TransportReceive(ProtocolError),

    /// An error occurred while writing to the transport.
    TransportSend(<T as Sink<S::Response>>::Error),

    /// The underlying service failed to process a request.
    Service(S::Error),
}

impl<T, S, Request> PipelineError<T, S, Request>
where
    T: Sink<S::Response> + Stream<Item = Result<Request, ProtocolError>>,
    S: Service<Request>,
{
    pub fn receive(e: ProtocolError) -> PipelineError<T, S, Request> {
        PipelineError::TransportReceive(e)
    }

    pub fn send(e: <T as Sink<S::Response>>::Error) -> PipelineError<T, S, Request> {
        PipelineError::TransportSend(e)
    }

    pub fn service(e: S::Error) -> PipelineError<T, S, Request> {
        PipelineError::Service(e)
    }
}

impl<T, S, Request> fmt::Display for PipelineError<T, S, Request>
where
    T: Sink<S::Response> + Stream<Item = Result<Request, ProtocolError>>,
    <T as Sink<S::Response>>::Error: fmt::Display,
    S: Service<Request>,
    S::Error: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PipelineError::TransportReceive(ref se) => fmt::Display::fmt(se, f),
            PipelineError::TransportSend(ref se) => fmt::Display::fmt(se, f),
            PipelineError::Service(ref se) => fmt::Display::fmt(se, f),
        }
    }
}

impl<T, S, Request> fmt::Debug for PipelineError<T, S, Request>
where
    T: Sink<S::Response> + Stream<Item = Result<Request, ProtocolError>>,
    <T as Sink<S::Response>>::Error: fmt::Debug,
    S: Service<Request>,
    S::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PipelineError::TransportReceive(ref se) => write!(f, "TransportReceive({:?})", se),
            PipelineError::TransportSend(ref se) => write!(f, "TransportSend({:?})", se),
            PipelineError::Service(ref se) => write!(f, "Service({:?})", se),
        }
    }
}
