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
use futures::future::{Either, FutureResult};
use std::io::Error;
use tokio::net::{ConnectFuture, TcpStream};

mod task;

pub use self::task::{TaskBackend, TaskBackendConnection, TaskBackendParticipant};

/// An existing or pending TcpStream.
pub type TcpStreamFuture = Either<FutureResult<TcpStream, Error>, ConnectFuture>;

/// Transformers a request into a response by generating a future which will consume a given
/// TcpStream to an underlying server, do its work, and hand back both the stream and the results.
pub trait RequestTransformer {
    type Request;
    type Response;
    type Executor;

    fn transform(&self, Self::Request, TcpStreamFuture) -> Self::Executor;
}
