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
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::tcp::TcpStream;

/// Wraps any future that does protocol operations and hands back a TCP stream.
pub struct ProcessFuture {
    inner: Box<dyn Future<Output = Result<TcpStream, ProtocolError>>>,
}

impl ProcessFuture {
    pub fn new<F>(inner: F) -> ProcessFuture
    where
        F: Future<Output = Result<TcpStream, ProtocolError>>,
    {
        ProcessFuture { inner: Box::new(inner) }
    }
}

impl Future for ProcessFuture {
    type Output = Result<TcpStream, ProtocolError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.inner.poll(cx)
    }
}
