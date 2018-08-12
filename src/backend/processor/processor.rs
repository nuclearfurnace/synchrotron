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
use backend::processor::ProcessorError;
use common::OrderedMessages;
use futures::future::{Either, Future, FutureResult};
use std::{collections::HashMap, error::Error, io};
use tokio::net::{ConnectFuture, TcpStream};

/// An existing or pending TcpStream.
pub type TcpStreamFuture = Either<FutureResult<TcpStream, io::Error>, ConnectFuture>;

/// Processors a request into a response by generating a future which will consume a given
/// TcpStream to an underlying server, do its work, and hand back both the stream and the results.
pub trait RequestProcessor {
    type Message;
    type ClientReader;
    type ClientWriter;
    type Future;

    fn is_fragmented(&self, &Self::Message) -> bool;
    fn get_fragments(&self, Self::Message) -> Result<Vec<Self::Message>, ProcessorError>;
    fn defragment_messages(
        &self, OrderedMessages<Self::Message>, HashMap<u64, Self::Message>,
    ) -> Result<OrderedMessages<Self::Message>, ProcessorError>;

    fn get_error_message(&self, Box<Error>) -> Self::Message;

    fn get_client_streams(&self, TcpStream) -> (Self::ClientReader, Self::ClientWriter);

    fn process(&self, OrderedMessages<Self::Message>, TcpStreamFuture) -> Self::Future;
}

pub trait MessageSink {
    type Message;

    fn send(
        self, OrderedMessages<Self::Message>,
    ) -> Box<Future<Item = (usize, usize, Self), Error = io::Error> + Send + 'static>;
}
