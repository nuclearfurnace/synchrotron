use futures::future::Either;
use futures::prelude::*;
use std::io::Error;
use tokio::net::{ConnectFuture, TcpStream};

mod mutex;
mod task;

pub use self::mutex::{MutexBackend, MutexBackendConnection, MutexBackendParticipant};
pub use self::task::{TaskBackend, TaskBackendConnection, TaskBackendParticipant};

/// Basic machinery for pushing an existing TcpStream as a future.
pub struct ExistingTcpStreamFuture {
    stream: Option<TcpStream>,
}

impl ExistingTcpStreamFuture {
    pub fn from_stream(stream: TcpStream) -> ExistingTcpStreamFuture {
        ExistingTcpStreamFuture {
            stream: Some(stream),
        }
    }
}

impl Future for ExistingTcpStreamFuture {
    type Item = TcpStream;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(self.stream.take().unwrap()))
    }
}

/// An existing or pending TcpStream.
pub type TcpStreamFuture = Either<ExistingTcpStreamFuture, ConnectFuture>;

/// Transformers a request into a response by generating a future which will consume a given
/// TcpStream to an underlying server, do its work, and hand back both the stream and the results.
pub trait RequestTransformer {
    type Request;
    type Response;
    type Executor;

    fn transform(&self, Self::Request, TcpStreamFuture) -> Self::Executor;
}
