use std::io::Error;
use futures::future::{Either, FutureResult};
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
