use crate::service::{Service, DrivenService};
use tokio::sync::{mpsc, oneshot};
use tokio_executor::{TypedExecutor, SpawnError};
use std::sync::{Arc, Mutex};
use std::fmt;
use async_trait::async_trait;
use std::future::Future;
use std::task::{Poll, Context};
use std::pin::Pin;
use futures::future::poll_fn;

pub type Tx<T> = mpsc::Sender<Result<T, ServiceError>>;
pub type Rx<T> = mpsc::Receiver<Result<T, ServiceError>>;
pub type SharedState<S, Request> = Arc<Mutex<State<S, Request>>>;

/// Message sent from the facade to the worker.
struct Message<Request, Fut>
{
    req: Request,
    tx: Tx<Fut>,
}

/// State of the worker task.
enum State<S, Request>
where
    S: DrivenService<Request>,
{
    Initializing,
    Running,
    Failed(S::Error),
}

pub struct ResponseFuture<F> {
    state: ResponseState<F>,
}

enum ResponseState<F> {
    Failed(Option<Error>),
    Rx(Rx<F>),
    Poll(F),
}

impl<F> ResponseFuture<F> {
    pub(crate) fn new(rx: Rx<F>) -> Self {
        ResponseFuture {
            state: ResponseState::Rx(rx),
        }
    }

    pub(crate) fn failed(err: GenericError) -> Self {
        ResponseFuture {
            state: ResponseState::Failed(Some(err)),
        }
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, E>>,
    E: Into<GenericError>,
{
    type Output = Result<T, GenericError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let fut;

            match self.state {
                ResponseState::Failed(ref mut e) => {
                    return Poll::Ready(Err(e.take().expect("polled after error")));
                }
                ResponseState::Rx(ref mut rx) => match ready!(rx.poll()) {
                    Ok(f) => fut = f,
                    Err(_) => return Poll::Ready(Err(Closed::new().into())),
                },
                ResponseState::Poll(ref mut fut) => {
                    let result = ready!(fut.poll(cx));
                    Poll::Ready(result.map_err(Into::into))
                }
            }

            self.state = ResponseState::Poll(fut);
        }
    }
}

/// Trait to allow specializing a service that is Send or !Send.
pub trait WorkerExecutor<S, Request>: TypedExecutor<Worker<S, Request>>
where
    S: DrivenService<Request>,
{
}

impl<S, Request, E: TypedExecutor<Worker<S, Request>>> WorkerExecutor<S, Request> for E
where
    S: DrivenService<Request>,
{
}

/// User-facing type to access the underlying service.
pub struct Facade<S, Request>
where
    S: DrivenService<Request>,
{
    tx: mpsc::Sender<Message<Request, S::Future>>,
    worker: SharedState<S, Request>,
}

impl<S, Request> Facade<S, Request>
where
    S: DrivenService<Request>,
{
    pub fn new<E>(service: S, executor: E, buffer: usize) -> Result<Self, SpawnError>
    where
        S: DrivenService<Request>,
        E: WorkerExecutor<S, Request>,
    {
        let (tx, rx) = mpsc::channel(buffer);

        let worker = Worker::new(service, rx, executor)?;
        let facade = Facade {
            tx,
            worker,
        };

        Ok(facade)
    }

    fn get_worker_error(&self) -> GenericError {
    }
}

#[async_trait]
impl<S, Request> Service<Request> for Facade<S, Request>
where
    S: DrivenService<Request>,
{
    type Response = S::Response;
    type Error = GenericError;
    type Future = ResponseFuture<S::Future>;

    async fn ready(&mut self) -> Result<(), Self::Error> {
        // Readiness is based on the underlying worker: either it has room for a request (or not),
        // or the worker has failed permanently and all requests from this point on will fail.
        let result = poll_fn(|cx| self.tx.poll_ready(cx)).await;
        result.map_err(|_| self.get_worker_error())
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let (tx, rx) = oneshot::channel();

        // Try to send the request into the worker.
        match self.tx.try_send(Message { req, tx }) {
            Err(e) => {
                if e.is_closed() {
                    ResponseFuture::failed(self.get_worker_error())
                } else {
                    panic!("facade buffer full; poll_ready should have reserved slot");
                }
            }
            Ok(_) => ResponseFuture::new(rx),
        }
    }
}

/// Background task driving the underlying service.
pub struct Worker<S, Request>
where
    S: DrivenService<Request>,
{
    service: S,
    rx: mpsc::Receiver<Message<Request, S::Future>>,
    state: SharedState<S, Request>,
}

impl<S, Request> Worker<S, Request> {
    pub fn new<E>(service: S, rx: Rx<S>, executor: E) -> Result<SharedState<S, Request>, SpawnError>
    where
        S: DrivenService<Request>,
        E: WorkerExecutor<S, Request>,
    {
        let state = Arc::new(Mutex::new(State::Initializing));
        let worker = Worker {
            service,
            rx,
            state: state.clone(),
        };

        executor.spawn(worker)
            .map(move |_| state)
    }
}

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

/// An error produced by a `Service` wrapped by a `Buffer`
#[derive(Debug)]
pub struct ServiceError {
    inner: Arc<Error>,
}

/// An error when the buffer's worker closes unexpectedly.
#[derive(Debug)]
pub struct Closed {
    _p: (),
}

/// Errors produced by `Buffer`.
pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;

// ===== impl ServiceError =====

impl ServiceError {
    pub(crate) fn new(inner: Error) -> ServiceError {
        let inner = Arc::new(inner);
        ServiceError { inner }
    }

    /// Private to avoid exposing `Clone` trait as part of the public API
    pub(crate) fn clone(&self) -> ServiceError {
        ServiceError {
            inner: self.inner.clone(),
        }
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "buffered service failed: {}", self.inner)
    }
}

impl std::error::Error for ServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&**self.inner)
    }
}

// ===== impl Closed =====

impl Closed {
    pub(crate) fn new() -> Self {
        Closed { _p: () }
    }
}

impl fmt::Display for Closed {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("buffer's worker closed unexpectedly")
    }
}

impl std::error::Error for Closed {}
