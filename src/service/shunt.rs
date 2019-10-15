use crate::common::{Message, GenericError};
use std::future::Future;
use std::task::{Context, Poll};
use std::pin::Pin;
use futures::future::FutureExt;
use tower::{Service, layer::Layer};

#[derive(Clone)]
pub struct ShuntLayer;

impl ShuntLayer {
    pub fn new() -> Self {
        ShuntLayer {
        }
    }
}

impl<S> Layer<S> for ShuntLayer {
    type Service = Shunt<S>;

    fn layer(&self, service: S) -> Self::Service {
        Shunt {
            service,
        }
    }
}

/// Shunts responses if a request indicates it doesn't need or want one.
///
/// This allows us to handle basic "noreply" capabilities in protocols by checking to see if the
/// request inherently needs/expects a response, so we can short-circuit the request/response flow
/// and return an immediately available default message.
///
/// Default messages are a less-than-ideal indicator here for "no op" responses: a variant of the
/// response such that upper layers will know not to act on it or do anything with it.
pub struct Shunt<S> {
    service: S,
}

impl<S, Request> Service<Request> for Shunt<S>
where
    S: Service<Request>,
    S::Future: Unpin,
    S::Response: Message + Default,
    S::Error: Into<GenericError>,
    Request: Message,
{
    type Response = S::Response;
    type Error = GenericError;
    type Future = ShuntResponse<S::Future, S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
            .map_err(Into::into)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        if !req.needs_reply() {
            debug!("shunting response");

            // We fork off from the service call, returning an empty body for upper layers
            // to deal with while the request makes its way to the backend.
            let _ = self.service.call(req);
            let empty: S::Response = Default::default();
            ShuntResponse::shunted(empty)
        } else {
            debug!("passing through normal response");
            ShuntResponse::normal(self.service.call(req))
        }
    }
}

enum State<F, T> {
    Pending(F),
    Shunted(Option<T>),
}

pub struct ShuntResponse<F, T> {
    state: State<F, T>,
}

impl<F: Unpin, T> Unpin for ShuntResponse<F, T> {}

impl<F, T> ShuntResponse<F, T> {
    pub fn normal(inner: F) -> ShuntResponse<F, T> {
        ShuntResponse {
            state: State::Pending(inner),
        }
    }

    pub fn shunted(msg: T) -> ShuntResponse<F, T> {
        ShuntResponse {
            state: State::Shunted(Some(msg)),
        }
    }
}

impl<F, T, E> Future for ShuntResponse<F, T>
where
    F: Future<Output = Result<T, E>> + Unpin,
    E: Into<GenericError>
{
    type Output = Result<T, GenericError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state {
            // We're just waiting for a regular response.
            State::Pending(ref mut f) => f.poll_unpin(cx).map_err(Into::into),

            // We have a shunted message, so we can just hairpin the response.
            State::Shunted(ref mut v) => Poll::Ready(Ok(v.take().unwrap())),
        }
    }
}
