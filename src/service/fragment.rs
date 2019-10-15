use std::sync::Arc;
use crate::backend::processor::Processor;
use crate::common::{Response, MessageResponse, GenericError};
use std::future::Future;
use std::task::{Context, Poll};
use std::pin::Pin;
use futures::{ready, future::FutureExt};
use tower::{Service, layer::Layer};
use crate::common::MessageState;

const ERR_INTERNAL_RECV: &str = "failed to receive internal message (SSF12)";

#[derive(Clone)]
pub struct FragmentLayer<P> {
    processor: Arc<P>,
}

impl<P> FragmentLayer<P> {
    pub fn new(processor: P) -> Self {
        FragmentLayer {
            processor: Arc::new(processor),
        }
    }
}

impl<S, P> Layer<S> for FragmentLayer<P> {
    type Service = Fragment<S, P>;

    fn layer(&self, service: S) -> Self::Service {
        Fragment {
            service,
            processor: self.processor.clone(),
        }
    }
}

pub struct Fragment<S, P> {
    processor: Arc<P>,
    service: S,
}

impl<S, P> Service<P::Message> for Fragment<S, P>
where
    S: Service<Vec<P::Message>>,
    S::Future: Unpin,
    S::Response: IntoIterator<Item = Response<P::Message>>,
    S::Error: Into<GenericError>,
    P: Processor + Send + Sync,
{
    type Response = P::Message;
    type Error = GenericError;
    type Future = FragmentResponse<S::Future, P>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
            .map_err(Into::into)
    }

    fn call(&mut self, req: P::Message) -> Self::Future {
        // Try to fragment the message.  If we get an error, just short circuit
        // the response future and move on.
        let mut freq = match self.processor.fragment_message(req) {
            Ok(freq) => freq,
            Err(e) => return FragmentResponse::failed(e),
        };

        // Optimization: if we have a single message after fragmentation, and it's an inline
        // message, we can turn around and hand it back.
        if freq.len() == 1 && freq[0].0 == MessageState::Inline {
            let pair = freq.remove(0);
            debug!("passing as inline");
            return FragmentResponse::inline(pair.1);
        }

        // Line up our response storage and the requests we actually need to send.  Since some
        // requests are "inline", it means we essentially parsed a request and instead of handing
        // over the request, we gave the known response to the request, so we're just doing a
        // hairpin maneuver here where we try to give it back as soon as possible.
        // Since inline messages are inherently not fragmentable, we treat the fragmented requests
        // we got back uniformly and construct the slots for them.
        let mut slots = Vec::with_capacity(freq.len());
        let mut sreq = Vec::new();
        for (state, msg) in freq {
            slots.push(state);
            sreq.push(msg);
        }

        // Now, pass along the subrequests that actually need to hit a backend node to the
        // underlying service, and pass our laid-out response slots and the "actual" subrequest
        // response futures along so we can defragment when all subrequests are ready.
        debug!("request is fragmented");
        let response = self.service.call(sreq);
        FragmentResponse::new(slots, response, self.processor.clone())
    }
}

enum State<F, T> {
    Pending(F),
    Inline(Option<T>),
    Failed(Option<GenericError>),
}

pub struct FragmentResponse<F, P>
where
    P: Processor,
{
    state: State<F, P::Message>,
    msg_states: Vec<MessageState>,
    processor: Option<Arc<P>>,
}

impl<F: Unpin, P: Processor> Unpin for FragmentResponse<F, P> {}

impl<F, P> FragmentResponse<F, P>
where
    P: Processor,
{
    pub fn new(msg_states: Vec<MessageState>, inner: F, processor: Arc<P>) -> FragmentResponse<F, P> {
        FragmentResponse {
            state: State::Pending(inner),
            msg_states,
            processor: Some(processor),
        }
    }

    pub fn inline(msg: P::Message) -> FragmentResponse<F, P> {
        FragmentResponse {
            state: State::Inline(Some(msg)),
            msg_states: Vec::new(),
            processor: None,
        }
    }

    pub fn failed<E>(e: E) -> FragmentResponse<F, P>
    where
        E: Into<GenericError>,
    {
        FragmentResponse {
            state: State::Failed(Some(e.into())),
            msg_states: Vec::new(),
            processor: None,
        }
    }
}

impl<F, P, T, E> Future for FragmentResponse<F, P>
where
    P: Processor,
    F: Future<Output = Result<T, E>> + Unpin,
    T: IntoIterator<Item = Response<P::Message>>,
    E: Into<GenericError>
{
    type Output = Result<P::Message, GenericError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state {
            // We're waiting for subresponses.  Once we have them, we'll attempt to
            // defragment them into a single message.
            State::Pending(ref mut f) => {
                let responses = match ready!(f.poll_unpin(cx)) {
                    Ok(responses) => responses,
                    Err(e) => return Poll::Ready(Err(e.into())),
                };

                let mut ordered = Vec::with_capacity(self.msg_states.len());
                for msg in responses {
                    let amsg = match msg {
                        MessageResponse::Failed => {
                            self.processor.as_ref().unwrap().get_error_message(ERR_INTERNAL_RECV)
                        },
                        MessageResponse::Complete(m) => m,
                    };

                    ordered.push((self.msg_states.remove(0), amsg));
                }

                Poll::Ready(self.processor.take().unwrap()
                    .defragment_message(ordered)
                    .map_err(|e| e.into()))
            },

            // We have an inline message, so we can just hairpin the response.
            State::Inline(ref mut v) => Poll::Ready(Ok(v.take().unwrap())),

            // We failed right out of the gate (fragmenting) so just give them
            // the error back immediately.
            State::Failed(ref mut e) => Poll::Ready(Err(e.take().unwrap())),
        }
    }
}
