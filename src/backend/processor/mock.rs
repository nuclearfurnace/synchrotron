use crate::{
    common::{ConnectionFuture, EnqueuedRequests, MessageState},
    protocol::mock::{MockTransport, MockMessage},
    util::MockStream,
};
use crate::backend::processor::{Processor, ProcessorError};
use std::{
    fmt::Display,
    net::SocketAddr,
};
use futures::future::ready;

#[derive(Clone)]
pub struct MockProcessor;

impl MockProcessor {
    pub fn new() -> Self {
        Self {
        }
    }
}

impl Processor<MockStream> for MockProcessor {
    type Message = MockMessage;
    type Transport = MockTransport<MockStream>;

    fn fragment_message(&self, msg: Self::Message) -> Result<Vec<(MessageState, Self::Message)>, ProcessorError> {
        Ok(vec![(MessageState::Standalone, msg)])
    }

    fn defragment_message(&self, mut fragments: Vec<(MessageState, Self::Message)>) -> Result<Self::Message, ProcessorError> {
        if fragments.len() != 1 {
            panic!("should only have one fragment when mock defragmenting");
        }

        let (_state, fragment) = fragments.remove(0);
        Ok(fragment)
    }

    fn get_error_message<E: Display>(&self, e: E) -> Self::Message {
        MockMessage::from_error(e)
    }

    fn get_transport(&self, rw: MockStream) -> Self::Transport {
        MockTransport::new(rw)
    }

    fn preconnect(&self, _: SocketAddr, _: bool) -> ConnectionFuture<MockStream> {
        ConnectionFuture::new(ready(Ok(MockStream::new())))
    }

    fn process(&self, _: EnqueuedRequests<Self::Message>, rw: MockStream) -> ConnectionFuture<MockStream> {
        ConnectionFuture::new(ready(Ok(rw)))
    }
}
