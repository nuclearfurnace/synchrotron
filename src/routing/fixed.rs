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
use crate::{
    backend::processor::Processor,
    common::{EnqueuedRequest, EnqueuedRequests, Message},
};
use crate::service::Service;
use async_trait::async_trait;

#[derive(Clone)]
pub struct FixedRouter<P, S>
where
    P: Processor + Clone + Send + Sync + Unpin,
    P::Message: Message + Send + Sync,
    S: Service<EnqueuedRequests<P::Message>> + Clone + Send,
{
    processor: P,
    inner: S,
}

impl<P, S> FixedRouter<P, S>
where
    P: Processor + Clone + Send + Sync + Unpin,
    P::Message: Message + Send + Sync,
    S: Service<EnqueuedRequests<P::Message>> + Clone + Send,
{
    pub fn new(processor: P, inner: S) -> FixedRouter<P, S> { FixedRouter { processor, inner } }
}

#[async_trait]
impl<P, S> Service<Vec<P::Message>> for FixedRouter<P, S>
where
    P: Processor + Clone +  Send + Sync + Unpin,
    P::Message: Message + Send + Sync,
    S: Service<EnqueuedRequests<P::Message>> + Clone + Send,
{
    type Error = S::Error;
    type Future = S::Future;
    type Response = S::Response;

    async fn ready(&mut self) -> Result<(), Self::Error> {
        self.inner.ready().await
    }

    fn call(&mut self, req: Vec<P::Message>) -> Self::Future {
        let transformed = req.into_iter()
            .map(EnqueuedRequest::new)
            .collect();
        self.inner.call(transformed)
    }
}
