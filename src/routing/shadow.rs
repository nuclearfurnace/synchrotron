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
use backend::{
    pool::BackendPool,
    PoolError, ResponseFuture,
    processor::Processor
};
use futures::prelude::*;
use common::{AssignedRequests, AssignedResponses, Message};
use tower_direct_service::DirectService;

pub struct ShadowRouter<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
{
    processor: P,
    default_pool: BackendPool<P>,
    shadow_pool: BackendPool<P>,
}

impl<P> ShadowRouter<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
{
    pub fn new(processor: P, default: BackendPool<P>, shadow: BackendPool<P>) -> ShadowRouter<P> {
        ShadowRouter {
            processor,
            default_pool: default,
            shadow_pool: shadow,
        }
    }
}

impl<P> DirectService<AssignedRequests<P::Message>> for ShadowRouter<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
{
    type Error = PoolError;
    type Future = ResponseFuture<P, Self::Error>;
    type Response = AssignedResponses<P::Message>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> { self.default_pool.poll_ready() }

    fn poll_service(&mut self) -> Poll<(), Self::Error> { self.default_pool.poll_service() }

    fn poll_close(&mut self) -> Poll<(), Self::Error> { self.default_pool.poll_close() }

    fn call(&mut self, req: AssignedRequests<P::Message>) -> Self::Future {
        // we need to duplicate these requests and send them to the shadow pool buttttt we also
        // need to enqueue messages using `with_noop` so that nothing funnels backwards.
        self.default_pool.call(req)
    }
}
