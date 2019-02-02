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

pub struct FixedRouter<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send,
{
    processor: P,
    pool: BackendPool<P>,
}

impl<P> FixedRouter<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send,
{
    pub fn new(processor: P, pool: BackendPool<P>) -> FixedRouter<P> { FixedRouter { processor, pool } }
}

impl<P> DirectService<AssignedRequests<P::Message>> for FixedRouter<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send,
{
    type Error = PoolError;
    type Future = ResponseFuture<P, Self::Error>;
    type Response = AssignedResponses<P::Message>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> { self.pool.poll_ready() }

    fn poll_service(&mut self) -> Poll<(), Self::Error> { self.pool.poll_service() }

    fn poll_close(&mut self) -> Poll<(), Self::Error> { self.pool.poll_close() }

    fn call(&mut self, req: AssignedRequests<P::Message>) -> Self::Future {
        self.pool.call(req)
    }
}
