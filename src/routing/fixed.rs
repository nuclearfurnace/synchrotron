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
use backend::{pool::BackendPool, processor::RequestProcessor, BackendError};
use common::{Keyed, OrderedMessages};
use futures::{
    future::{ok, result},
    prelude::*,
    stream::futures_unordered::FuturesUnordered,
};
use routing::{Router, RouterError};
use std::{collections::HashMap, error, io, sync::Arc};
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct FixedRouter<T: RequestProcessor> {
    processor: T,
    pool: Arc<BackendPool<T>>,
}

impl<T> FixedRouter<T>
where
    T: RequestProcessor + Clone + Send + 'static,
    T::Message: Keyed + Clone + Send + 'static,
    T::Future: Future<Item = (TcpStream, OrderedMessages<T::Message>), Error = io::Error> + Send + 'static,
{
    pub fn new(processor: T, pool: Arc<BackendPool<T>>) -> FixedRouter<T> { FixedRouter { processor, pool } }

    fn run_batch(
        &self, mut req: OrderedMessages<T::Message>,
    ) -> Result<impl Future<Item = OrderedMessages<T::Message>, Error = RouterError>, RouterError> {
        let mut batches: HashMap<usize, OrderedMessages<T::Message>> = HashMap::default();
        let mut fragment_ops = HashMap::default();

        // Split all the messages out into backend/associated-keys groupings.
        //
        // This gets slightly opaque because we need to be able to support breaking apart multi-key
        // messages, so we have to defer to the processor to give us opaque multi-key futures.
        while req.len() > 0 {
            let (i, _j, msg) = req.remove(0);

            if self.processor.is_fragmented(&msg) {
                let mut j = 0;

                // Fragmented message.  We get the individual pieces, which lets us batch them up
                // with unfragmented messages, but assign them a fragment ID so we can reorder the
                // pieces back into a coherent message afterwards.
                let mut fragments = self.processor.get_fragments(msg)?;

                // Strip off the first fragment, which will be the command itself.  We need to
                // track this so that the processor has a map of how to assemble any fragmented
                // responses.  Some commands that are fragmented may return a single value for
                // every fragment, or they may return a single value for the whole command if it
                // succeeded, so while we want to run fragments blindly, so to speak, against the
                // backends, we need to know how to properly respond to the client depending on
                // what the command was.
                let op = fragments.remove(0);
                fragment_ops.insert(i, op);

                // Batch the rest of the fragments like normal.
                for fragment in fragments {
                    let backend_idx = {
                        let msg_key = fragment.key();
                        self.pool.get_backend_index(msg_key)
                    };

                    let batch = batches.entry(backend_idx).or_insert(Vec::new());
                    batch.push((i, j, fragment));

                    j += 1;
                }
            } else {
                // Unfragmented message.  That means we can send it directly to a backend, no
                // alterations or splitting required.  We set the fragment order to -1 to indicate
                // that this response can be passed back to the client directly.
                let backend_idx = {
                    let msg_key = msg.key();
                    self.pool.get_backend_index(msg_key)
                };

                let batch = batches.entry(backend_idx).or_insert(Vec::new());
                batch.push((i, -1, msg));
            }
        }

        // At this point, we've batched up all messages according to which backend they should go
        // to, including fragmented messages.  Now we need to actually submit these batches to
        // their respective backend, and we need to coalesce the results *and* assemble any
        // fragments back into a coherent message.
        //
        // Our map is backend index -> [(batch order, fragment order, message)], which means we're
        // incurring an API cost by having fragments represented directly but oh well!
        //
        // Let's run each backend batch, and then we'll coalesce the results.
        let processor = self.processor.clone();
        let mut backend_responses = FuturesUnordered::new();
        for (backend_idx, batch) in batches {
            let mut order = Vec::new();
            for (i, j, _) in &batch {
                order.push((*i, *j));
            }

            let backend = self.pool.get_backend_by_index(backend_idx);
            let processor2 = processor.clone();
            let response = backend
                .submit(batch)
                .map_err(|_| BackendError::InternalError(format!("internal error (FRBSRE)")))
                .and_then(|r| result(r))
                .or_else(move |e| ok(generate_ordered_errors(&processor2, order, e)));

            backend_responses.push(response);
        }

        // We have to generate a separate map of messages which are fragmented, which we can then
        // pass with our fragment map to the processor for reassembly.

        // Now we have a list of futures to run which will actually make the backend requests and
        // get our data for us.  We have to defragment them after we get all of our responses back
        // but we have the processor handle it for us.
        let processor3 = processor.clone();
        let f = backend_responses
            .collect()
            .map_err(|e| RouterError::from_backend(e))
            .map(|r| r.into_iter().flatten().collect::<OrderedMessages<T::Message>>())
            .and_then(move |r| {
                result(processor3.defragment_messages(r, fragment_ops)).map_err(|e| RouterError::from_processor(e))
            });

        Ok(f)
    }
}

impl<T> Router<T> for FixedRouter<T>
where
    T: RequestProcessor + Clone + Sync + Send + 'static,
    T::Message: Keyed + Clone + Send + 'static,
    T::Future: Future<Item = (TcpStream, OrderedMessages<T::Message>), Error = io::Error> + Send + 'static,
{
    type Future = Box<Future<Item = OrderedMessages<T::Message>, Error = RouterError> + Send + 'static>;

    fn route(&self, req: OrderedMessages<T::Message>) -> Result<Self::Future, RouterError> {
        debug!("[fixed] running batch of {} messages", req.len());
        let f = self.run_batch(req)?;
        Ok(Box::new(f))
    }
}

fn generate_ordered_errors<T, E>(processor: &T, order: Vec<(u64, i64)>, e: E) -> OrderedMessages<T::Message>
where
    T: RequestProcessor,
    T::Message: Clone,
    E: Into<Box<error::Error>>,
{
    let mut new_msgs = Vec::new();
    let err_msg = processor.get_error_message(e.into());
    for (i, j) in order {
        new_msgs.push((i, j, err_msg.clone()));
    }
    new_msgs
}
