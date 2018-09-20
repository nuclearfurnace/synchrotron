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
use super::{
    distributor::{BackendDescriptor, Distributor},
    hasher::KeyHasher,
};
use backend::{backend::Backend, processor::RequestProcessor};
use futures::{future::Shared, prelude::*};
use futures_turnstyle::Waiter;
use protocol::errors::ProtocolError;
use std::net::SocketAddr;
use tokio::net::TcpStream;

pub struct BackendPool<T>
where
    T: RequestProcessor + Clone + Send,
    T::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    distributor: Box<Distributor + Send + Sync>,
    hasher: Box<KeyHasher + Send + Sync>,
    backends: Vec<Backend<T>>,
}

impl<T> BackendPool<T>
where
    T: RequestProcessor + Clone + Send + 'static,
    T::Message: Send,
    T::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    pub fn new(
        addresses: &[SocketAddr], transformer: T, mut dist: Box<Distributor + Send + Sync>,
        hasher: Box<KeyHasher + Send + Sync>, conn_limit: usize, close: Shared<Waiter>,
    ) -> BackendPool<T> {
        // Assemble the list of backends and backend descriptors.
        let mut backends = vec![];
        let mut descriptors = vec![];
        for address in addresses {
            let (backend, runner) = Backend::new(*address, transformer.clone(), conn_limit, close.clone());
            backends.push(backend);

            // eventually, we'll populate this with weight, etc, so that
            // we can actually do weighted things.
            let descriptor = BackendDescriptor::new();
            descriptors.push(descriptor);

            // Spawn our backend runner.
            tokio::spawn(runner);
        }

        // Seed the distributor.
        dist.seed(descriptors);

        BackendPool {
            backends,
            distributor: dist,
            hasher,
        }
    }

    pub fn get_backend_index(&self, key: &[u8]) -> usize {
        let key_id = self.hasher.hash(key);
        self.distributor.choose(key_id)
    }

    pub fn get_backend_by_index(&self, idx: usize) -> &Backend<T> {
        match self.backends.get(idx) {
            Some(backend) => backend,
            None => unreachable!("incorrect backend idx"),
        }
    }
}
