use std::collections::HashMap;
use std::io::Error;
use bytes::BytesMut;
use futures::prelude::*;
use protocol::redis;
use protocol::redis::RedisMessage;
use backend::pool::{BackendPool, BackendParticipant, Distributor, Hasher};

pub struct RedisBackendQueue<F>
    where F: Future
{
    msg_indexes: Vec<u64>,
    inner: F
}

impl<F> RedisBackendQueue<F>
    where F: Future
{
    fn new(backend: BackendParticipant, messages: Vec<(u64, RedisMessage)>) -> RedisBackendQueue<F>
        where F: Future
    {
        // Break down the messages;
        let msg_len = messages.len();
        let msg_indexes = Vec::new();
        let msgs = Vec::new();
        for (index, msg) in messages {
            msg_indexes.push(index);
            msgs.push(msg);
        }

        let (conns_tx, conns_rx) = backend.split();
        let forward = conns_rx.take(1)
            .and_then(|server| server)
            .and_then(move |server| redis::write_messages(server, msgs))
            .and_then(|(server, _n)| server)
            .and_then(move |server| {
                redis::read_messages(server, msg_len)
                    .and_then(|(server, resps, _n)| {
                        conns_tx.send(server)
                            .map(move |_| resps)
                    })
            });

        RedisBackendQueue {
            msg_indexes: msg_indexes,
            inner: forward,
        }
    }

    pub fn to_queues<D, H>(pool: &BackendPool<D, H>, messages: Vec<RedisMessage>) -> Vec<RedisBackendQueue<F>>
        where F: Future, D: Distributor, H: Hasher
    {
        // Group all of our messages by their target backend index.
        let assigned_msgs = HashMap::new();

        let i = 0;
        while messages.len() > 0 {
            let msg = messages.remove(0);
            let msg_key = get_message_key(&msg);
            let backend_idx = pool.get_backend_index(&msg_key[..]);

            let batched_msgs = assigned_msgs.entry(backend_idx)
                .or_insert(Vec::new());
            batched_msgs.push((i, msg));

            i = i + 1;
        }

        // Now that we have things mapped, create our queues for each backend.
        let queues = Vec::new();

        for (backend_idx, msgs) in assigned_msgs {
            let backend = pool.get_backend_by_index(backend_idx);
            let queue = RedisBackendQueue::new(backend, msgs);
            queues.push(queue);
        }

        queues
    }
}

impl<F> Future for RedisBackendQueue<F>
    where F: Future
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

fn get_message_key(msg: &RedisMessage) -> BytesMut {
    match msg {
        RedisMessage::Bulk(_, ref args) => {
            if args.len() < 2 {
                panic!("can only get message key for arg count >= 2");
            }

            match args.get(1) {
                Some(RedisMessage::Data(buf, offset)) => {
                    let buf2 = buf.clone();
                    let _ = buf2.split_to(*offset);
                    let key_len = buf2.len() - 2;
                    let _ = buf2.split_off(key_len);
                    buf2
                },
                _ => panic!("command message does not have expected structure")
            }
        },
        _ => panic!("command message should be multi-bulk!")
    }
}
