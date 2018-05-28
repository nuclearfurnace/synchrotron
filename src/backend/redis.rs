use backend::distributor::Distributor;
use backend::hasher::Hasher;
use backend::pool::BackendPool;
use bytes::BytesMut;
use futures::future::ok;
use futures::prelude::*;
use protocol::redis;
use protocol::redis::RedisMessage;
use std::collections::HashMap;
use std::io::Error;

type OrderedMessages = Vec<(u64, RedisMessage)>;

pub fn generate_batched_writes<D, H>(
    pool: &BackendPool<D, H>,
    mut messages: Vec<RedisMessage>,
) -> Vec<impl Future<Item = OrderedMessages, Error = Error>>
where
    D: Distributor,
    H: Hasher,
{
    // Group all of our messages by their target backend index.
    let mut assigned_msgs = HashMap::new();

    let mut i = 0;
    while messages.len() > 0 {
        let msg = messages.remove(0);
        let msg_key = get_message_key(&msg);
        let backend_idx = pool.get_backend_index(&msg_key[..]);

        let batched_msgs = assigned_msgs.entry(backend_idx).or_insert(Vec::new());
        batched_msgs.push((i, msg));

        debug!(
            "[redis batch] message #{} ({:?}) assigned to backend #{}",
            i,
            &msg_key[..],
            backend_idx
        );

        i += 1;
    }

    // Now that we have things mapped, create our queues for each backend.
    let mut queues = Vec::with_capacity(assigned_msgs.len());

    for (backend_idx, msgs) in assigned_msgs {
        let backend = pool.get_backend_by_index(backend_idx);

        let msg_len = msgs.len();
        let mut msg_indexes = Vec::with_capacity(msg_len);
        let mut split_msgs = Vec::with_capacity(msg_len);
        for v in msgs {
            let (index, msg) = v;
            msg_indexes.push(index);
            split_msgs.push(msg);
        }

        let (conns_tx, conns_rx) = backend.split();
        let responses = conns_rx
            .take(1)
            .into_future()
            .map_err(|(err, _)| err)
            .and_then(|(server, _)| {
                debug!("[redis batch] unwrapped connection to backend");
                server.unwrap()
            })
            .and_then(move |server| {
                debug!("[redis batch] writing batched messages to server");
                redis::write_messages(server, split_msgs)
            })
            .and_then(move |(server, _n)| {
                debug!(
                    "[redis batch] trying to read batch of {} messages from server",
                    msg_len
                );
                redis::read_messages(server, msg_len)
            })
            .and_then(|(server, _n, resps)| conns_tx.send(server).map(move |_| resps))
            .and_then(move |resps| {
                ok(msg_indexes
                    .into_iter()
                    .zip(resps)
                    .collect::<OrderedMessages>())
            });

        queues.push(responses);
    }

    queues
}

fn get_message_key(msg: &RedisMessage) -> BytesMut {
    match msg {
        RedisMessage::Bulk(_, ref args) => {
            let arg_pos = if args.len() < 2 { 0 } else { 1 };

            match args.get(arg_pos) {
                Some(RedisMessage::Data(buf, offset)) => {
                    let mut buf2 = buf.clone();
                    let _ = buf2.split_to(*offset);
                    let key_len = buf2.len() - 2;
                    let _ = buf2.split_off(key_len);
                    buf2
                }
                _ => panic!("command message does not have expected structure"),
            }
        }
        _ => panic!("command message should be multi-bulk!"),
    }
}
