use backend::pool::BackendPool;
use backend::sync::{RequestTransformer, TcpStreamFuture};
use bytes::BytesMut;
use futures::future::{ok, result};
use futures::prelude::*;
use futures::sync::{mpsc, oneshot};
use itoa;
use protocol::redis;
use protocol::redis::{RedisMessage, RedisOrderedMessages};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use tokio;
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct RedisRequestTransformer;

impl RedisRequestTransformer {
    pub fn new() -> RedisRequestTransformer {
        RedisRequestTransformer {}
    }
}

impl RequestTransformer for RedisRequestTransformer {
    type Request = RedisOrderedMessages;
    type Response = RedisOrderedMessages;
    type Executor = Box<Future<Item = (TcpStream, Self::Response), Error = Error> + Send>;

    fn transform(&self, req: Self::Request, stream: TcpStreamFuture) -> Self::Executor {
        let inner = stream
            .and_then(move |server| {
                debug!("[redis backend] about to write batched messages to backend");
                redis::write_ordered_messages(server, req)
            })
            .and_then(move |(server, msgs, _n)| {
                debug!("[redis backend] now reading the responses from the backend");
                redis::read_messages(server, msgs)
            })
            .and_then(move |(server, _n, resps)| {
                debug!("[redis backend] assembling backend responses to send to client");
                ok((server, resps))
            });
        Box::new(inner)
    }
}

pub fn generate_batched_redis_writes(
    pool: &BackendPool<RedisRequestTransformer>,
    mut messages: Vec<RedisMessage>,
) -> Vec<oneshot::Receiver<RedisOrderedMessages>> {
    // Group all of our messages by their target backend index.
    let mut assigned_msgs = HashMap::new();
    let mut responses = Vec::with_capacity(messages.len());

    let mut i = 0;
    while messages.len() > 0 {
        let msg = messages.remove(0);
        if redis_is_multi_message(&msg) {
            let response = redis_fragment_multi_message(pool, i, msg);
            responses.push(response);
        } else {
            let backend_idx = {
                let msg_key = get_message_key(&msg);
                pool.get_backend_index(msg_key)
            };

            let batched_msgs = assigned_msgs.entry(backend_idx).or_insert(Vec::new());
            batched_msgs.push((i, msg));
        }

        i += 1;
    }

    // Now that we have things mapped, create our queues for each backend.
    for (backend_idx, msgs) in assigned_msgs {
        let mut msg_indexes = Vec::with_capacity(msgs.len());
        for (id, _) in &msgs {
            msg_indexes.push(*id);
        }

        let mut backend = pool.get_backend_by_index(backend_idx);
        let msg_indexes2 = msg_indexes.clone();

        let (response_tx, response_rx) = oneshot::channel();
        let response = backend
            .submit(msgs)
            .and_then(|r| {
                result(r).or_else(move |err| ok(to_vectored_error_response(err, msg_indexes2)))
            })
            .map_err(|_| Error::new(ErrorKind::Other, "internal synchrotron failure (RRCE)"))
            .or_else(move |err| ok(to_vectored_error_response(err, msg_indexes)))
            .and_then(|r| response_tx.send(r))
            .map(|_| ())
            .map_err(|_| ());

        tokio::spawn(response);

        responses.push(response_rx);
    }

    responses
}

fn redis_is_multi_message(msg: &RedisMessage) -> bool {
    match msg {
        RedisMessage::Bulk(_, args) => match args.len() {
            0 => false,
            _ => {
                let arg = &args[0];
                match arg {
                    RedisMessage::Data(buf, offset) => {
                        let cmd_key = redis_clean_data(&buf, *offset);
                        match cmd_key {
                            b"mget" | b"mset" | b"del" => true,
                            _ => false,
                        }
                    }
                    _ => false,
                }
            }
        },
        _ => false,
    }
}

fn redis_fragment_multi_message(
    pool: &BackendPool<RedisRequestTransformer>,
    id: u64,
    msg: RedisMessage,
) -> oneshot::Receiver<RedisOrderedMessages> {
    match msg {
        RedisMessage::Bulk(_, mut args) => {
            let command = args.remove(0);
            match command {
                RedisMessage::Data(buf, offset) => {
                    let cmd_str = redis_clean_data(&buf, offset);
                    match cmd_str {
                        b"mget" | b"del" => {
                            // We pull out one argument at a time for each possible fragment,
                            // assigning it to a backend and keeping track of its internal order.
                            //
                            // backend ID -> vector<(intramessage index, message)>
                            let mut batches = HashMap::new();

                            let mut i = 0;
                            while args.len() > 0 {
                                let arg = args.remove(0);
                                let backend_idx = {
                                    let msg_key = get_message_key(&arg);
                                    pool.get_backend_index(msg_key)
                                };

                                let mut batched_msgs =
                                    batches.entry(backend_idx).or_insert(Vec::new());
                                batched_msgs.push((i, arg));

                                i += 1;
                            }

                            // Now transform these into a single command per backend.
                            let batches_len = batches.len();
                            let (batch_tx, batch_rx) = mpsc::channel(batches_len);
                            let mut backend_order_map = HashMap::new();

                            for (backend_idx, msgs) in batches {
                                let mut indexes = Vec::new();

                                // Create a new redis message, using the same command name, and
                                // append all the backend-related keys to it.
                                let arg_count = msgs.len() + 1;
                                let mut new_buf = redis_new_bulk_buffer(arg_count);
                                new_buf.extend_from_slice(&buf);

                                for (id, msg) in msgs {
                                    indexes.push(id);
                                    let ids = backend_order_map
                                        .entry(backend_idx as u64)
                                        .or_insert(Vec::new());
                                    ids.push(id);

                                    new_buf.unsplit(msg.as_resp());
                                }

                                let msg = RedisMessage::Raw(new_buf);
                                let wrapped_msg = vec![(backend_idx as u64, msg)];
                                let tx = batch_tx.clone();

                                // Send this aggregated message to be processed, and forward the
                                // results to the accumulator channel.
                                let mut backend = pool.get_backend_by_index(backend_idx);
                                let indexes2 = indexes.clone();
                                let work = backend
                                    .submit(wrapped_msg)
                                    .and_then(|r| {
                                        result(r).or_else(move |err| {
                                            ok(to_vectored_error_response(err, indexes))
                                        })
                                    })
                                    .map_err(|_| {
                                        Error::new(
                                            ErrorKind::Other,
                                            "internal synchrotron failure (RRCE)",
                                        )
                                    })
                                    .or_else(move |err| {
                                        ok(to_vectored_error_response(err, indexes2))
                                    })
                                    .and_then(move |r| tx.send(r))
                                    .map(|_| ())
                                    .map_err(|_| ());

                                tokio::spawn(work);
                            }

                            // Now we have a map of backend ID/aggregated message, as well as
                            // backend ID/intramessage index, which lets us know which order the
                            // results from a given backend should be sorted to when we assemble
                            // the final message back to the client.
                            let (result_tx, result_rx) = oneshot::channel();
                            let collector = batch_rx
                                .take(batches_len as u64)
                                .collect()
                                .map(move |batches| {
                                    let mut unfragmented = Vec::new();
                                    for batch in batches {
                                        for (backend_idx, msg) in batch {
                                            let mut batch_order =
                                                backend_order_map.remove(&backend_idx).unwrap();
                                            match msg {
                                                RedisMessage::Bulk(_, mut results) => {
                                                    while results.len() > 0 {
                                                        let result = results.remove(0);
                                                        let result_id = batch_order.remove(0);
                                                        unfragmented.push((result_id, result));
                                                    }
                                                }
                                                x => panic!("expected bulk response: {:?}", x),
                                            }
                                        }
                                    }

                                    unfragmented.sort_by(|(a_id, _), (b_id, _)| a_id.cmp(b_id));

                                    let result_count = unfragmented.len();
                                    let mut new_buf = redis_new_bulk_buffer(result_count);

                                    for (_, fragment) in unfragmented {
                                        new_buf.unsplit(fragment.as_resp());
                                    }

                                    let wrapped = vec![(id, RedisMessage::Raw(new_buf))];
                                    result_tx.send(wrapped)
                                })
                                .map(|_| ())
                                .map_err(|e| error!("multi frag err: {:?}", e));

                            tokio::spawn(collector);

                            result_rx
                        }
                        _ => panic!("mset not implemented yet"),
                    }
                }
                _ => panic!("can't fragment bulk message with non-data arguments"),
            }
        }
        _ => panic!("can't fragment non-multi message"),
    }
}

fn redis_clean_data<'a>(buf: &'a BytesMut, offset: usize) -> &'a [u8] {
    let val_len = buf.len() - 2;
    &buf[offset..val_len]
}

fn redis_new_bulk_buffer(arg_count: usize) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"*");

    let mut cnt_buf = [b'\0'; 20];
    let n = itoa::write(&mut cnt_buf[..], arg_count).unwrap();
    buf.extend_from_slice(&cnt_buf[..n]);

    buf.extend_from_slice(b"\r\n");
    buf
}

pub fn to_vectored_error_response(err: Error, indexes: Vec<u64>) -> RedisOrderedMessages {
    let mut msgs = Vec::new();
    let msg = RedisMessage::from_error(err);
    for i in indexes {
        msgs.push((i, msg.clone()));
    }
    msgs
}

fn get_message_key<'a>(msg: &'a RedisMessage) -> &'a [u8] {
    match msg {
        RedisMessage::Bulk(_, ref args) => {
            let arg_pos = if args.len() < 2 { 0 } else { 1 };

            match args.get(arg_pos) {
                Some(RedisMessage::Data(buf, offset)) => {
                    let end = buf.len() - 2;
                    &buf[*offset..end]
                }
                _ => panic!("command message does not have expected structure"),
            }
        }
        RedisMessage::Data(buf, offset) => {
            let end = buf.len() - 2;
            &buf[*offset..end]
        }
        _ => panic!("command message should be multi-bulk or data !"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    static DATA_GET_SIMPLE: &[u8] = b"*2\r\n$3\r\nget\r\n$6\r\nfoobar\r\n";
    static DATA_GET_SIMPLE_ARG0: &[u8] = b"$3\r\nget\r\n";
    static DATA_GET_SIMPLE_ARG1: &[u8] = b"$6\r\nfoobar\r\n";

    fn build_simple_get_command() -> RedisMessage {
        let arg0_data = BytesMut::from(DATA_GET_SIMPLE_ARG0);
        let arg0 = RedisMessage::Data(arg0_data, 4);

        let arg1_data = BytesMut::from(DATA_GET_SIMPLE_ARG1);
        let arg1 = RedisMessage::Data(arg1_data, 4);

        let mut args = Vec::new();
        args.push(arg0);
        args.push(arg1);

        let cmd_buf = BytesMut::from(DATA_GET_SIMPLE);
        RedisMessage::Bulk(cmd_buf, args)
    }

    #[test]
    fn test_get_message_key_simple_get() {
        let cmd = build_simple_get_command();
        let res = get_message_key(&cmd);
        assert_eq!(res, b"foobar");
    }

    #[bench]
    fn bench_get_message_key_simple_get(b: &mut Bencher) {
        let cmd = build_simple_get_command();
        b.iter(|| get_message_key(&cmd));
    }
}
