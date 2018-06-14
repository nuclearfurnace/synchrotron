use backend::pool::BackendPool;
use backend::sync::{RequestTransformer, TcpStreamFuture};
use bytes::BytesMut;
use futures::future::{ok, result};
use futures::prelude::*;
use protocol::redis;
use protocol::redis::{RedisMessage, RedisOrderedMessages};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
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
) -> Vec<impl Future<Item = RedisOrderedMessages, Error = Error>> {
    // Group all of our messages by their target backend index.
    let mut assigned_msgs = HashMap::new();

    let mut i = 0;
    while messages.len() > 0 {
        let msg = messages.remove(0);
        let backend_idx = {
            let msg_key = get_message_key(&msg);
            pool.get_backend_index(msg_key)
        };

        let batched_msgs = assigned_msgs.entry(backend_idx).or_insert(Vec::new());
        batched_msgs.push((i, msg));

        i += 1;
    }

    // Now that we have things mapped, create our queues for each backend.
    let mut responses = Vec::with_capacity(assigned_msgs.len());

    for (backend_idx, msgs) in assigned_msgs {
        let mut msg_indexes = Vec::with_capacity(msgs.len());
        for (id, _) in &msgs {
            msg_indexes.push(*id);
        }

        let mut backend = pool.get_backend_by_index(backend_idx);
        let msg_indexes2 = msg_indexes.clone();
        let response = backend
            .submit(msgs)
            .and_then(|r| {
                result(r).or_else(move |err| ok(to_vectored_error_response(err, msg_indexes2)))
            })
            .map_err(|_| Error::new(ErrorKind::Other, "internal synchrotron failure (RRCE)"))
            .or_else(move |err| ok(to_vectored_error_response(err, msg_indexes)));

        responses.push(response);
    }

    responses
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
        _ => panic!("command message should be multi-bulk!"),
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
