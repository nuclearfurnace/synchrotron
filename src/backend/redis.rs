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
use backend::processor::{ProcessorError, RequestProcessor, TcpStreamFuture};
use bytes::BytesMut;
use common::OrderedMessages;
use futures::{future::ok, prelude::*};
use itoa;
use protocol::{
    self,
    redis::{self, RedisMessage, RedisMessageSink, RedisMessageStream},
};
use std::{collections::HashMap, error::Error, io};
use tokio::{
    io::{AsyncRead, ReadHalf, WriteHalf},
    net::TcpStream,
};

#[derive(Clone)]
pub struct RedisRequestProcessor;

impl RedisRequestProcessor {
    pub fn new() -> RedisRequestProcessor { RedisRequestProcessor {} }
}

impl RequestProcessor for RedisRequestProcessor {
    type ClientReader = RedisMessageStream<ReadHalf<TcpStream>>;
    type ClientWriter = RedisMessageSink<WriteHalf<TcpStream>>;
    type Future = Box<Future<Item = (TcpStream, OrderedMessages<Self::Message>), Error = io::Error> + Send>;
    type Message = RedisMessage;

    fn is_fragmented(&self, msg: &Self::Message) -> bool { redis_is_multi_message(msg) }

    fn get_fragments(&self, msg: Self::Message) -> Result<Vec<Self::Message>, ProcessorError> {
        redis_fragment_message(msg)
    }

    fn defragment_messages(
        &self, msgs: OrderedMessages<Self::Message>, ops: HashMap<u64, Self::Message>,
    ) -> Result<OrderedMessages<Self::Message>, ProcessorError> {
        redis_defragment_messages(msgs, ops)
    }

    fn get_error_message(&self, e: Box<Error>) -> Self::Message { RedisMessage::from_error(e) }

    fn get_client_streams(&self, client: TcpStream) -> (Self::ClientReader, Self::ClientWriter) {
        let (client_rx, client_tx) = client.split();

        let rd = protocol::redis::read_messages_stream(client_rx);
        let wr = protocol::redis::write_messages_sink(client_tx);

        (rd, wr)
    }

    fn process(&self, req: OrderedMessages<Self::Message>, stream: TcpStreamFuture) -> Self::Future {
        let inner = stream
            .and_then(move |server| redis::write_server_ordered_messages(server, req))
            .and_then(move |(server, msgs, _nw)| redis::read_messages(server, msgs))
            .and_then(move |(server, resps)| ok((server, resps)));
        Box::new(inner)
    }
}

fn redis_fragment_message(msg: RedisMessage) -> Result<Vec<RedisMessage>, ProcessorError> {
    match msg {
        RedisMessage::Bulk(_, mut args) => {
            // Split off the actual command string and figure out what the new command string
            // will be for our fragments.
            let cmd = args.remove(0);
            let cmd_buf = redis_get_data_buffer(&cmd);
            let new_cmd_buf = match cmd_buf {
                Some(buf) => {
                    match buf {
                        b"mget" => b"get",
                        b"del" => b"del",
                        b"mset" => b"set",
                        x => {
                            return Err(ProcessorError::FragmentError(format!(
                                "tried to fragment command '{:?}' but command is not fragmentable!",
                                x
                            )))
                        },
                    }
                },
                None => {
                    return Err(ProcessorError::FragmentError(format!(
                        "tried to fragment bulk message with non-data argument in position 0!"
                    )))
                },
            };

            let cmd_arg = redis_new_data_buffer(&new_cmd_buf[..]);

            // Now do the actual spliting.  Depending on the command, we may take one or two
            // items.  We build a new bulk message based on the replacement command plus the args
            // that we peel off.
            let arg_take_cnt = if new_cmd_buf == b"set" { 2 } else { 1 };

            let mut fragments = Vec::new();
            fragments.push(cmd);
            while args.len() > 0 {
                if args.len() < arg_take_cnt {
                    return Err(ProcessorError::FragmentError(format!(
                        "tried to take {} arguments, only {} left!",
                        arg_take_cnt,
                        args.len()
                    )));
                }

                // This is contorted but we split off the first N arguments, which leaves `args`
                // with those N and `new_args` with the rest.  We feed those to a function which
                // builds us our new message, and then finally we replace `args` with `new_args`
                // so that we can continue on.
                let new_args = args.split_off(arg_take_cnt);
                args.insert(0, cmd_arg.clone());
                let new_bulk = redis_new_bulk_from_args(args);
                fragments.push(new_bulk);

                args = new_args;
            }

            Ok(fragments)
        },
        x => Ok(vec![x]),
    }
}

fn redis_defragment_messages(
    msgs: OrderedMessages<RedisMessage>, ops: HashMap<u64, RedisMessage>,
) -> Result<OrderedMessages<RedisMessage>, ProcessorError> {
    let mut new_msgs = Vec::with_capacity(msgs.len());
    let mut fragmented_parts = HashMap::new();

    // For each message, if it has a fragment ID of -1, it's standalone, so just store it.  If it
    // has anything else, it's a fragment, and we need to track it so we can compress it down at
    // the end.
    for (id, fragment_id, msg) in msgs {
        if fragment_id < 0 {
            new_msgs.push((id, fragment_id, msg));
        } else {
            let fragment_batch = fragmented_parts.entry(id).or_insert(Vec::new());
            fragment_batch.insert(fragment_id as usize, msg);
        }
    }

    // Now we build new messages from the fragments.  Go through our ops map to figure out what
    // sort of message we should be building.
    for (id, op) in ops {
        match redis_get_data_buffer(&op) {
            Some(cmd) => {
                match cmd {
                    b"mget" => {
                        // MGET is straight-forward.  Take all the fragments we got, and create a bulk message.
                        let fragments = fragmented_parts
                            .remove(&id)
                            .ok_or(ProcessorError::DefragmentError(format!("no fragment batch for MGET")))?;
                        let mut new_buf = redis_new_bulk_buffer(fragments.len());
                        for fragment in &fragments {
                            new_buf.unsplit(fragment.get_buf());
                        }

                        new_msgs.push((id, -1, RedisMessage::Bulk(new_buf, fragments)));
                    },
                    b"del" => {
                        // DEL returns the number of keys it deleted, so we have to tally up the
                        // integer responses.
                        let mut keys_deleted = 0;
                        let fragments = fragmented_parts
                            .remove(&id)
                            .ok_or(ProcessorError::DefragmentError(format!("no fragment batch for DEL")))?;
                        for fragment in &fragments {
                            match fragment {
                                RedisMessage::Integer(_, value) => keys_deleted += value,
                                _ => {
                                    return Err(ProcessorError::DefragmentError(format!(
                                        "non-integer response for DEL!"
                                    )))
                                },
                            }
                        }

                        new_msgs.push((id, -1, RedisMessage::from_integer(keys_deleted)));
                    },
                    b"mset" => {
                        // MSET apparently "can't fail", and supposedly only ever returns OK, so just
                        // blindly push in an OK message.
                        new_msgs.push((id, -1, RedisMessage::OK));
                    },
                    x => {
                        return Err(ProcessorError::DefragmentError(format!(
                            "unaccounted for fragment op: {:?}",
                            x
                        )))
                    },
                }
            },
            None => {
                return Err(ProcessorError::DefragmentError(format!(
                    "fragment op was not data message"
                )))
            },
        }
    }

    Ok(new_msgs)
}

fn redis_get_data_buffer(msg: &RedisMessage) -> Option<&[u8]> {
    match msg {
        RedisMessage::Data(buf, offset) => Some(redis_clean_data(buf, *offset)),
        _ => None,
    }
}

fn redis_is_multi_message(msg: &RedisMessage) -> bool {
    match msg {
        RedisMessage::Bulk(_, args) => {
            match args.len() {
                0 => false,
                _ => {
                    let arg = &args[0];
                    match redis_get_data_buffer(arg) {
                        Some(buf) => {
                            match buf {
                                b"mget" | b"mset" | b"del" => true,
                                _ => false,
                            }
                        },
                        None => false,
                    }
                },
            }
        },
        _ => false,
    }
}

fn redis_clean_data<'a>(buf: &'a BytesMut, offset: usize) -> &'a [u8] {
    assert!(buf.len() > 2);
    let val_len = buf.len() - 2;
    &buf[offset..val_len]
}

fn redis_new_data_buffer(buf: &[u8]) -> RedisMessage {
    let mut new_buf = BytesMut::new();
    new_buf.extend_from_slice(b"$");

    let mut cnt_buf = [b'\0'; 20];
    let n = itoa::write(&mut cnt_buf[..], buf.len()).unwrap();
    new_buf.extend_from_slice(&cnt_buf[..n]);
    new_buf.extend_from_slice(b"\r\n");
    new_buf.extend_from_slice(buf);
    new_buf.extend_from_slice(b"\r\n");

    RedisMessage::Data(new_buf, 1 + n + 2)
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

fn redis_new_bulk_from_args(args: Vec<RedisMessage>) -> RedisMessage {
    let mut buf = redis_new_bulk_buffer(args.len());
    let mut new_args = Vec::new();
    for arg in args {
        let arg_buf = arg.get_buf();
        buf.unsplit(arg_buf);

        new_args.push(arg);
    }

    RedisMessage::Bulk(buf, new_args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    const STATUS_BUF: &str = "StAtUs_BuF";
    const DATA_BUF: &[u8; 8] = b"DaTa_BuF";
    const DATA_BUF_2: &[u8; 10] = b"DaTa_BuF_2";
    const DATA_BUF_3: &[u8; 4] = b"mget";

    lazy_static! {
        static ref NULL_MSG: RedisMessage = RedisMessage::Null;
        static ref OK_MSG: RedisMessage = RedisMessage::OK;
        static ref STATUS_MSG: RedisMessage = RedisMessage::from_status(&STATUS_BUF[..]);
        static ref ERR_MSG: RedisMessage =
            RedisMessage::from_error(Box::new(Error::new(ErrorKind::Other, "fake error message")));
        static ref INT_MSG: RedisMessage = RedisMessage::from_integer(-42);
        static ref DATA_MSG: RedisMessage = redis_new_data_buffer(&DATA_BUF[..]);
        static ref DATA_MSG_2: RedisMessage = redis_new_data_buffer(&DATA_BUF_2[..]);
        static ref DATA_MSG_3: RedisMessage = redis_new_data_buffer(&DATA_BUF_3[..]);
        static ref BULK_MSG: RedisMessage =
            redis_new_bulk_from_args(vec![DATA_MSG.clone(), DATA_MSG_2.clone(), DATA_MSG_3.clone()]);
        static ref BULK_MULTI_MSG: RedisMessage =
            redis_new_bulk_from_args(vec![DATA_MSG_3.clone(), DATA_MSG_2.clone(), DATA_MSG.clone()]);
    }

    #[test]
    fn test_is_multi_message() {
        assert!(!redis_is_multi_message(&NULL_MSG));
        assert!(!redis_is_multi_message(&OK_MSG));
        assert!(!redis_is_multi_message(&STATUS_MSG));
        assert!(!redis_is_multi_message(&ERR_MSG));
        assert!(!redis_is_multi_message(&INT_MSG));
        assert!(!redis_is_multi_message(&DATA_MSG));
        assert!(!redis_is_multi_message(&BULK_MSG));
        assert!(redis_is_multi_message(&BULK_MULTI_MSG));
    }

    #[test]
    fn test_get_data_buffer() {
        let nm_buf = redis_get_data_buffer(&NULL_MSG);
        let om_buf = redis_get_data_buffer(&OK_MSG);
        let sm_buf = redis_get_data_buffer(&STATUS_MSG);
        let em_buf = redis_get_data_buffer(&ERR_MSG);
        let im_buf = redis_get_data_buffer(&INT_MSG);
        let dm_buf = redis_get_data_buffer(&DATA_MSG);
        let bm_buf = redis_get_data_buffer(&BULK_MSG);

        assert!(nm_buf.is_none());
        assert!(om_buf.is_none());
        assert!(sm_buf.is_none());
        assert!(em_buf.is_none());
        assert!(im_buf.is_none());
        assert!(dm_buf.is_some());
        assert_eq!(dm_buf, Some(&DATA_BUF[..]));
        assert!(bm_buf.is_none());
    }
}
