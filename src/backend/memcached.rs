// Copyright (c) 2019 Nuclear Furnace
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
    backend::{
        processor::{Processor, ProcessorError},
    },
    common::{EnqueuedRequests, Message, MessageState, ConnectionFuture},
    protocol::{
        memcached::{self, MemcachedMessage, MemcachedTransport},
    },
};
use bytes::BytesMut;
use itoa;
use std::{borrow::Borrow, error::Error, net::SocketAddr};
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct MemcachedProcessor;

impl MemcachedProcessor {
    pub fn new() -> MemcachedProcessor { MemcachedProcessor {} }
}

impl Processor for MemcachedProcessor {
    type Message = MemcachedMessage;
    type Transport = MemcachedTransport<TcpStream>;

    fn fragment_message(&self, msg: Self::Message) -> Result<Vec<(MessageState, Self::Message)>, ProcessorError> {
        memcached_fragment_message(msg)
    }

    fn defragment_message(&self, fragments: Vec<(MessageState, Self::Message)>) -> Result<Self::Message, ProcessorError> {
        memcached_defragment_message(fragments)
    }

    fn get_error_message(&self, e: Box<dyn Error>) -> Self::Message { MemcachedMessage::from_error(e) }

    fn get_error_message_str(&self, e: &str) -> Self::Message { MemcachedMessage::from_error_str(e) }

    fn get_transport(&self, client: TcpStream) -> Self::Transport { MemcachedTransport::new(client) }

    fn preconnect(&self, addr: SocketAddr, noreply: bool) -> ConnectionFuture {
        let inner = async move {
            let mut conn = TcpStream::connect(&addr).await?;
            Ok(conn)
        };

        ConnectionFuture::new(inner)
    }

    fn process(&self, req: EnqueuedRequests<Self::Message>, mut conn: TcpStream) -> ConnectionFuture {
        let inner = async move {
            let (msgs, _n) = memcached::write_messages(&mut conn, req).await?;
            let _n = memcached::read_messages(&mut conn, msgs).await?;
            Ok(conn)
        };

        ConnectionFuture::new(inner)
    }
}

fn memcached_fragment_message(msg: MemcachedMessage) -> Result<Vec<(MessageState, MemcachedMessage)>, ProcessorError> {
    let mut fragments = Vec::new();

    if !memcached_is_multi_message(&msg) {
        // This message isn't fragmentable, so it passes through untouched.
        let state = if msg.is_inline() {
            MessageState::Inline
        } else {
            MessageState::Standalone
        };
        fragments.push((state, msg));
    } else {
        match msg {
            MemcachedMessage::Retrieval(buf, cmd, ref mut opts) => {
                let cmd_buf = match cmd {
                    // We just need the command.
                    MemcachedCommand::Get => opts.remove(0).clone(),
                    MemcachedCommand::Gat => {
                        // We need the command _and_ the expiration time.
                        let cmd = opts.keys.remove(0).clone();
                        let expiration = opts.keys.remove(0).clone();
                        let mut buf = BytesMut::with_capacity(4 + expiration.len());
                        buf.put(cmd);
                        buf.put_slice(b" ");
                        buf.put(expiration);
                        buf.freeze()
                    },
                    _ => unreachable!("got retrieval message with command that isn't get/gat!"),
                };

                // Now we'll do the actual splitting.  For each key, we build a brand new message
                // and prepend our original command to the key.
                let total_fragments = opts.keys.len();
                let mut fragment_count = 0;
                while !opts.keys.is_empty() {
                    // This is contorted but we split off the first N arguments, which leaves `args`
                    // with those N and `new_args` with the rest.  We feed those to a function which
                    // builds us our new message, and then finally we replace `args` with `new_args`
                    // so that we can continue on.
                    let new_args = args.split_off(arg_take_cnt);
                    args.insert(0, cmd_arg.clone());
                    let new_bulk = memcached_new_bulk_from_args(args);
                    let is_last = new_args.is_empty();

                    let state = if is_streaming {
                        MessageState::StreamingFragmented(, is_last)
                    } else {
                        // Normal fragments need to know the command they're being used for so
                        // we can properly form a command-specific response when we ultimately
                        // defragment these messages later on.
                    let state = MessageState::Fragmented(cmd_type.clone(), fragment_count, total_fragments)
                    };

                    fragments.push((state, new_bulk));
                    fragment_count += 1;
                    args = new_args;
                }
            },
            _ => unreachable!(),
        }
    }

    Ok(fragments)
}

fn memcached_defragment_message(mut fragments: Vec<(MessageState, MemcachedMessage)>) -> Result<MemcachedMessage, ProcessorError> {
    // This shouldn't happen but it's a simple invariant that lets me write slightly cleaner code.
    if fragments.is_empty() {
        return Ok(MemcachedMessage::Null);
    }

    // This handles inline and standalone messages.
    //
    // Invariant: if there's only a single response, then even though it may have been a message
    // that is _capable_ of being fragmented, we know it wasn't _actually_ fragmented.
    if fragments.len() == 1 {
        let (_state, msg) = fragments.remove(0);
        return Ok(msg);
    }

    // Peek at the metadata buffer on the first message.  If it's not a fragmented message, then
    // something isn't right and we need to bomb out.
    let first = fragments.first().unwrap();
    let cmd_type = match first {
        (MessageState::Fragmented(buf, _, _), _) => buf.clone(),
        _ => {
            return Err(ProcessorError::DefragmentError(
                "tried to defragment messages, but got non-fragmented message in list".to_owned(),
            ));
        },
    };

    // We have the command type, so let's actually defragment now.
    match cmd_type.borrow() {
        // DEL returns the number of keys it deleted, so we have to tally up the integer responses.
        memcached_DEL => {
            let mut keys_deleted = 0;
            for (_state, fragment) in fragments {
                match fragment {
                    MemcachedMessage::Integer(_, value) => keys_deleted += value,
                    MemcachedMessage::Error(_, _) => return Ok(fragment),
                    _ => {
                        return Err(ProcessorError::DefragmentError(
                            "non-integer response for DEL!".to_owned(),
                        ));
                    },
                }
            }

            Ok(MemcachedMessage::from_integer(keys_deleted))
        },
        memcached_SET => {
            // MSET is funny because it says it can't fail, but really, the command has no failure
            // mode _except_ for, like, you know, the server running out of memory.  However, MSET
            // also promises to be atomic.
            //
            // So, we have to lie a bit here.  If we get back an error, other things could have
            // completed, but we'll send back the first error we iterate over so we can at least
            // inform the caller that _something_ bad happened.  If we see no errors, we assume
            // everything went well, and send back the "normal" OK message.
            for (_state, fragment) in fragments {
                if let MemcachedMessage::Error(_, _) = fragment {
                    return Ok(fragment);
                }
            }

            Ok(MemcachedMessage::OK)
        },
        x => {
            Err(ProcessorError::DefragmentError(format!(
                "unknown command type '{:?}'",
                x
            )))
        },
    }
}

fn memcached_get_data_buffer(msg: &MemcachedMessage) -> Option<&[u8]> {
    match msg {
        MemcachedMessage::Data(buf, offset) => Some(memcached_clean_data(buf, *offset)),
        _ => None,
    }
}

fn memcached_is_multi_message(msg: &MemcachedMessage) -> bool {
    match msg {
        MemcachedMessage::Bulk(_, args) => {
            match args.len() {
                0 => false,
                _ => {
                    let arg = &args[0];
                    match memcached_get_data_buffer(arg) {
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

fn memcached_clean_data(buf: &BytesMut, offset: usize) -> &[u8] {
    assert!(buf.len() > 2);
    let val_len = buf.len() - 2;
    &buf[offset..val_len]
}

fn memcached_new_data_buffer(buf: &[u8]) -> MemcachedMessage {
    let mut new_buf = BytesMut::new();
    new_buf.extend_from_slice(b"$");

    let mut cnt_buf = [b'\0'; 20];
    let n = itoa::write(&mut cnt_buf[..], buf.len()).unwrap();
    new_buf.extend_from_slice(&cnt_buf[..n]);
    new_buf.extend_from_slice(b"\r\n");
    new_buf.extend_from_slice(buf);
    new_buf.extend_from_slice(b"\r\n");

    MemcachedMessage::Data(new_buf, 1 + n + 2)
}

fn memcached_new_bulk_buffer(arg_count: usize) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"*");

    let mut cnt_buf = [b'\0'; 20];
    let n = itoa::write(&mut cnt_buf[..], arg_count).unwrap();
    buf.extend_from_slice(&cnt_buf[..n]);
    buf.extend_from_slice(b"\r\n");
    buf
}

fn memcached_new_bulk_from_args(args: Vec<MemcachedMessage>) -> MemcachedMessage {
    let mut buf = memcached_new_bulk_buffer(args.len());
    let mut new_args = Vec::new();
    for arg in args {
        let arg_buf = arg.get_buf();
        buf.unsplit(arg_buf);

        new_args.push(arg);
    }

    MemcachedMessage::Bulk(buf, new_args)
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
        static ref NULL_MSG: MemcachedMessage = MemcachedMessage::Null;
        static ref OK_MSG: MemcachedMessage = MemcachedMessage::OK;
        static ref STATUS_MSG: MemcachedMessage = MemcachedMessage::from_status(&STATUS_BUF[..]);
        static ref ERR_MSG: MemcachedMessage =
            MemcachedMessage::from_error(Box::new(Error::new(ErrorKind::Other, "fake error message")));
        static ref INT_MSG: MemcachedMessage = MemcachedMessage::from_integer(-42);
        static ref DATA_MSG: MemcachedMessage = memcached_new_data_buffer(&DATA_BUF[..]);
        static ref DATA_MSG_2: MemcachedMessage = memcached_new_data_buffer(&DATA_BUF_2[..]);
        static ref DATA_MSG_3: MemcachedMessage = memcached_new_data_buffer(&DATA_BUF_3[..]);
        static ref BULK_MSG: MemcachedMessage =
            memcached_new_bulk_from_args(vec![DATA_MSG.clone(), DATA_MSG_2.clone(), DATA_MSG_3.clone()]);
        static ref BULK_MULTI_MSG: MemcachedMessage =
            memcached_new_bulk_from_args(vec![DATA_MSG_3.clone(), DATA_MSG_2.clone(), DATA_MSG.clone()]);
    }

    #[test]
    fn test_is_multi_message() {
        assert!(!memcached_is_multi_message(&NULL_MSG));
        assert!(!memcached_is_multi_message(&OK_MSG));
        assert!(!memcached_is_multi_message(&STATUS_MSG));
        assert!(!memcached_is_multi_message(&ERR_MSG));
        assert!(!memcached_is_multi_message(&INT_MSG));
        assert!(!memcached_is_multi_message(&DATA_MSG));
        assert!(!memcached_is_multi_message(&BULK_MSG));
        assert!(memcached_is_multi_message(&BULK_MULTI_MSG));
    }

    #[test]
    fn test_get_data_buffer() {
        let nm_buf = memcached_get_data_buffer(&NULL_MSG);
        let om_buf = memcached_get_data_buffer(&OK_MSG);
        let sm_buf = memcached_get_data_buffer(&STATUS_MSG);
        let em_buf = memcached_get_data_buffer(&ERR_MSG);
        let im_buf = memcached_get_data_buffer(&INT_MSG);
        let dm_buf = memcached_get_data_buffer(&DATA_MSG);
        let bm_buf = memcached_get_data_buffer(&BULK_MSG);

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
