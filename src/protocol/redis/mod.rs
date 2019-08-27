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
use crate::{
    common::{EnqueuedRequests, Message},
    protocol::errors::ProtocolError,
    util::Sizable,
};
use btoi::btoi;
use bytes::{BufMut, BytesMut};
use futures::{ready, stream::Stream, sink::Sink};
use itoa;
use std::{
    io,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, Error, ErrorKind};
use pin_project::pin_project;

mod filtering;
use self::filtering::check_command_validity;

const MAX_OUTSTANDING_WBUF: usize = 8192;

const REDIS_COMMAND_ERROR: u8 = b'-';
const REDIS_COMMAND_STATUS: u8 = b'+';
const REDIS_COMMAND_INTEGER: u8 = b':';
const REDIS_COMMAND_DATA: u8 = b'$';
const REDIS_COMMAND_BULK: u8 = b'*';

const REDIS_NULL_BUF: [u8; 5] = [b'$', b'-', b'1', b'\r', b'\n'];
const REDIS_OK_BUF: [u8; 5] = [b'+', b'O', b'K', b'\r', b'\n'];
const REDIS_PING_RESP_BUF: [u8; 7] = [b'+', b'P', b'O', b'N', b'G', b'\r', b'\n'];
const REDIS_STATUS_BUF: [u8; 1] = [REDIS_COMMAND_STATUS];
const REDIS_ERR_BUF: [u8; 5] = [b'-', b'E', b'R', b'R', b' '];
const REDIS_INT_BUF: [u8; 1] = [REDIS_COMMAND_INTEGER];
const REDIS_CRLF: [u8; 2] = [b'\r', b'\n'];
const REDIS_BACKEND_CLOSED: &str = "backend closed prematurely";

/// A Redis-specific transport.
#[pin_project]
pub struct RedisTransport<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    #[pin]
    transport: T,
    rbuf: BytesMut,
    wbuf: BytesMut,
    closed: bool,
}

pub struct RedisMultipleMessages<'a, T>
where
    T: AsyncRead + Unpin,
{
    transport: &'a mut T,
    rbuf: BytesMut,
    bytes_read: usize,
    msgs: EnqueuedRequests<RedisMessage>,
}

/// A RESP-based client/server message for Redis.
///
/// For all possible responses that contain dynamic data, we provide the full message as a BytesMut
/// buffer in the 1st field slot.  The 2nd field slot contains the offset into the buffer where the
/// value exists.
///
/// For example, for a bulk message containing the string "foobar", the 1st field would represent the
/// entire message payload of "*6\r\nfoobar\r\n", but the 2nd field would be 4, pointing to the 5th
/// slot of the buffer.
///
/// This means that callers themselves must chop off any remaining data, such as the trailing CRLF
/// for data values.
#[derive(Clone, Debug, PartialEq)]
pub enum RedisMessage {
    Null,
    OK,
    Ping,
    Quit,
    Status(BytesMut, usize),
    Error(BytesMut, usize),
    Integer(BytesMut, i64),
    Data(BytesMut, usize),
    Bulk(BytesMut, Vec<RedisMessage>),
}

impl RedisMessage {
    pub fn from_inline(cmd: &str) -> RedisMessage {
        let args = cmd
            .split_whitespace()
            .map(|part| {
                let buf = part.as_bytes();
                let mut new_buf = BytesMut::new();
                new_buf.extend_from_slice(&[REDIS_COMMAND_DATA]);

                let mut cnt_buf = [b'\0'; 20];
                let n = itoa::write(&mut cnt_buf[..], buf.len()).unwrap();
                new_buf.extend_from_slice(&cnt_buf[..n]);
                new_buf.extend_from_slice(b"\r\n");
                new_buf.extend_from_slice(buf);
                new_buf.extend_from_slice(b"\r\n");

                RedisMessage::Data(new_buf, 1 + n + 2)
            })
            .collect::<Vec<_>>();

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[REDIS_COMMAND_BULK]);

        let mut cnt_buf = [b'\0'; 20];
        let n = itoa::write(&mut cnt_buf[..], args.len()).unwrap();
        buf.extend_from_slice(&cnt_buf[..n]);
        buf.extend_from_slice(b"\r\n");

        for arg in &args {
            let arg_buf = arg.get_buf();
            buf.unsplit(arg_buf);
        }

        RedisMessage::Bulk(buf, args)
    }

    pub fn from_status(status_str: &str) -> RedisMessage {
        let bytes = status_str.as_bytes();
        let mut rd = BytesMut::with_capacity(REDIS_STATUS_BUF.len() + bytes.len() + 2);
        rd.put_slice(&REDIS_STATUS_BUF[..]);
        rd.put_slice(&bytes);
        rd.put_slice(&REDIS_CRLF[..]);

        RedisMessage::Status(rd, 1)
    }

    pub fn from_error(e: Box<dyn std::error::Error>) -> RedisMessage {
        let error_str = e.description();
        let bytes = error_str.as_bytes();

        let mut rd = BytesMut::with_capacity(REDIS_ERR_BUF.len() + bytes.len() + 2);
        rd.put_slice(&REDIS_ERR_BUF[..]);
        rd.put_slice(&bytes);
        rd.put_slice(&REDIS_CRLF[..]);

        RedisMessage::Error(rd, 5)
    }

    pub fn from_error_str(error_str: &str) -> RedisMessage {
        let bytes = error_str.as_bytes();

        let mut rd = BytesMut::with_capacity(REDIS_ERR_BUF.len() + bytes.len() + 2);
        rd.put_slice(&REDIS_ERR_BUF[..]);
        rd.put_slice(&bytes);
        rd.put_slice(&REDIS_CRLF[..]);

        RedisMessage::Error(rd, 5)
    }

    pub fn from_integer(value: i64) -> RedisMessage {
        let mut value_buf = [b'\0'; 20];
        let n = itoa::write(&mut value_buf[..], value).unwrap();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&REDIS_INT_BUF);
        buf.extend_from_slice(&value_buf[..n]);
        buf.extend_from_slice(&REDIS_CRLF[..]);

        RedisMessage::Integer(buf, value)
    }

    pub fn get_command(&self) -> Option<&[u8]> {
        match self {
            RedisMessage::Bulk(_, ref args) => {
                match args.get(0) {
                    Some(RedisMessage::Data(buf, offset)) => {
                        let end = buf.len() - 2;
                        Some(&buf[*offset..end])
                    },
                    _ => None,
                }
            },
            _ => None,
        }
    }

    pub fn into_resp(self) -> BytesMut {
        match self {
            RedisMessage::Null => BytesMut::from(&REDIS_NULL_BUF[..]),
            RedisMessage::OK => BytesMut::from(&REDIS_OK_BUF[..]),
            RedisMessage::Ping => BytesMut::from(&REDIS_PING_RESP_BUF[..]),
            RedisMessage::Quit => BytesMut::from(&REDIS_OK_BUF[..]),
            RedisMessage::Status(buf, _) => buf,
            RedisMessage::Error(buf, _) => buf,
            RedisMessage::Integer(buf, _) => buf,
            RedisMessage::Data(buf, _) => buf,
            RedisMessage::Bulk(buf, _) => buf,
        }
    }

    pub fn get_buf(&self) -> BytesMut {
        match self {
            RedisMessage::Null => BytesMut::from(&REDIS_NULL_BUF[..]),
            RedisMessage::OK => BytesMut::from(&REDIS_OK_BUF[..]),
            RedisMessage::Ping => BytesMut::from(&REDIS_PING_RESP_BUF[..]),
            RedisMessage::Quit => BytesMut::from(&REDIS_OK_BUF[..]),
            RedisMessage::Status(ref buf, _) => buf.clone(),
            RedisMessage::Error(ref buf, _) => buf.clone(),
            RedisMessage::Integer(ref buf, _) => buf.clone(),
            RedisMessage::Data(ref buf, _) => buf.clone(),
            RedisMessage::Bulk(ref buf, _) => buf.clone(),
        }
    }
}

impl Sizable for RedisMessage {
    fn size(&self) -> usize {
        match self {
            RedisMessage::Null => REDIS_NULL_BUF[..].len(),
            RedisMessage::OK => REDIS_OK_BUF[..].len(),
            RedisMessage::Ping => REDIS_PING_RESP_BUF[..].len(),
            RedisMessage::Quit => REDIS_OK_BUF[..].len(),
            RedisMessage::Status(ref buf, _) => buf.len(),
            RedisMessage::Error(ref buf, _) => buf.len(),
            RedisMessage::Integer(ref buf, _) => buf.len(),
            RedisMessage::Data(ref buf, _) => buf.len(),
            RedisMessage::Bulk(ref buf, _) => buf.len(),
        }
    }
}

impl Message for RedisMessage {
    fn key(&self) -> &[u8] {
        match self {
            RedisMessage::Bulk(_, ref args) => {
                let arg_pos = if args.len() < 2 { 0 } else { 1 };

                match args.get(arg_pos) {
                    Some(RedisMessage::Data(buf, offset)) => {
                        let end = buf.len() - 2;
                        &buf[*offset..end]
                    },
                    _ => panic!("command message does not have expected structure"),
                }
            },
            RedisMessage::Data(buf, offset) => {
                let end = buf.len() - 2;
                &buf[*offset..end]
            },
            RedisMessage::Ping => b"ping",
            RedisMessage::Quit => b"quit",
            _ => panic!("message should be multi-bulk or data!"),
        }
    }

    fn is_inline(&self) -> bool {
        match self {
            RedisMessage::Data(_, _) => false,
            RedisMessage::Bulk(_, _) => false,
            _ => true,
        }
    }

    fn into_buf(self) -> BytesMut { self.into_resp() }
}

impl<T> RedisTransport<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(transport: T) -> Self {
        RedisTransport {
            transport,
            rbuf: BytesMut::new(),
            wbuf: BytesMut::new(),
            closed: false,
        }
    }

    fn fill_read_buf(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            self.rbuf.reserve(8192);

            let transport = unsafe { Pin::new_unchecked(&mut self.transport) };
            match ready!(transport.poll_read_buf(cx, &mut self.rbuf)) {
                Ok(0) => return Poll::Ready(Ok(())),
                Ok(_) => continue,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl<T> Stream for RedisTransport<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<RedisMessage, ProtocolError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }

        let socket_closed = self.fill_read_buf(cx).is_ready();

        match read_message(&mut self.rbuf) {
            Poll::Ready(Ok((bytes_read, cmd))) => {
                trace!("[protocol] got message from client! ({} bytes)", bytes_read);

                // If client has quit, mark the stream closed so that we return Ready(None) on the
                // next call to poll.  This is the easiest way to ensure that all messages before
                // this get processed but that we stop the flow of messages and thus close out the
                // connection to the client.
                if let RedisMessage::Quit = cmd {
                    self.closed = true;
                }

                // If this command is invalid, kill the transport.  We also give the transport
                // owner an error message, which is inlined and so we can kill the transport while
                // still sending an error back to the client themselves.
                if let Some(cmd_key) = cmd.get_command() {
                    if !check_command_validity(cmd_key) {
                        self.closed = true;

                        let emsg = RedisMessage::from_error_str("command not valid");
                        return Poll::Ready(Some(Ok(emsg)));
                    }
                }

                Poll::Ready(Some(Ok(cmd)))
            },
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            _ => {
                if socket_closed {
                    // If the socket is closed, let's also close up shop.
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            },
        }
    }
}

impl<T> Sink<RedisMessage> for RedisTransport<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // keep this as a no-op for now; in the future, maybe we hold the actual TCP stream halves
        // and do poll_write_ready or some shit
        let this = self.project();
        if this.wbuf.len() >= MAX_OUTSTANDING_WBUF {
            ready!(this.transport.poll_flush(cx));

            if this.wbuf.len() >= MAX_OUTSTANDING_WBUF {
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: RedisMessage) -> Result<(), Self::Error> {
        self.wbuf.unsplit(item.into_buf());
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        while !this.wbuf.is_empty() {
            let n = ready!(this.transport.as_mut().poll_write(cx, &this.wbuf))?;
            if n == 0 {
                return Poll::Ready(Err(ErrorKind::WriteZero.into()));
            }
            let _ = this.wbuf.split_to(n);
        }

        this.transport.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
}

impl<'a, T> RedisMultipleMessages<'a, T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(transport: &'a mut T, msgs: EnqueuedRequests<RedisMessage>) -> Self {
        RedisMultipleMessages {
            transport,
            rbuf: BytesMut::new(),
            bytes_read: 0,
            msgs,
        }
    }

    fn fill_read_buf(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            self.rbuf.reserve(16384);

            let transport = unsafe { Pin::new_unchecked(&mut self.transport) };
            match ready!(transport.poll_read_buf(cx, &mut self.rbuf)) {
                Ok(0) => return Poll::Ready(Ok(())),
                Ok(n) => {
                    self.bytes_read += n;
                    continue
                },
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl<'a, T> Future for RedisMultipleMessages<'a, T>
where
    T: AsyncRead + Unpin,
{
    type Output = Result<usize, ProtocolError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let socket_closed = self.fill_read_buf(cx).is_ready();

        loop {
            // We've collected all the messages, time to return.
            if self.msgs.is_empty() {
                return Poll::Ready(Ok(self.bytes_read));
            }

            match read_message(&mut self.rbuf) {
                Poll::Ready(Ok((bytes_read, msg))) => {
                    trace!("[protocol] got message from server! ({} bytes)", bytes_read);

                    let mut qmsg = self.msgs.remove(0);
                    qmsg.fulfill(msg)
                },
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                _ => {
                    return if socket_closed {
                        // If the socket is closed, let's also close up shop after responding to
                        // the client with errors.
                        let err = RedisMessage::from_error_str(REDIS_BACKEND_CLOSED);
                        while let Some(mut qmsg) = self.msgs.pop() {
                            qmsg.fulfill(err.clone())
                        }

                        Poll::Ready(Err(ProtocolError::BackendClosedPrematurely))
                    } else {
                        Poll::Pending
                    };
                },
            }
        }
    }
}

pub fn read_messages<T>(rx: &mut T, msgs: EnqueuedRequests<RedisMessage>) -> RedisMultipleMessages<T>
where
    T: AsyncRead + Unpin,
{
    RedisMultipleMessages::new(rx, msgs)
}

fn read_message(rd: &mut BytesMut) -> Poll<Result<(usize, RedisMessage), ProtocolError>> {
    // Check to see if we got any inline commands.
    //
    // This is either shortform commands -- like PING or QUIT -- or hard-coded responses like an OK
    // or null string.  Rather than using the full parser, we can quickly and easily match on those
    // hard-coded buffers.
    if let Some(msg_tuple) = read_inline_messages(rd) {
        return Poll::Ready(Ok(msg_tuple));
    }

    read_message_internal(rd)
}

fn read_inline_messages(rd: &mut BytesMut) -> Option<(usize, RedisMessage)> {
    // The only command we handle in non-RESP format is PING.  This is for simplicity
    // and compatibility with redis-benchmark, and also allowing health checks that don't need to
    // follow the RESP structure.
    if rd.starts_with(&b"ping\r\n"[..]) || rd.starts_with(&b"PING\r\n"[..]) {
        let _ = rd.split_to(6);
        return Some((6, RedisMessage::Ping));
    }

    if rd.starts_with(&b"*1\r\n$4\r\nping\r\n"[..]) || rd.starts_with(&b"*1\r\n$4\r\nPING\r\n"[..]) {
        let _ = rd.split_to(14);
        return Some((14, RedisMessage::Ping));
    }

    if rd.starts_with(&b"quit\r\n"[..]) || rd.starts_with(&b"QUIT\r\n"[..]) {
        let _ = rd.split_to(6);
        return Some((6, RedisMessage::Quit));
    }

    if rd.starts_with(&b"*1\r\n$4\r\nquit\r\n"[..]) || rd.starts_with(&b"*1\r\n$4\r\nQUIT\r\n"[..]) {
        let _ = rd.split_to(14);
        return Some((14, RedisMessage::Quit));
    }

    // See if this is an OK response.
    if rd.starts_with(&b"+OK\r\n"[..]) {
        let _ = rd.split_to(5);
        return Some((5, RedisMessage::OK));
    }

    // See if this is a NULL response.
    if rd.starts_with(&b"$-1\r\n"[..]) {
        let _ = rd.split_to(5);
        return Some((5, RedisMessage::Null));
    }

    None
}

fn read_message_internal(rd: &mut BytesMut) -> Poll<Result<(usize, RedisMessage), ProtocolError>> {
    // Try reading a single byte to see if we have a message.  Match it against known
    // message types, and process accordingly.
    let first = match rd.len() {
        0 => None,
        _ => Some(rd[0]),
    };

    match first {
        None => Poll::Pending,
        Some(t) => {
            match &t {
                &REDIS_COMMAND_BULK => read_bulk(rd),
                &REDIS_COMMAND_DATA => read_data(rd),
                &REDIS_COMMAND_STATUS => read_status(rd),
                &REDIS_COMMAND_ERROR => read_error(rd),
                &REDIS_COMMAND_INTEGER => read_integer(rd),
                x => {
                    debug!("got unknown type sigil: {:?}", x);
                    Poll::Pending
                },
            }
        },
    }
}

fn read_line(rd: &BytesMut) -> Poll<usize> {
    let result = rd
        .windows(2)
        .enumerate()
        .find(|&(_, bytes)| bytes == b"\r\n")
        .map(|(i, _)| i);

    match result {
        Some(v) => Poll::Ready(v),
        None => Poll::Pending,
    }
}

fn read_bulk_count(rd: &mut BytesMut) -> Poll<Result<(usize, usize), ProtocolError>> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let pos = ready!(read_line(rd));

    // Try to extract the bulk count integer, leaving the rest.
    let buf = rd.split_to(pos + 2);
    match btoi::<usize>(&buf[1..pos]) {
        Ok(count) => Poll::Ready(Ok((pos + 2, count))),
        Err(_) => Poll::Ready(Err(ProtocolError::InvalidProtocol)),
    }
}

fn read_integer(rd: &mut BytesMut) -> Poll<Result<(usize, RedisMessage), ProtocolError>> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let pos = ready!(read_line(rd));

    // Try to extract the integer, leaving the rest.
    let value = btoi::<i64>(&rd[1..pos]).map_err(|_| ProtocolError::InvalidProtocol)?;

    // Slice off the entire message.
    let total = pos + 2;
    let buf = rd.split_to(total);

    Poll::Ready(Ok((total, RedisMessage::Integer(buf, value))))
}

fn read_status(rd: &mut BytesMut) -> Poll<Result<(usize, RedisMessage), ProtocolError>> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let pos = ready!(read_line(rd));

    // Slice off the entire message.
    let total = pos + 2;
    let buf = rd.split_to(total);

    Poll::Ready(Ok((total, RedisMessage::Status(buf, 1))))
}

fn read_error(rd: &mut BytesMut) -> Poll<Result<(usize, RedisMessage), ProtocolError>> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let pos = ready!(read_line(rd));

    // Slice off the entire message.
    let total = pos + 2;
    let buf = rd.split_to(total);

    Poll::Ready(Ok((total, RedisMessage::Error(buf, 1))))
}

fn read_data(rd: &mut BytesMut) -> Poll<Result<(usize, RedisMessage), ProtocolError>> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let pos = ready!(read_line(rd));

    match &rd[1] {
        b'-' => {
            // See if this is just a null datum.
            let null_len = btoi::<i8>(&rd[1..pos]).map_err(|_| ProtocolError::InvalidProtocol)?;

            match null_len {
                -1 => Poll::Ready(Ok((pos + 2, RedisMessage::Null))),
                _ => Poll::Ready(Err(ProtocolError::InvalidProtocol)),
            }
        },
        _ => {
            // Try to extract the data length integer, leaving the rest.
            let len = btoi::<usize>(&rd[1..pos]).map_err(|_| ProtocolError::InvalidProtocol)?;

            // See if the actual data is available in the buffer.
            if rd.len() < pos + 2 + len + 2 {
                return Poll::Pending;
            }

            // Slice off the entire message.
            let total = pos + 2 + len + 2;
            let buf = rd.split_to(total);

            Poll::Ready(Ok((total, RedisMessage::Data(buf, pos + 2))))
        },
    }
}

fn read_bulk(rd: &mut BytesMut) -> Poll<Result<(usize, RedisMessage), ProtocolError>> {
    let mut total = 0;
    let mut buf = rd.clone();

    // Get the number of items in the command.
    let (n, count) = ready!(read_bulk_count(&mut buf))?;
    if count < 1 {
        return Poll::Ready(Err(ProtocolError::InvalidProtocol));
    }
    total += n;

    // Loop through, trying to read the number of arguments we were told exist in the message.
    // This can legitimately fail because, at this point, buf might not contain the full message.
    let mut args = Vec::new();
    for _ in 0..count {
        let (n, msg) = ready!(read_message_internal(&mut buf))?;
        total += n;

        args.push(msg);
    }

    // Slice off all the bytes we "read", updating the original in the process, and pass them along.
    let buf = rd.split_to(total);
    Poll::Ready(Ok((total, RedisMessage::Bulk(buf, args))))
}

pub async fn write_raw_message<T>(tx: &mut T, msg: RedisMessage) -> Result<usize, ProtocolError>
where
    T: AsyncWrite + Unpin,
{
    let buf = msg.into_resp();
    let buf_len = buf.len();
    tx.write_all(&buf).await
        .map(move |_| buf_len)
        .map_err(Into::into)
}

pub async fn write_messages<T>(
    tx: &mut T,
    mut msgs: EnqueuedRequests<RedisMessage>,
) -> Result<(EnqueuedRequests<RedisMessage>, usize), ProtocolError>
where
    T: AsyncWrite + Unpin,
{
    let msgs_len = msgs.len();
    let buf = match msgs_len {
        1 => {
            let msg = &mut msgs[0];
            msg.consume().into_resp()
        },
        _ => {
            let mut buf = BytesMut::new();
            for msg in &mut msgs {
                let msg_buf = msg.consume().into_resp();
                buf.extend_from_slice(&msg_buf[..]);
            }
            buf
        },
    };

    let buf_len = buf.len();
    tx.write_all(&buf).await
        .map(move |_| (msgs, buf_len))
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use spectral::prelude::*;
    use test::Bencher;

    static DATA_GET_SIMPLE: &[u8] = b"*2\r\n$3\r\nget\r\n$6\r\nfoobar\r\n";
    static DATA_OK: &[u8] = b"+OK\r\n";
    static DATA_STATUS: &[u8] = b"+LIMITED\r\n";
    static DATA_ERROR: &[u8] = b"-ERR warning limit exceeded\r\n";
    static DATA_NULL: &[u8] = b"$-1\r\n";
    static DATA_BULK_WITH_NULL: &[u8] = b"*2\r\n$3\r\nboo\r\n$-1\r\n";
    static DATA_INTEGER_1337: &[u8] = b":1337\r\n";
    static DATA_SHORT_CIRCUIT_ZERO_DATA: &[u8] = b"";
    static DATA_SHORT_CIRCUIT_NO_ARRAY_CRLF: &[u8] = b"*2";
    static DATA_SHORT_CIRCUIT_NO_ARG_LEN_CRLF: &[u8] = b"*2\r\n$3";
    static DATA_SHORT_CIRCUIT_PARTIAL_ARG: &[u8] = b"*2\r\n$3\r\n";
    static DATA_SHORT_CIRCUIT_MISSING_ARG: &[u8] = b"*2\r\n$3\r\nget\r\n";
    static DATA_SHORT_CIRCUIT_ARG_LEN_PAST_END: &[u8] = b"*2\r\n$3\r\nget\r\n$9\r\nfoobar\r\n";
    static DATA_PING_LOWER: &[u8] = b"ping\r\n";
    static DATA_PING_UPPER: &[u8] = b"PING\r\n";
    static DATA_PING_FULL_LOWER: &[u8] = b"*1\r\n$4\r\nping\r\n";
    static DATA_PING_FULL_UPPER: &[u8] = b"*1\r\n$4\r\nPING\r\n";
    static DATA_QUIT_LOWER: &[u8] = b"quit\r\n";
    static DATA_QUIT_UPPER: &[u8] = b"QUIT\r\n";
    static DATA_QUIT_FULL_LOWER: &[u8] = b"*1\r\n$4\r\nquit\r\n";
    static DATA_QUIT_FULL_UPPER: &[u8] = b"*1\r\n$4\r\nQUIT\r\n";

    fn get_message_from_buf(buf: &[u8]) -> Poll<RedisMessage, ProtocolError> {
        let mut rd = BytesMut::with_capacity(buf.len());
        rd.put_slice(&buf[..]);
        read_message(&mut rd).map(|res| res.map(|(_, msg)| msg))
    }

    fn check_data_matches(msg: RedisMessage, data: &[u8]) {
        match msg {
            RedisMessage::Data(ref buf, offset) => {
                let data_len = buf.len() - 2;
                assert_eq!(&buf[offset..data_len], data);
            },
            _ => panic!("message is not data"),
        }
    }

    fn check_integer_matches(msg: RedisMessage, value: i64) {
        match msg {
            RedisMessage::Integer(_, msg_value) => assert_eq!(msg_value, value),
            _ => panic!("message is not integer"),
        }
    }

    fn check_status_matches(msg: RedisMessage, data: &[u8]) {
        match msg {
            RedisMessage::Status(ref buf, offset) => {
                let data_len = buf.len() - 2;
                assert_eq!(&buf[offset..data_len], data);
            },
            _ => panic!("message is not status"),
        }
    }

    fn check_error_matches(msg: RedisMessage, data: &[u8]) {
        match msg {
            RedisMessage::Error(ref buf, offset) => {
                let data_len = buf.len() - 2;
                assert_eq!(&buf[offset..data_len], data);
            },
            _ => panic!("message is not error"),
        }
    }

    fn check_bulk_matches(mut msg: RedisMessage, values: Vec<&[u8]>) {
        match msg {
            RedisMessage::Bulk(_, ref mut args) => {
                assert_that(args).has_length(values.len());
                for value in &values {
                    check_data_matches(args.remove(0), value);
                }
            },
            _ => panic!("message is not bulk"),
        }
    }

    #[test]
    fn parse_get_simple() {
        let res = get_message_from_buf(&DATA_GET_SIMPLE);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Poll::Ready(msg) => check_bulk_matches(msg, vec![b"get", b"foobar"]),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_ok() {
        let res = get_message_from_buf(&DATA_OK);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Poll::Ready(msg) => assert_eq!(msg, RedisMessage::OK),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_status() {
        let res = get_message_from_buf(&DATA_STATUS);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Poll::Ready(msg) => check_status_matches(msg, b"LIMITED"),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_error() {
        let res = get_message_from_buf(&DATA_ERROR);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Poll::Ready(msg) => check_error_matches(msg, b"ERR warning limit exceeded"),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_null() {
        let res = get_message_from_buf(&DATA_NULL);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Poll::Ready(msg) => assert_eq!(msg, RedisMessage::Null),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_bulk_with_null() {
        let res = get_message_from_buf(&DATA_BULK_WITH_NULL);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Poll::Ready(mut msg) => {
                match msg {
                    RedisMessage::Bulk(_, ref mut args) => {
                        assert_that(args).has_length(2);
                        let arg0 = args.remove(0);
                        let arg1 = args.remove(0);

                        check_data_matches(arg0, b"boo");
                        assert_eq!(arg1, RedisMessage::Null);
                    },
                    _ => panic!("message is not bulk"),
                }
            },
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_integer() {
        let res = get_message_from_buf(&DATA_INTEGER_1337);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Poll::Ready(msg) => check_integer_matches(msg, 1337),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_short_circuit_zero_data() {
        let res = get_message_from_buf(&DATA_SHORT_CIRCUIT_ZERO_DATA);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_short_circuit_no_array_crlf() {
        let res = get_message_from_buf(&DATA_SHORT_CIRCUIT_NO_ARRAY_CRLF);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_short_circuit_no_arg_len_crlf() {
        let res = get_message_from_buf(&DATA_SHORT_CIRCUIT_NO_ARG_LEN_CRLF);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_short_circuit_partial_arg() {
        let res = get_message_from_buf(&DATA_SHORT_CIRCUIT_PARTIAL_ARG);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_short_circuit_missing_arg() {
        let res = get_message_from_buf(&DATA_SHORT_CIRCUIT_MISSING_ARG);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_short_circuit_arg_len_past_end() {
        let res = get_message_from_buf(&DATA_SHORT_CIRCUIT_ARG_LEN_PAST_END);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_ping() {
        match get_message_from_buf(&DATA_PING_LOWER) {
            Ok(Poll::Ready(msg)) => assert_eq!(msg, RedisMessage::Ping),
            _ => panic!("should have had message"),
        }

        match get_message_from_buf(&DATA_PING_UPPER) {
            Ok(Poll::Ready(msg)) => assert_eq!(msg, RedisMessage::Ping),
            _ => panic!("should have had message"),
        }

        match get_message_from_buf(&DATA_PING_FULL_LOWER) {
            Ok(Poll::Ready(msg)) => assert_eq!(msg, RedisMessage::Ping),
            _ => panic!("should have had message"),
        }

        match get_message_from_buf(&DATA_PING_FULL_UPPER) {
            Ok(Poll::Ready(msg)) => assert_eq!(msg, RedisMessage::Ping),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_quit() {
        match get_message_from_buf(&DATA_QUIT_LOWER) {
            Ok(Poll::Ready(msg)) => assert_eq!(msg, RedisMessage::Quit),
            _ => panic!("should have had message"),
        }

        match get_message_from_buf(&DATA_QUIT_UPPER) {
            Ok(Poll::Ready(msg)) => assert_eq!(msg, RedisMessage::Quit),
            _ => panic!("should have had message"),
        }

        match get_message_from_buf(&DATA_QUIT_FULL_LOWER) {
            Ok(Poll::Ready(msg)) => assert_eq!(msg, RedisMessage::Quit),
            _ => panic!("should have had message"),
        }

        match get_message_from_buf(&DATA_QUIT_FULL_UPPER) {
            Ok(Poll::Ready(msg)) => assert_eq!(msg, RedisMessage::Quit),
            _ => panic!("should have had message"),
        }
    }

    #[bench]
    fn bench_parse_get_simple(b: &mut Bencher) { b.iter(|| get_message_from_buf(&DATA_GET_SIMPLE)); }

    #[bench]
    fn bench_short_circuit_0_zero_data(b: &mut Bencher) {
        b.iter(|| get_message_from_buf(&DATA_SHORT_CIRCUIT_ZERO_DATA));
    }

    #[bench]
    fn bench_short_circuit_1_no_array_crlf(b: &mut Bencher) {
        b.iter(|| get_message_from_buf(&DATA_SHORT_CIRCUIT_NO_ARRAY_CRLF));
    }

    #[bench]
    fn bench_short_circuit_2_no_arg_len_crlf(b: &mut Bencher) {
        b.iter(|| get_message_from_buf(&DATA_SHORT_CIRCUIT_NO_ARG_LEN_CRLF));
    }

    #[bench]
    fn bench_short_circuit_3_partial_arg(b: &mut Bencher) {
        b.iter(|| get_message_from_buf(&DATA_SHORT_CIRCUIT_PARTIAL_ARG));
    }

    #[bench]
    fn bench_short_circuit_4_missing_arg(b: &mut Bencher) {
        b.iter(|| get_message_from_buf(&DATA_SHORT_CIRCUIT_MISSING_ARG));
    }

    #[bench]
    fn bench_short_circuit_5_arg_len_past_end(b: &mut Bencher) {
        b.iter(|| get_message_from_buf(&DATA_SHORT_CIRCUIT_ARG_LEN_PAST_END));
    }

    #[bench]
    fn bench_ping_lower(b: &mut Bencher) { b.iter(|| get_message_from_buf(&DATA_PING_LOWER)); }

    #[bench]
    fn bench_ping_upper(b: &mut Bencher) { b.iter(|| get_message_from_buf(&DATA_PING_UPPER)); }
}
