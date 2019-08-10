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
use futures::prelude::*;
use nom::{
  IResult,
  Err as NomErr,
  branch::alt,
  combinator::map,
  bytes::complete::tag,
  character::complete::crlf,
  sequence::terminated};
use tokio::io::{write_all, AsyncRead, AsyncWrite, Error, ErrorKind};
use std::str;
use std::pin::Pin;

const MAX_OUTSTANDING_WBUF: usize = 8192;

const MC_ERROR_BUF: [u8; 7] = [b'E', b'R', b'R', b'O', b'R', b'\r', b'\n'];
const MC_CLIENT_ERROR_BUF: [u8; 13] = [b'C', b'L', b'I', b'E', b'N', b'T', b'_', b'E', b'R', b'R', b'O', b'R', b' '];
const MC_SERVER_ERROR_BUF: [u8; 13] = [b'S', b'E', b'R', b'V', b'E', b'R', b'_', b'E', b'R', b'R', b'O', b'R', b' '];
const MC_QUIT_BUF: [u8; 6] = [b'q', b'u', b'i', b't', b'\r', b'\n'];
const MC_CRLF: [u8; 2] = [b'\r', b'\n'];
const MC_BACKEND_CLOSED: &str = "backend closed prematurely";

/// A Memcached-specific transport.
pub struct MemcachedTransport<T>
where
    T: AsyncRead + AsyncWrite,
{
    transport: T,
    rbuf: BytesMut,
    wbuf: BytesMut,
    closed: bool,
}

pub struct MemcachedMultipleMessages<T>
where
    T: AsyncRead,
{
    transport: Option<T>,
    rbuf: BytesMut,
    bytes_read: usize,
    msgs: EnqueuedRequests<MemcachedMessage>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MemcachedCommand {
    Get,
    Gets,
    Gat,
    Gats,
    Set,
    Delete,
    Add,
    Replace,
    Append,
    Prepend,
    Touch,
    Increment,
    Decrement,
    CAS,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MemcachedMessage {
    Storage(BytesMut, MemcachedCommand, Option<StorageOptions>),
    Retrieval(BytesMut, MemcachedCommand, Option<RetrievalOptions>),
    Data(BytesMut),
    Unstructured(BytesMut),
    Status(BytesMut),
    GeneralError,
    ClientError(BytesMut),
    ServerError(BytesMut),
    Raw(BytesMut),
    Version,
    Quit,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageOptions {
    flags: u16,
    expiration: u64,
    no_reply: bool,
    cas: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetrievalOptions {
    expiration: u64,
}

impl MemcachedMessage {
    pub fn from_raw(cmd: &str) -> MemcachedMessage {
        let bytes = cmd.as_bytes();

        let mut mbuf = BytesMut::with_capacity(bytes.len() + 2);
        mbuf.put_slice(&bytes);
        mbuf.put_slice(&MC_CRLF[..]);

        MemcachedMessage::Raw(mbuf)
    }

    pub fn from_status(status_str: &str) -> MemcachedMessage {
        let bytes = status_str.as_bytes();

        let mut mbuf = BytesMut::with_capacity(bytes.len() + 2);
        mbuf.put_slice(&bytes);
        mbuf.put_slice(&MC_CRLF[..]);

        MemcachedMessage::Status(mbuf)
    }

    pub fn from_server_error(error_str: &str) -> MemcachedMessage {
        let bytes = error_str.as_bytes();

        let mut mbuf = BytesMut::with_capacity(MC_SERVER_ERROR_BUF.len() + bytes.len() + 2);
        mbuf.put_slice(&MC_SERVER_ERROR_BUF[..]);
        mbuf.put_slice(&bytes);
        mbuf.put_slice(&MC_CRLF[..]);

        MemcachedMessage::ServerError(mbuf)
    }

    pub fn from_client_error(error_str: &str) -> MemcachedMessage {
        let bytes = error_str.as_bytes();

        let mut mbuf = BytesMut::with_capacity(MC_CLIENT_ERROR_BUF.len() + bytes.len() + 2);
        mbuf.put_slice(&MC_CLIENT_ERROR_BUF[..]);
        mbuf.put_slice(&bytes);
        mbuf.put_slice(&MC_CRLF[..]);

        MemcachedMessage::ClientError(mbuf)
    }

    pub fn into_resp(self) -> BytesMut {
        match self {
            MemcachedMessage::Storage(mbuf, _, _) => mbuf,
            MemcachedMessage::Retrieval(mbuf, _, _) => mbuf,
            MemcachedMessage::Unstructured(mbuf) => mbuf,
            MemcachedMessage::Status(mbuf) => mbuf,
            MemcachedMessage::GeneralError => BytesMut::from(&MC_ERROR_BUF[..]),
            MemcachedMessage::ClientError(mbuf) => mbuf,
            MemcachedMessage::ServerError(mbuf) => mbuf,
            MemcachedMessage::Data(mbuf) => mbuf,
            MemcachedMessage::Raw(mbuf) => mbuf,
            MemcachedMessage::Version => BytesMut::new(),
            MemcachedMessage::Quit => BytesMut::from(&MC_QUIT_BUF[..]),
        }
    }

    pub fn get_buf(&self) -> BytesMut {
        match self {
            MemcachedMessage::Storage(mbuf, _, _) => mbuf.clone(),
            MemcachedMessage::Retrieval(mbuf, _, _) => mbuf.clone(),
            MemcachedMessage::Unstructured(mbuf) => mbuf.clone(),
            MemcachedMessage::Status(mbuf) => mbuf.clone(),
            MemcachedMessage::GeneralError => BytesMut::from(&MC_ERROR_BUF[..]),
            MemcachedMessage::ClientError(mbuf) => mbuf.clone(),
            MemcachedMessage::ServerError(mbuf) => mbuf.clone(),
            MemcachedMessage::Data(mbuf) => mbuf.clone(),
            MemcachedMessage::Raw(mbuf) => mbuf.clone(),
            MemcachedMessage::Version => BytesMut::new(),
            MemcachedMessage::Quit => BytesMut::from(&MC_QUIT_BUF[..]),
        }
    }
}

impl Sizable for MemcachedMessage {
    fn size(&self) -> usize {
        match self {
            MemcachedMessage::Storage(mbuf, _, _) => mbuf.len(),
            MemcachedMessage::Retrieval(mbuf, _, _) => mbuf.len(),
            MemcachedMessage::Unstructured(mbuf) => mbuf.len(),
            MemcachedMessage::Status(mbuf) => mbuf.len(),
            MemcachedMessage::GeneralError => MC_ERROR_BUF.len(),
            MemcachedMessage::ClientError(mbuf) => mbuf.len(),
            MemcachedMessage::ServerError(mbuf) => mbuf.len(),
            MemcachedMessage::Data(mbuf) => mbuf.len(),
            MemcachedMessage::Raw(mbuf) => mbuf.len(),
            MemcachedMessage::Version => 0,
            MemcachedMessage::Quit => MC_QUIT_BUF.len(),
        }
    }
}

impl Message for MemcachedMessage {
    fn key(&self) -> &[u8] {
        match self {
            MemcachedMessage::Retrieval(buf, cmd, _) => {
                match cmd {
                    MemcachedCommand::Gat | MemcachedCommand::Gats => {
                        buf.split(|c| *c == 0x20).nth(2).expect("message has no key field")
                    },
                    _ => buf.split(|c| *c == 0x20).nth(1).expect("message has no key field"),
                }
            },
            MemcachedMessage::Storage(buf, _, _) => {
                buf.split(|c| *c == 0x20).nth(1).expect("message has no key field")
            },
            MemcachedMessage::Quit => b"quit",
            _ => panic!("message should be multi-bulk or data!"),
        }
    }

    fn is_inline(&self) -> bool {
        match self {
            MemcachedMessage::Retrieval(_, _, _) => false,
            _ => true,
        }
    }

    fn into_buf(self) -> BytesMut { self.into_resp() }
}

impl<T> MemcachedTransport<T>
where
    T: AsyncRead + AsyncWrite,
{
    pub fn new(transport: T) -> Self {
        MemcachedTransport {
            transport,
            rbuf: BytesMut::new(),
            wbuf: BytesMut::new(),
            closed: false,
        }
    }

    fn fill_read_buf(&mut self) -> Poll<(), ProtocolError> {
        loop {
            self.rbuf.reserve(8192);

            let n = try_ready!(self.transport.read_buf(&mut self.rbuf));
            if n == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl<T> Stream for MemcachedTransport<T>
where
    T: AsyncRead + AsyncWrite,
{
    type Item = Result<MemcachedMessage, ProtocolError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Ok(Async::Ready(None));
        }

        let socket_closed = self.fill_read_buf()?.is_ready();

        match parse_message(&self.rbuf) {
            Ok(Async::Ready((bytes_read, cmd))) => {
                trace!("[protocol] got message from client! ({} bytes)", bytes_read);

                // If client has quit, mark the stream closed so that we return Ready(None) on the
                // next call to poll.  This is the easiest way to ensure that all messages before
                // this get processed but that we stop the flow of messages and thus close out the
                // connection to the client.
                if let MemcachedMessage::Quit = cmd {
                    self.closed = true;
                }

                self.rbuf.advance(bytes_read);

                Ok(Async::Ready(Some(cmd)))
            },
            Err(e) => Err(e),
            _ => {
                if socket_closed {
                    // If the socket is closed, let's also close up shop.
                    Ok(Async::Ready(None))
                } else {
                    Ok(Async::NotReady)
                }
            },
        }
    }
}

impl<T> Sink for MemcachedTransport<T>
where
    T: AsyncRead + AsyncWrite,
{
    type SinkError = Error;
    type SinkItem = BytesMut;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if self.wbuf.len() >= MAX_OUTSTANDING_WBUF {
            self.poll_complete()?;

            if self.wbuf.len() >= MAX_OUTSTANDING_WBUF {
                return Ok(AsyncSink::NotReady(item));
            }
        }

        self.wbuf.unsplit(item);
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        while !self.wbuf.is_empty() {
            let n = try_ready!(self.transport.poll_write(&self.wbuf));
            if n == 0 {
                return Err(ErrorKind::WriteZero.into());
            }
            let _ = self.wbuf.split_to(n);
        }

        try_ready!(self.transport.poll_flush());

        Ok(Async::Ready(()))
    }
}

impl<T> MemcachedMultipleMessages<T>
where
    T: AsyncRead,
{
    pub fn new(transport: T, msgs: EnqueuedRequests<MemcachedMessage>) -> Self {
        MemcachedMultipleMessages {
            transport: Some(transport),
            rbuf: BytesMut::new(),
            bytes_read: 0,
            msgs,
        }
    }

    fn fill_read_buf(&mut self) -> Poll<(), ProtocolError> {
        loop {
            self.rbuf.reserve(16384);

            let n = try_ready!(self.transport.as_mut().unwrap().read_buf(&mut self.rbuf));
            self.bytes_read += n;

            if n == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl<T> Future for MemcachedMultipleMessages<T>
where
    T: AsyncRead,
{
    type Error = ProtocolError;
    type Item = (T, usize);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let socket_closed = self.fill_read_buf()?.is_ready();

        loop {
            // We've collected all the messages, time to return.
            if self.msgs.is_empty() {
                return Ok(Async::Ready((self.transport.take().unwrap(), self.bytes_read)));
            }

            let result = read_message(&mut self.rbuf);
            match result {
                Ok(Async::Ready((bytes_read, msg))) => {
                    trace!("[protocol] got message from server! ({} bytes)", bytes_read);

                    let mut qmsg = self.msgs.remove(0);
                    qmsg.fulfill(msg)
                },
                Err(e) => return Err(e),
                _ => {
                    return if socket_closed {
                        // If the socket is closed, let's also close up shop after responding to
                        // the client with errors.
                        let err = MemcachedMessage::from_server_error(MC_BACKEND_CLOSED);
                        while let Some(mut qmsg) = self.msgs.pop() {
                            qmsg.fulfill(err.clone())
                        }

                        Err(ProtocolError::BackendClosedPrematurely)
                    } else {
                        Ok(Async::NotReady)
                    };
                },
            }
        }
    }
}

pub fn read_messages<T>(rx: T, msgs: EnqueuedRequests<MemcachedMessage>) -> MemcachedMultipleMessages<T>
where
    T: AsyncRead,
{
    MemcachedMultipleMessages::new(rx, msgs)
}

fn parse_message(input: &[u8]) -> Poll<(usize, MemcachedMessage), ProtocolError> {
    let start = input.len();
    match alt((cmd_quit, cmd_version))(input) {
        Ok((remaining, msg)) => {
            let count = start - remaining.len();
            Ok(Async::Ready((count, msg)))
        },
        Err(NomErr::Incomplete(_)) => Ok(Async::NotReady),
        Err(_) => Err(ProtocolError::InvalidProtocol),
    }
}

fn cmd_quit(input: &[u8]) -> IResult<&[u8], MemcachedMessage> {
    map(
        terminated(tag(b"quit"), crlf),
        |_| MemcachedMessage::Quit,
    )(input)
}

fn cmd_version(input: &[u8]) -> IResult<&[u8], MemcachedMessage> {
    map(
        terminated(tag(b"version"), crlf),
        |_| MemcachedMessage::Version,
    )(input)
}

fn read_message(rd: &mut BytesMut) -> Poll<(usize, MemcachedMessage), ProtocolError> {
    // Check to see if we got any inline commands.
    //
    // This is either shortform commands -- like QUIT -- or hard-coded responses like STORED or
    // NOT_FOUND.  Rather than using the full parser, we can quickly and easily match on those
    // hard-coded buffers.
    if let Some(msg_tuple) = read_inline_messages(rd) {
        return Ok(Async::Ready(msg_tuple));
    }

    read_message_internal(rd)
}

fn read_inline_messages(rd: &mut BytesMut) -> Option<(usize, MemcachedMessage)> {
    // Quitting is its own thing so we match that separately.
    if rd.starts_with(b"quit\r\n") {
        let _ = rd.split_to(6);
        return Some((6, MemcachedMessage::Quit));
    }

    // Try and match against the generic command statuses.
    let statuses = [&b"STORED\r\n"[..], &b"NOT_STORED\r\n"[..], &b"NOT_FOUND\r\n"[..], &b"EXISTS\r\n"[..], &b"DELETED\r\n"[..], &b"TOUCHED\r\n"[..]];
    for status in &statuses {
        if rd.starts_with(status) {
            let count = status.len();
            let mbuf = rd.split_to(count);
            return Some((count, MemcachedMessage::Status(mbuf)))
        }
    }

    None
}

fn read_message_internal(rd: &mut BytesMut) -> Poll<(usize, MemcachedMessage), ProtocolError> {
    // Match any retrieval commands first.
    if rd.starts_with(b"get") || rd.starts_with(b"gets") || rd.starts_with(b"get") || rd.starts_with(b"gets") {
        return read_retrieval(rd)
    }

    if rd.starts_with(b"VALUE") || rd.starts_with(b"END") {
        return read_values(rd)
    }

    if rd.starts_with(b"set") || rd.starts_with(b"add") || rd.starts_with(b"replace") || rd.starts_with(b"append") || rd.starts_with(b"prepend") || rd.starts_with(b"cas") {
        return read_storage(rd)
    }

    if rd.starts_with(b"touch") {
        return read_touch(rd)
    }

    if rd.starts_with(b"incr") || rd.starts_with(b"decr") {
        return read_counter(rd)
    }

    // If we couldn't extract a "known" message, see if we can parse a line.  Other commands are
    // all line-delimited, so if we have a line available, we can extract it as an unstructured
    // message.
    read_unstructured(rd)
}

// Finds the next occurence of CRLF.
fn read_line(rd: &BytesMut) -> Poll<usize, ProtocolError> {
    let result = rd
        .windows(2)
        .enumerate()
        .find(|&(_, bytes)| bytes == b"\r\n")
        .map(|(i, _)| i);

    match result {
        Some(v) => Ok(Async::Ready(v)),
        None => Ok(Async::NotReady),
    }
}

// finds the next occurence of CRLF starting from a specific offset.
fn read_line_from(rd: &BytesMut, offset: usize) -> Poll<usize, ProtocolError> {
    let result = rd[offset..]
        .windows(2)
        .enumerate()
        .find(|&(_, bytes)| bytes == b"\r\n")
        .map(|(i, _)| i + offset);

    match result {
        Some(v) => Ok(Async::Ready(v)),
        None => Ok(Async::NotReady),
    }
}

fn read_unstructured(rd: &mut BytesMut) -> Poll<(usize, MemcachedMessage), ProtocolError> {
    // All we care about is whether or not there's a CRLF-terminated line.  If there is, take it.
    let pos = try_ready!(read_line(rd));
    let buf = rd.split_to(pos + 2);
    Ok(Async::Ready((pos + 2, MemcachedMessage::Unstructured(buf))))
}

fn read_retrieval(rd: &mut BytesMut) -> Poll<(usize, MemcachedMessage), ProtocolError> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let pos = try_ready!(read_line(rd));
    let buf = rd.split_to(pos + 2);

    // Figure out which get command it is.
    let cmd = if buf.starts_with(b"get") {
        MemcachedCommand::Get
    } else if buf.starts_with(b"gets") {
        MemcachedCommand::Gets
    } else if buf.starts_with(b"gat") {
        MemcachedCommand::Gat
    } else if buf.starts_with(b"gats") {
        MemcachedCommand::Gats
    } else {
        unreachable!("read_retrieval called for command that is not get/gets/gat/gats");
    };

    let options = if cmd == MemcachedCommand::Gat || cmd == MemcachedCommand::Gats {
        // We have to pull out the first "key" as it's actually the expiration time to reset all of
        // the keys to in the given command.
        let expiration = &buf[..pos-2].split(|c| *c == 0x20)
            .nth(1)
            .ok_or(ProtocolError::InvalidProtocol)
            .and_then(|b| btoi::<u64>(b).map_err(|_| ProtocolError::InvalidProtocol))?;

        Some(RetrievalOptions { expiration: *expiration })
    } else {
        None
    };

    Ok(Async::Ready((pos + 2, MemcachedMessage::Retrieval(buf, cmd, options))))
}

fn read_values(rd: &mut BytesMut) -> Poll<(usize, MemcachedMessage), ProtocolError> {
    let mut global_pos = 0;

    loop {
        // First, make sure we have a valid line.  This will either be VALUE or END.  We read from
        // our last offset.
        let line_pos = try_ready!(read_line_from(rd, global_pos));
        if rd[global_pos..].starts_with(b"END\r\n") {
            // We're at the end of this response, so ship back the message.
            let buf = rd.split_to(line_pos + 2);
            return Ok(Async::Ready((line_pos + 2, MemcachedMessage::Data(buf))))
        }

        // We got a line, but it wasn't END, so it should be VALUE.
        if !&rd[global_pos..].starts_with(b"VALUE") {
            return Err(ProtocolError::InvalidProtocol)
        }

        // Pull out the size of the payload.
        let payload_size_raw = &rd[global_pos..line_pos].split(|c| *c == 0x20)
            .nth(3)
            .ok_or(ProtocolError::InvalidProtocol)?;
        let payload_size = btoi::<usize>(&payload_size_raw).map_err(|_| ProtocolError::InvalidProtocol)?;

        // See if the actual data is available in the buffer.
        if rd.len() < line_pos + 2 + payload_size + 2 {
            return Ok(Async::NotReady);
        }

        // We parse the entire VALUE line, and the data itself is all there.  Advance our global
        // position and loop again.
        global_pos = line_pos + 2 + payload_size + 2;
    }
}

fn read_storage(rd: &mut BytesMut) -> Poll<(usize, MemcachedMessage), ProtocolError> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let pos = try_ready!(read_line(rd));

    // Figure out which get command it is.
    let cmd = if rd.starts_with(b"set") {
        MemcachedCommand::Set
    } else if rd.starts_with(b"add") {
        MemcachedCommand::Add
    } else if rd.starts_with(b"replace") {
        MemcachedCommand::Replace
    } else if rd.starts_with(b"append") {
        MemcachedCommand::Append
    } else if rd.starts_with(b"prepend") {
        MemcachedCommand::Prepend
    } else if rd.starts_with(b"cas") {
        MemcachedCommand::CAS
    } else {
        unreachable!("read_storage called for command that is not set/add/replace/append/prepend/cas");
    };

    // Pull out all of the command arguments.
    let mut args = rd[..pos-2].split(|c| *c == 0x20);

    let flags = args.nth(2)
        .map(|b| {
            println!("{:?}", b);
            b
        })
        .ok_or(ProtocolError::InvalidProtocol)
        .and_then(|b| btoi::<u16>(b).map_err(|_| ProtocolError::InvalidProtocol))?;

    let expiration = args.nth(0)
        .map(|b| {
            println!("{:?}", b);
            b
        })
        .ok_or(ProtocolError::InvalidProtocol)
        .and_then(|b| btoi::<u64>(b).map_err(|_| ProtocolError::InvalidProtocol))?;

    let data_len = args.nth(0)
        .map(|b| {
            println!("{:?}", b);
            b
        })
        .ok_or(ProtocolError::InvalidProtocol)
        .and_then(|b| btoi::<u64>(b).map_err(|_| ProtocolError::InvalidProtocol))?;

    let cas = if cmd == MemcachedCommand::CAS {
        args.nth(0)
            .ok_or(ProtocolError::InvalidProtocol)
            .and_then(|b| btoi::<u64>(b).map_err(|_| ProtocolError::InvalidProtocol))?
    } else {
        0
    };

    let no_reply = args.nth(0)
        .and_then(|b| str::from_utf8(b).ok())
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    // Make sure we have the data available.
    let full_length = pos + 2 + data_len as usize + 2;
    if rd.len() < full_length {
        return Ok(Async::NotReady);
    }

    let buf = rd.split_to(full_length);
    let opts = StorageOptions {
        flags,
        expiration,
        no_reply,
        cas,
    };

    Ok(Async::Ready((full_length, MemcachedMessage::Storage(buf, cmd, Some(opts)))))
}

fn read_touch(rd: &mut BytesMut) -> Poll<(usize, MemcachedMessage), ProtocolError> {
    let pos = try_ready!(read_line(rd));
    let buf = rd.split_to(pos + 2);

    let mut parts = buf.split(|c| *c == 0x20);
    let expiration = parts.nth(2)
        .ok_or(ProtocolError::InvalidProtocol)
        .and_then(|b| btoi::<u64>(b).map_err(|_| ProtocolError::InvalidProtocol))?;
    let no_reply = parts.nth(0)
        .and_then(|b| str::from_utf8(b).ok())
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let opts = StorageOptions {
        expiration,
        no_reply,
        flags: 0,
        cas: 0,
    };

    Ok(Async::Ready((pos + 2, MemcachedMessage::Storage(buf, MemcachedCommand::Touch, Some(opts)))))
}

fn read_counter(rd: &mut BytesMut) -> Poll<(usize, MemcachedMessage), ProtocolError> {
    let pos = try_ready!(read_line(rd));
    let buf = rd.split_to(pos + 2);

    let mut parts = buf.split(|c| *c == 0x20);
    let _ = parts.nth(2)
        .ok_or(ProtocolError::InvalidProtocol)
        .and_then(|b| btoi::<u64>(b).map_err(|_| ProtocolError::InvalidProtocol))?;
    let no_reply = parts.nth(0)
        .and_then(|b| str::from_utf8(b).ok())
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let cmd = if buf.starts_with(b"incr") {
        MemcachedCommand::Increment
    } else if buf.starts_with(b"decr") {
        MemcachedCommand::Decrement
    } else {
        unreachable!("read_counter called for command that is not incr/decr");
    };

    let opts = StorageOptions {
        expiration: 0,
        no_reply,
        flags: 0,
        cas: 0,
    };

    Ok(Async::Ready((pos + 2, MemcachedMessage::Storage(buf, cmd, Some(opts)))))
}

pub fn write_raw_message<T>(tx: T, msg: MemcachedMessage) -> impl Future<Item = (T, usize), Error = ProtocolError>
where
    T: AsyncWrite,
{
    let buf = msg.into_resp();
    let buf_len = buf.len();
    write_all(tx, buf)
        .map(move |(tx, _buf)| (tx, buf_len))
        .map_err(|e| e.into())
}

pub fn write_messages<T>(
    transport: T, mut msgs: EnqueuedRequests<MemcachedMessage>,
) -> impl Future<Item = (T, EnqueuedRequests<MemcachedMessage>, usize), Error = ProtocolError>
where
    T: AsyncWrite,
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
    write_all(transport, buf)
        .map(move |(transport, _buf)| (transport, msgs, buf_len))
        .map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use spectral::prelude::*;
    use test::Bencher;

    static DATA_GET_SIMPLE: &[u8] = b"get foo\r\n";
    static DATA_SET_SIMPLE: &[u8] = b"set foo 0 0 5\r\nbooze\r\n";

    fn get_message_from_buf(buf: &[u8]) -> Poll<MemcachedMessage, ProtocolError> {
        let mut rd = BytesMut::with_capacity(buf.len());
        rd.put_slice(&buf[..]);
        read_message(&mut rd).map(|res| res.map(|(_, msg)| msg))
    }

    #[test]
    fn parse_get_simple() {
        let res = get_message_from_buf(&DATA_GET_SIMPLE);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(MemcachedMessage::Retrieval(_, cmd, _)) => assert_eq!(cmd, MemcachedCommand::Get),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_set_simple() {
        let res = get_message_from_buf(&DATA_SET_SIMPLE);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(MemcachedMessage::Storage(_, cmd, _)) => assert_eq!(cmd, MemcachedCommand::Set),
            _ => panic!("should have had message"),
        }
    }
}
