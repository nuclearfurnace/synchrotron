use std::{
    pin::Pin,
    task::{Context, Poll},
};
use crate::common::{Sizable, Message};
use tokio::io::{AsyncRead, AsyncWrite, Error, ErrorKind};
use pin_project::pin_project;
use bytes::{BytesMut, BufMut, IntoBuf, buf::FromBuf};
use std::fmt::Display;
use std::io;
use futures::{ready, Stream, Sink};
use crate::protocol::errors::ProtocolError;

const MAX_OUTSTANDING_WBUF: usize = 8192;

#[derive(Clone, Debug, PartialEq)]
pub enum MockMessage {
    Empty,
    Data(BytesMut, BytesMut, Option<BytesMut>),
    Error(BytesMut),
    Quit,
}

impl Default for MockMessage {
    fn default() -> Self {
        MockMessage::Empty
    }
}

impl MockMessage {
    pub fn from_data<C, K, D>(cmd: C, key: K, data: Option<D>) -> Self
    where
        C: IntoBuf,
        K: IntoBuf,
        D: IntoBuf,
    {
        let cbuf = BytesMut::from_buf(cmd);
        let kbuf = BytesMut::from_buf(key);
        MockMessage::Data(cbuf, kbuf, data.map(BytesMut::from_buf))
    }

    pub fn from_error<E>(e: E) -> Self
    where
        E: Display,
    {
        let ebuf = e.to_string().into_bytes();
        let mut buf = BytesMut::new();
        buf.put(&b"ERROR "[..]);
        buf.put(ebuf);
        MockMessage::Error(buf)
    }

    pub fn get_command(&self) -> Option<&[u8]> {
        match self {
            MockMessage::Data(cmd, _, _) => Some(&cmd),
            _ => None,
        }
    }
}

impl Sizable for MockMessage {
    fn size(&self) -> usize {
        match self {
            MockMessage::Empty => 0,
            MockMessage::Data(cmd, key, data) => {
                // Command + space + key [ + space + data] + \n
                cmd.len() + key.len() + data.as_ref().map(|x| x.len() + 1).unwrap_or(0) + 2
            },
            // Error string + \n
            MockMessage::Error(buf) => buf.len() + 1,
            // "quit" + \n
            MockMessage::Quit => 5,
        }
    }
}

impl Message for MockMessage {
    fn key(&self) -> &[u8] {
        match self {
            MockMessage::Data(_, key, _) => &key,
            _ => panic!("message is not command!"),
        }
    }

    fn is_inline(&self) -> bool {
        match self {
            MockMessage::Quit => true,
            _ => false,
        }
    }

    fn needs_reply(&self) -> bool {
        true
    }

    fn into_buf(self) -> BytesMut {
        match self {
            MockMessage::Empty => BytesMut::new(),
            MockMessage::Data(mut cmd, key, data) => {
                cmd.put_u8(b' ');
                cmd.put(key);
                if let Some(data) = data {
                    cmd.put(b' ');
                    cmd.put(data);
                }
                cmd.put_u8(b'\n');
                cmd
            },
            MockMessage::Error(mut buf) => {
                buf.put_u8(b'\n');
                buf
            },
            MockMessage::Quit => BytesMut::from_buf(&b"OK\n"[..]),
        }
    }
}

#[pin_project]
pub struct MockTransport<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    #[pin]
    transport: T,
    rbuf: BytesMut,
    wbuf: BytesMut,
    closed: bool,
}

impl<T> MockTransport<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(transport: T) -> Self {
        MockTransport {
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

impl<T> Stream for MockTransport<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<MockMessage, ProtocolError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }

        let socket_closed = self.fill_read_buf(cx).is_ready();

        match read_message(&mut self.rbuf) {
            Poll::Ready(Ok((bytes_read, cmd))) => {
                debug!("got message from client! ({} bytes)", bytes_read);
                debug!("message: {:?}", cmd);

                if let MockMessage::Quit = cmd {
                    self.closed = true;
                }

                // If this command is invalid, kill the transport.  We also give the transport
                // owner an error message, which is inlined and so we can kill the transport while
                // still sending an error back to the client themselves.
                if let Some(cmd_key) = cmd.get_command() {
                    if !check_command_validity(cmd_key) {
                        self.closed = true;

                        let emsg = MockMessage::from_error("command not valid");
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

impl<T> Sink<MockMessage> for MockTransport<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // keep this as a no-op for now; in the future, maybe we hold the actual TCP stream halves
        // and do poll_write_ready or some shit
        let this = self.project();
        if this.wbuf.len() >= MAX_OUTSTANDING_WBUF {
            if let Err(e) = ready!(this.transport.poll_flush(cx)) {
                return Poll::Ready(Err(e))
            }

            if this.wbuf.len() >= MAX_OUTSTANDING_WBUF {
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: MockMessage) -> Result<(), Self::Error> {
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

fn read_message(rd: &mut BytesMut) -> Poll<Result<(usize, MockMessage), ProtocolError>> {
    let line = ready!(read_line(rd));

    let key_pos = rd.iter().enumerate()
        .find(|&(_, b)| *b == b' ')
        .map(|(i, _)| i)
        .ok_or(ProtocolError::InvalidProtocol)?;
    let cmd = rd.split_to(key_pos);

    let remaining_pos = rd.iter().enumerate()
        .find(|&(_, b)| *b == b' ')
        .map(|(i, _)| i);

    let mut remaining = None;
    let key = match remaining_pos {
        Some(pos) => {
            // There's more data, so pull out the key and fill the remaining with whatever.
            let key = rd.split_to(pos);

            let remaining_pos = line - key_pos - pos;
            if remaining_pos > 0 {
                remaining = Some(rd.split_to(remaining_pos));
            }

            key
        },
        None => {
            // There was no space, so make sure we have the key.
            let remaining_pos = line - key_pos;
            if remaining_pos == 0 {
                // There _has_ to be a key.
                return Poll::Ready(Err(ProtocolError::InvalidProtocol))
            }

            rd.split_to(remaining_pos)
        },
    };

    Poll::Ready(Ok((line + 2, MockMessage::Data(cmd, key, remaining))))
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

fn check_command_validity(cmd: &[u8]) -> bool {
    match cmd {
        b"get" | b"set" => true,
        _ => false,
    }
}
