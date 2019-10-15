use std::io::{self, Write};
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

pub struct MockStream {
    readable: Vec<u8>,
    writable: Vec<u8>,
    read_closed: bool,
    write_closed: bool,
}

impl Unpin for MockStream {}

impl MockStream {
    pub fn new() -> Self {
        MockStream {
            readable: Vec::new(),
            writable: Vec::new(),
            read_closed: false,
            write_closed: false,
        }
    }

    pub fn append_readable(&mut self, buf: &[u8]) {
        self.readable.extend_from_slice(buf);
    }

    pub fn close_readable(&mut self) {
        self.read_closed = true;
    }

    pub fn append_writable(&mut self, buf: &[u8]) {
        self.writable.extend_from_slice(buf);
    }

    pub fn close_writable(&mut self) {
        self.write_closed = true;
    }
}

impl AsyncRead for MockStream {
    fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, mut buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match buf.write(self.readable.as_slice()) {
            Ok(n) => match n {
                // Nothing available?  That means we're waiting or we're done.
                0 => if self.read_closed { Poll::Ready(Ok(0)) } else { Poll::Pending },
                // We have data, so adjust our internal buffers to remove that.
                x => {
                    let mut new = self.readable.split_off(n);
                    mem::swap(&mut self.readable, &mut new);
                    Poll::Ready(Ok(x))
                },
            },
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for MockStream {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.writable.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
