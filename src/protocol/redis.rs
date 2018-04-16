use std::io::{Error, ErrorKind};
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncWrite, AsyncRead};
use tokio::net::TcpStream;
use futures::prelude::*;
use atoi::atoi;

const REDIS_COMMAND_ERROR: u8 = '-' as u8;
const REDIS_COMMAND_STATUS: u8 = '+' as u8;
const REDIS_COMMAND_INTEGER: u8 = ':' as u8;
const REDIS_COMMAND_BULK: u8 = '$' as u8;
const REDIS_COMMAND_MULTI: u8 = '*' as u8;

const REDIS_CLIENT_ERROR_BAD_COMMAND_FORMAT: &str = "bad command format";
const REDIS_CLIENT_ERROR_NON_RESP_PAYLOAD: &str = "payload must be in RESP format";

pub struct RedisClientProtocol {
    socket: TcpStream,
    rd: BytesMut,
    wr: BytesMut,
}

#[derive(Debug)]
pub struct RedisCommandArg {
    buf: BytesMut,
}

#[derive(Debug)]
pub struct RedisCommand {
    buf: BytesMut,
    args: Vec<RedisCommandArg>,
}

impl RedisCommand {
    pub fn args(&self) -> &Vec<RedisCommandArg> {
        &self.args
    }
}

impl RedisCommandArg {
    fn from_buf(buf: BytesMut) -> RedisCommandArg {
        RedisCommandArg {
            buf: buf,
        }
    }

    pub fn buf(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl RedisCommand {
    fn from_args(buf: BytesMut, args: Vec<RedisCommandArg>) -> RedisCommand {
        RedisCommand {
            buf: buf,
            args: args,
        }
    }

    fn from_single(buf: &str) -> RedisCommand {
        let raw = buf.as_bytes();
        let mut buf = BytesMut::with_capacity(raw.len());
        buf.put_slice(raw);
        let arg = RedisCommandArg::from_buf(buf.clone());

        RedisCommand {
            buf: buf,
            args: vec![arg],
        }
    }
}

// shamelessly inspired from Tokio's chat server example
impl RedisClientProtocol {
    pub fn new(socket: TcpStream) -> Self {
        RedisClientProtocol {
            socket,
            rd: BytesMut::new(),
            wr: BytesMut::new(),
        }
    }

    fn buffer(&mut self, buf: &[u8]) {
        self.wr.reserve(buf.len());
        self.wr.put(buf);
    }

    fn poll_flush(&mut self) -> Poll<(), Error> {
        while !self.wr.is_empty() {
            let n = try_ready!(self.socket.poll_write(&self.wr));
            assert!(n > 0);
            let _ = self.wr.split_to(n);
        }

        Ok(Async::Ready(()))
    }

    fn fill_read_buf(&mut self) -> Poll<(), Error> {
        loop {
            self.rd.reserve(1024);

            let n = try_ready!(self.socket.read_buf(&mut self.rd));
            if n == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl Stream for RedisClientProtocol {
    type Item = RedisCommand;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let socket_closed = self.fill_read_buf()?.is_ready();

        // Check to see what we have for payload.  We have some special cases and constraints here:
        // - if the payload is "PING", we package that up by hand.
        // - if the payload start with "*" -- aka array/multi aka a command -- then we process it
        //
        // Everything else is invalid.  We expect to be talking to a client, which should only be
        // giving us commands to execute on its behalf, and we expect those commands (with the
        // small exception of PING) to be in RESP format.

        // This is contorted but we can't try `try_ready!` over `read_command` because it has no
        // concept of the socket being dead (aka Ready(None)), we have to inject that here.
        match read_command(&mut self.rd) {
            Ok(Async::Ready(cmd)) => Ok(Async::Ready(Some(cmd))),
            Err(e) => Err(e),
            _ => match socket_closed {
                // If the socket is closed, let's also close up shop.
                true => Ok(Async::Ready(None)),
                false => Ok(Async::NotReady),
            },
        }
    }
}

/// Searches `rd` for a CRLF-terminated line, and returns the position within `rd`
/// to the end of the line, CRLF included.
fn get_line(rd: &BytesMut) -> Poll<usize, Error> {
    let result = rd.windows(2).enumerate()
        .find(|&(_, bytes)| bytes == b"\r\n")
        .map(|(i, _)| i);

    match result {
        Some(v) => Ok(Async::Ready(v)),
        None => Ok(Async::NotReady),
    }
}

fn read_multi_count(rd: &BytesMut) -> Poll<(usize, usize, BytesMut), Error> {
    let mut buf = rd.clone();

    // Make sure we have a line in our buffer.  We can't possibly proceed without one.
    let pos = try_ready!(get_line(&buf));

    // Make sure it's an array.
    if buf.first() != Some(&REDIS_COMMAND_MULTI) {
        return Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_BAD_COMMAND_FORMAT))
    }

    // Pull out the line into `buf`, leaving the remainder of the buffer in `rest`.
    let rest = buf.split_off(pos + 2);
    let mut buf = buf.split_off(1); // Strip sigil off the front.
    let buf_len = buf.len() - 2;
    let _ = buf.split_off(buf_len); // Strip CRLF off the end.
    match atoi::<usize>(&buf) {
        Some(count) => Ok(Async::Ready((pos + 2, count, rest))),
        None => Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_BAD_COMMAND_FORMAT)),
    }
}

fn read_bulk_arg(rd: &BytesMut) -> Poll<(usize, BytesMut, BytesMut), Error> {
    let mut buf = rd.clone();

    // Make sure we have a line in our buffer.  We can't possibly proceed without one.
    let len_crlf_pos = try_ready!(get_line(&buf));

    // Make sure it's a bulk value.
    if buf.first() != Some(&REDIS_COMMAND_BULK) {
        return Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_BAD_COMMAND_FORMAT))
    }

    // This gets confusing, but, we're trying to find and chop off the "length of argument" line.
    // All the chopping does is split the buffers, so we chop in a way that if there was another
    // arg after this one, or another command, or whatever... the caller will get back the
    // remaining buffer to continue processing.  We also return the exact number of bytes we read
    // so the caller can update their master buffer.

    // First, try and chop off the "length of bulk string" line, and parse the value as an integer.
    let mut bulk_rest = buf.split_off(len_crlf_pos + 2); // Extract the length-of-arg line into `buf`.
    let mut len_buf = buf.split_off(1); // Strip sigil off the front.
    let len_buf_len = len_buf.len() - 2;
    let _ = len_buf.split_off(len_buf_len); // Strip CRLF off the end.
    let len = atoi::<usize>(&len_buf).ok_or(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_BAD_COMMAND_FORMAT))?;

    // We've parsed the length line, and `bulk_rest` is everything after it.  We need to make sure
    // that we even have enough data left to finish parsing this arg, or we need to signal to the
    // caller that we're not ready to complete parsing the overall command.
    //
    // The "2" here is the CRLF that comes at the end of a value.  Even though the data is
    // length-prefixed, it still has a CRLF trailer.
    if bulk_rest.len() < len + 2 {
        // Not enough data to proceed.
        return Ok(Async::NotReady);
    }

    // Split the line, so that `bulk_rest` has only the bulk data and `rest` is everything
    // remaining after this whole arg.  We also strip off the CRLF.
    let rest = bulk_rest.split_off(len + 2);
    let _ = bulk_rest.split_off(len);

    let total_read = len_crlf_pos + 2 + len + 2;
    Ok(Async::Ready((total_read, bulk_rest, rest)))
}

fn read_command(rd: &mut BytesMut) -> Poll<RedisCommand, Error> {
    // We need to keep track of the total bytes "read" so we can properly slice rd to
    // ignore what we've "read."
    let mut total = 0;

    // Check our special case of "ping".
    if rd.starts_with(&b"ping"[..]) || rd.starts_with(&b"PING"[..]) {
        return Ok(Async::Ready(RedisCommand::from_single("ping")))
    }

    // Get the number of items in the command.
    let (n, count, mut rest) = try_ready!(read_multi_count(&rd));
    if count < 1 {
        return Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_BAD_COMMAND_FORMAT))
    }
    total = total + n;

    // Loop through, trying to read the number of arguments we were told exist in the command.
    // This can legitimately fail because, at this point, rd might not contain the full command.
    let mut args = Vec::new();
    for _ in 0..count {
        let (n, buf, new_rest) = try_ready!(read_bulk_arg(&rest));
        rest = new_rest;
        total = total + n;

        args.push(RedisCommandArg::from_buf(buf));
    }

    // Slice off all the bytes we "read", updating the original in the process, and pass them along
    let buf = rd.split_to(total);
    Ok(Async::Ready(RedisCommand::from_args(buf, args)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;
    use spectral::prelude::*;

    type DataResult = Result<Async<RedisCommand>, Error>;

    static DATA_GET_SIMPLE: &[u8]                     = b"*2\r\n$3\r\nget\r\n$6\r\nfoobar\r\n";
    static DATA_SHORT_CIRCUIT_ZERO_DATA: &[u8]        = b"";
    static DATA_SHORT_CIRCUIT_NO_ARRAY_CRLF: &[u8]    = b"*2";
    static DATA_SHORT_CIRCUIT_NO_ARG_LEN_CRLF: &[u8]  = b"*2\r\n$3";
    static DATA_SHORT_CIRCUIT_PARTIAL_ARG: &[u8]      = b"*2\r\n$3\r\n";
    static DATA_SHORT_CIRCUIT_MISSING_ARG: &[u8]      = b"*2\r\n$3\r\nget\r\n";
    static DATA_SHORT_CIRCUIT_ARG_LEN_PAST_END: &[u8] = b"*2\r\n$3\r\nget\r\n$9foobar\r\n";
    static DATA_PING_LOWER: &[u8]                     = b"ping";
    static DATA_PING_UPPER: &[u8]                     = b"PING";

    fn get_command_from_buf(buf: &[u8]) -> DataResult {
        let mut rd = BytesMut::with_capacity(buf.len());
        rd.put_slice(&buf[..]);
        read_command(&mut rd)
    }

    #[test]
    fn parse_get_simple() {
        let res = get_command_from_buf(&DATA_GET_SIMPLE);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(cmd) => {
                assert_that(cmd.args()).has_length(2);
                assert_eq!(cmd.args()[0].buf(), b"get");
                assert_eq!(cmd.args()[1].buf(), b"foobar");
            },
            _ => panic!("should have had command"),
        }
    }

    #[test]
    fn parse_short_circuit_zero_data() {
        let res = get_command_from_buf(&DATA_SHORT_CIRCUIT_ZERO_DATA);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_short_circuit_no_array_crlf() {
        let res = get_command_from_buf(&DATA_SHORT_CIRCUIT_NO_ARRAY_CRLF);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_short_circuit_no_arg_len_crlf() {
        let res = get_command_from_buf(&DATA_SHORT_CIRCUIT_NO_ARG_LEN_CRLF);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

     #[test]
    fn parse_short_circuit_partial_arg() {
        let res = get_command_from_buf(&DATA_SHORT_CIRCUIT_PARTIAL_ARG);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_short_circuit_missing_arg() {
        let res = get_command_from_buf(&DATA_SHORT_CIRCUIT_MISSING_ARG);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_short_circuit_arg_len_past_end() {
        let res = get_command_from_buf(&DATA_SHORT_CIRCUIT_ARG_LEN_PAST_END);
        assert_that(&res).is_ok().matches(|val| val.is_not_ready());
    }

    #[test]
    fn parse_ping_lower() {
        let res = get_command_from_buf(DATA_PING_LOWER);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(cmd) => {
                assert_that(cmd.args()).has_length(1);
                assert_eq!(cmd.args()[0].buf(), b"ping");
            },
            _ => panic!("should have had command"),
        }
    }

    #[test]
    fn parse_ping_upper() {
        let res = get_command_from_buf(DATA_PING_UPPER);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(cmd) => {
                assert_that(cmd.args()).has_length(1);
                assert_eq!(cmd.args()[0].buf(), b"ping");
            },
            _ => panic!("should have had command"),
        }
    }

    #[bench]
    fn bench_parse_get_simple(b: &mut Bencher) {
        b.iter(|| get_command_from_buf(&DATA_GET_SIMPLE));
    }

    #[bench]
    fn bench_short_circuit_0_zero_data(b: &mut Bencher) {
        b.iter(|| get_command_from_buf(&DATA_SHORT_CIRCUIT_ZERO_DATA));
    }

    #[bench]
    fn bench_short_circuit_1_no_array_crlf(b: &mut Bencher) {
        b.iter(|| get_command_from_buf(&DATA_SHORT_CIRCUIT_NO_ARRAY_CRLF));
    }

    #[bench]
    fn bench_short_circuit_2_no_arg_len_crlf(b: &mut Bencher) {
        b.iter(|| get_command_from_buf(&DATA_SHORT_CIRCUIT_NO_ARG_LEN_CRLF));
    }

    #[bench]
    fn bench_short_circuit_3_partial_arg(b: &mut Bencher) {
        b.iter(|| get_command_from_buf(&DATA_SHORT_CIRCUIT_PARTIAL_ARG));
    }

    #[bench]
    fn bench_short_circuit_4_missing_arg(b: &mut Bencher) {
        b.iter(|| get_command_from_buf(&DATA_SHORT_CIRCUIT_MISSING_ARG));
    }

    #[bench]
    fn bench_short_circuit_5_arg_len_past_end(b: &mut Bencher) {
        b.iter(|| get_command_from_buf(&DATA_SHORT_CIRCUIT_ARG_LEN_PAST_END));
    }

    #[bench]
    fn bench_ping_lower(b: &mut Bencher) {
        b.iter(|| get_command_from_buf(&DATA_PING_LOWER));
    }

    #[bench]
    fn bench_ping_upper(b: &mut Bencher) {
        b.iter(|| get_command_from_buf(&DATA_PING_UPPER));
    }
}
