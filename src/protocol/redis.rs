use std::io::{Error, ErrorKind};
use bytes::{BufMut, BytesMut};
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};
use futures::prelude::*;
use atoi::atoi;
use itoa;

const REDIS_COMMAND_ERROR: u8 = '-' as u8;
const REDIS_COMMAND_STATUS: u8 = '+' as u8;
const REDIS_COMMAND_INTEGER: u8 = ':' as u8;
const REDIS_COMMAND_BULK: u8 = '$' as u8;
const REDIS_COMMAND_MULTI: u8 = '*' as u8;

const REDIS_NULL_BUF: [u8; 5] = [b'$', b'-', b'1', b'\r', b'\n'];
const REDIS_OK_BUF: [u8; 5] = [b'+', b'O', b'K', b'\r', b'\n'];

const REDIS_CLIENT_ERROR_PROTOCOL: &str = "protocol error; invalid payload";
const REDIS_CLIENT_ERROR_NON_RESP_PAYLOAD: &str = "payload must be in RESP format";

pub struct RedisCommandStream<R>
    where R: AsyncRead
{
    rx: R,
    rd: BytesMut,
}

pub struct RedisOneshotRead<R>
    where R: AsyncRead
{
    rx: R,
    rd: BytesMut,
}

pub enum RedisMessage {
    Null,
    OK,
    Integer(BytesMut, i64),
    Data(BytesMut),
    Bulk(BytesMut, Vec<RedisMessage>),
    Status(BytesMut),
}

impl RedisMessage {
    pub fn from_inline(cmd: &str) -> RedisMessage {
        let s = cmd.clone();
        let buf = s.as_bytes();
        let mut rd = BytesMut::with_capacity(buf.len());
        rd.put_slice(&buf[..]);

        let bulk = RedisMessage::Data(rd);
        let args = Vec::new();
        args.push(bulk);

        RedisMessage::Bulk(rd.clone(), args)
    }

    pub fn as_buf(&self) -> BytesMut {
        match self {
            RedisMessage::Null => BytesMut::from(&REDIS_NULL_BUF[..]),
            RedisMessage::OK => BytesMut::from(&REDIS_OK_BUF[..]),
            RedisMessage::Status(buf) => buf.clone(),
            RedisMessage::Integer(buf, _) => buf.clone(),
            RedisMessage::Data(buf) => buf.clone(),
            RedisMessage::Bulk(buf, parts) => buf.clone(),
        }
    }
}

impl<R> RedisCommandStream<R>
    where R: AsyncRead
{
    pub fn new(rx: R) -> Self {
        RedisCommandStream {
            rx: rx,
            rd: BytesMut::new(),
        }
    }

    fn fill_read_buf(&mut self) -> Poll<(), Error> {
        loop {
            self.rd.reserve(1024);

            let n = try_ready!(self.rx.read_buf(&mut self.rd));
            if n == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl<R> Stream for RedisCommandStream<R>
    where R: AsyncRead
{
    type Item = RedisMessage;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let socket_closed = self.fill_read_buf()?.is_ready();

        match read_message(&mut self.rd) {
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

impl<R> RedisOneshotRead<R>
    where R: AsyncRead
{
    pub fn new(rx: R) -> Self {
        RedisOneshotRead {
            rx: rx,
            rd: BytesMut::new(),
        }
    }

    fn fill_read_buf(&mut self) -> Poll<(), Error> {
        loop {
            self.rd.reserve(1024);

            let n = try_ready!(self.rx.read_buf(&mut self.rd));
            if n == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl<R> Future for RedisOneshotRead<R>
    where R: AsyncRead
{
    type Item = (R, RedisMessage);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let socket_closed = self.fill_read_buf()?.is_ready();

        // This is contorted but we can't try `try_ready!` over `read_message` because it has no
        // concept of the socket being dead (aka Ready(None)), we have to inject that here.
        match read_message(&mut self.rd) {
            Ok(Async::Ready(cmd)) => Ok(Async::Ready((self.rx, cmd))),
            Err(e) => Err(e),
            _ => match socket_closed {
                // If the socket is closed, let's also close up shop.
                true => Err(Error::new(ErrorKind::Other, "backend closed before sending response")),
                false => Ok(Async::NotReady),
            },
        }
    }
}

pub fn read_client_commands<R>(rx: R) -> RedisCommandStream<R>
    where R: AsyncRead
{
    RedisCommandStream::new(rx)
}

pub fn read_resp<R>(rx: R) -> RedisOneshotRead<R>
    where R: AsyncRead
{
    RedisOneshotRead::new(rx)
}

fn read_line(rd: &BytesMut) -> Poll<usize, Error> {
    let result = rd.windows(2).enumerate()
        .find(|&(_, bytes)| bytes == b"\r\n")
        .map(|(i, _)| i);

    match result {
        Some(v) => Ok(Async::Ready(v)),
        None => Ok(Async::NotReady),
    }
}

fn read_bulk_count(rd: &BytesMut) -> Poll<(usize, usize, BytesMut), Error> {
    let mut buf = rd.clone();

    // Make sure we have a line in our buffer.  We can't possibly proceed without one.
    let pos = try_ready!(read_line(&buf));

    // Make sure it's an array.
    if buf.first() != Some(&REDIS_COMMAND_MULTI) {
        return Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL))
    }

    // Pull out the line into `buf`, leaving the remainder of the buffer in `rest`.
    let rest = buf.split_off(pos + 2);
    let mut buf = buf.split_off(1); // Strip sigil off the front.
    let buf_len = buf.len() - 2;
    let _ = buf.split_off(buf_len); // Strip CRLF off the end.
    match atoi::<usize>(&buf) {
        Some(count) => Ok(Async::Ready((pos + 2, count, rest))),
        None => Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL)),
    }
}

fn read_bulk_arg(rd: &BytesMut) -> Poll<(usize, BytesMut, BytesMut), Error> {
    let mut buf = rd.clone();

    // Make sure we have a line in our buffer.  We can't possibly proceed without one.
    let len_crlf_pos = try_ready!(read_line(&buf));

    // Make sure it's a bulk value.
    if buf.first() != Some(&REDIS_COMMAND_BULK) {
        return Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL))
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
    let len = atoi::<usize>(&len_buf).ok_or(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL))?;

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

fn read_message(rd: &mut BytesMut) -> Poll<RedisMessage, Error> {
    // The only command we handle in non-RESP format is PING.  This is for simplicity
    // and compatibility with redis-benchmark.
    if rd.starts_with(&b"ping"[..]) || rd.starts_with(&b"PING"[..]) {
        return Ok(Async::Ready(RedisMessage::from_inline("ping")))
    }

    // Try reading a single byte to see if we have a message.  Match it against known
    // message types, and process accordingly.
    match rd.first() {
        None => Ok(Async::NotReady),
        Some(t) => match t {
            &REDIS_COMMAND_MULTI => read_bulk(rd),
            _ => Ok(Async::NotReady),
        }
    }
}

fn read_bulk(rd: &mut BytesMut) -> Poll<RedisMessage, Error> {
    let mut buf = rd.clone();

    // We need to keep track of the total bytes "read" so we can properly slice rd to
    // ignore what we've "read."
    let mut total = 0;

    // Make sure we have a line in our buffer.  We can't possibly proceed without one.
    let pos = try_ready!(read_line(&buf));

    // Get the number of items in the command.
    let (n, count, mut rest) = try_ready!(read_bulk_count(&rd));
    if count < 1 {
        return Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL))
    }
    total = total + n;

    // Loop through, trying to read the number of arguments we were told exist in the command.
    // This can legitimately fail because, at this point, rd might not contain the full command.
    let mut args = Vec::new();
    for _ in 0..count {
        let (n, buf, new_rest) = try_ready!(read_bulk_arg(&rest));
        rest = new_rest;
        total = total + n;

        args.push(RedisMessage::Data(buf));
    }

    // Slice off all the bytes we "read", updating the original in the process, and pass them along
    let buf = rd.split_to(total);
    Ok(Async::Ready(RedisMessage::Bulk(buf, args)))
}

pub fn write_resp<T>(tx: T, msg: RedisMessage) -> impl Future<Item=(T, usize), Error=Error>
    where T: AsyncWrite
{
    let buf = msg.as_buf();
    io::write_all(tx, buf).map(|(w, b)| (w, b.len()))
}

fn integer_to_buf(val: u64) -> BytesMut {
    // Forcefully unwrap, because this should never fail.
    let mut buf = [b'\0'; 20];
    let n = itoa::write(&mut buf[..], val).unwrap();

    let rd = BytesMut::with_capacity(buf.len());
    rd.put_slice(&buf[..n]);
    rd
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
        read_message(&mut rd)
    }

    #[test]
    fn parse_get_simple() {
        let res = get_command_from_buf(&DATA_GET_SIMPLE);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(RedisMessage::Bulk(_, ref args)) => {
                assert_that(args).has_length(2);
                assert_eq!(args[0].as_buf().as_ref(), b"get");
                assert_eq!(args[1].as_buf().as_ref(), b"foobar");
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
            Async::Ready(RedisMessage::Bulk(_, ref args)) => {
                assert_that(args).has_length(1);
                assert_eq!(args[0].as_buf().as_ref(), b"ping");
            },
            _ => panic!("should have had command"),
        }
    }

    #[test]
    fn parse_ping_upper() {
        let res = get_command_from_buf(DATA_PING_UPPER);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(RedisMessage::Bulk(_, ref args)) => {
                assert_that(args).has_length(1);
                assert_eq!(args[0].as_buf().as_ref(), b"ping");
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
