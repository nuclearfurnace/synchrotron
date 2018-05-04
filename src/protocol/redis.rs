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
const REDIS_COMMAND_DATA: u8 = '$' as u8;
const REDIS_COMMAND_BULK: u8 = '*' as u8;

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
    rx: Option<R>,
    rd: BytesMut,
}

/// A RESP-based client/server message for Redis.
///
/// For all possible responses that contain dynamic data, we provide the full message as a BytesMut
/// buffer in the 1st field slot.  The 2nd field slot contains the "value" itself
///
/// For example, for a bulk message containing the string "foobar", the 1st field would represent the
/// entire message payload of "*6\r\nfoobar\r\n", but the 2nd field would represent "foobar".
pub enum RedisMessage {
    Null,
    OK,
    Integer(BytesMut, i64),
    Data(BytesMut, BytesMut),
    Bulk(BytesMut, Vec<RedisMessage>),
    Status(BytesMut, BytesMut),
}

impl RedisMessage {
    pub fn from_inline(cmd: &str) -> RedisMessage {
        let s = cmd.clone();
        let buf = s.as_bytes();
        let mut rd = BytesMut::with_capacity(buf.len());
        rd.put_slice(&buf[..]);

        let bulk = RedisMessage::Data(rd.clone(), rd.clone());
        let mut args = Vec::new();
        args.push(bulk);

        RedisMessage::Bulk(rd, args)
    }

    pub fn as_resp(&self) -> BytesMut {
        match self {
            RedisMessage::Null => BytesMut::from(&REDIS_NULL_BUF[..]),
            RedisMessage::OK => BytesMut::from(&REDIS_OK_BUF[..]),
            RedisMessage::Status(buf, _) => buf.clone(),
            RedisMessage::Integer(buf, _) => buf.clone(),
            RedisMessage::Data(buf, _) => buf.clone(),
            RedisMessage::Bulk(buf, _) => buf.clone(),
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

        match read_message(&self.rd) {
            Ok(Async::Ready((remaining, cmd))) => {
                self.rd = remaining;
                Ok(Async::Ready(Some(cmd)))
            },
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
            rx: Some(rx),
            rd: BytesMut::new(),
        }
    }

    fn fill_read_buf(&mut self) -> Poll<(), Error> {
        loop {
            self.rd.reserve(1024);

            let n = try_ready!(self.rx.as_mut().unwrap().read_buf(&mut self.rd));
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

        match read_message(&self.rd) {
            Ok(Async::Ready((_remaining, cmd))) => Ok(Async::Ready((self.rx.take().unwrap(), cmd))),
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

fn read_message(rd: &BytesMut) -> Poll<(BytesMut, RedisMessage), Error> {
    let mut buf = rd.clone();

    // The only command we handle in non-RESP format is PING.  This is for simplicity
    // and compatibility with redis-benchmark.
    if buf.starts_with(&b"ping"[..]) || buf.starts_with(&b"PING"[..]) {
        let _ = buf.split_to(4);
        return Ok(Async::Ready((buf, RedisMessage::from_inline("ping"))))
    }

    read_message_internal(&buf)
}

fn read_message_internal(rd: &BytesMut) -> Poll<(BytesMut, RedisMessage), Error> {
    // Try reading a single byte to see if we have a message.  Match it against known
    // message types, and process accordingly.
    let first_buf = rd.clone();
    match first_buf.first() {
        None => Ok(Async::NotReady),
        Some(t) => match t {
            &REDIS_COMMAND_BULK => read_bulk(rd),
            &REDIS_COMMAND_DATA => read_data(rd),
            x => {
                info!("got unknown type sigil: {:?}", x);
                Ok(Async::NotReady)
            },
        }
    }
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

fn read_bulk_count(rd: &BytesMut) -> Poll<(BytesMut, usize), Error> {
    let mut buf = rd.clone();

    // Make sure there's at least a CRLF-terminated line in the buffer.
    let pos = try_ready!(read_line(&buf));

    // Try to extract the bulk count integer, leaving the rest.
    let rest = buf.split_off(pos + 2);
    let buf_len = buf.len() - 2;
    match atoi::<usize>(&buf[1..buf_len]) {
        Some(count) => Ok(Async::Ready((rest, count))),
        None => Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL)),
    }
}

fn read_data(rd: &BytesMut) -> Poll<(BytesMut, RedisMessage), Error> {
    let mut full = rd.clone();
    let mut buf = rd.clone();

    // Make sure there's at least a CRLF-terminated line in the buffer.
    let len_crlf_pos = try_ready!(read_line(&buf));

    // Try to extract the data length integer, leaving the rest.
    let mut data = buf.split_off(len_crlf_pos + 2);
    let buf_len = buf.len() - 2;
    let len = atoi::<usize>(&buf[1..buf_len]).ok_or(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL))?;

    // See if the actual data is available in the buffer.
    if data.len() < len + 2 {
        return Ok(Async::NotReady);
    }

    // Now chop off just the data value.
    let _ = data.split_off(len);

    // Slice off the entire message.
    let total_read = len_crlf_pos + 2 + len + 2;
    let remaining = full.split_off(total_read);

    Ok(Async::Ready((remaining, RedisMessage::Data(full, data))))
}

fn read_bulk(rd: &BytesMut) -> Poll<(BytesMut, RedisMessage), Error> {
    let mut buf = rd.clone();

    // Make sure we have a line in our buffer.  We can't possibly proceed without one.
    let _pos = try_ready!(read_line(&buf));

    // Get the number of items in the command.
    let (mut rest, count) = try_ready!(read_bulk_count(&buf));
    if count < 1 {
        return Err(Error::new(ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL))
    }

    // Loop through, trying to read the number of arguments we were told exist in the message.
    // This can legitimately fail because, at this point, buf might not contain the full message.
    let mut args = Vec::new();
    for _ in 0..count {
        let (new_rest, msg) = try_ready!(read_message_internal(&rest));
        rest = new_rest;

        args.push(msg);
    }

    // Slice off all the bytes we "read", updating the original in the process, and pass them along.
    let total = buf.len() - rest.len();
    let remaining = buf.split_off(total);
    Ok(Async::Ready((rest, RedisMessage::Bulk(buf, args))))
}

pub fn write_resp<T>(tx: T, msg: RedisMessage) -> impl Future<Item=(T, usize), Error=Error>
    where T: AsyncWrite
{
    let buf = msg.as_resp();
    io::write_all(tx, buf).map(|(w, b)| (w, b.len()))
}

fn integer_to_buf(val: u64) -> BytesMut {
    // Forcefully unwrap, because this should never fail.
    let mut buf = [b'\0'; 20];
    let n = itoa::write(&mut buf[..], val).unwrap();

    let mut rd = BytesMut::with_capacity(buf.len());
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
