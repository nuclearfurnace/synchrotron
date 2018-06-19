use btoi::btoi;
use bytes::{BufMut, BytesMut};
use futures::prelude::*;
use std::error::Error;
use std::io;
use std::mem;
use tokio::io::{write_all, AsyncRead, AsyncWrite};

const REDIS_COMMAND_ERROR: u8 = '-' as u8;
const REDIS_COMMAND_STATUS: u8 = '+' as u8;
const REDIS_COMMAND_INTEGER: u8 = ':' as u8;
const REDIS_COMMAND_DATA: u8 = '$' as u8;
const REDIS_COMMAND_BULK: u8 = '*' as u8;

const REDIS_NULL_BUF: [u8; 5] = [b'$', b'-', b'1', b'\r', b'\n'];
const REDIS_OK_BUF: [u8; 5] = [b'+', b'O', b'K', b'\r', b'\n'];
const REDIS_ERR_BUF: [u8; 5] = [b'-', b'E', b'R', b'R', b' '];
const REDIS_CRLF: [u8; 2] = [b'\r', b'\n'];

const REDIS_CLIENT_ERROR_PROTOCOL: &str = "protocol error; invalid payload";

pub type RedisOrderedMessages = Vec<(u64, RedisMessage)>;
pub type RedisUnorderedMessages = Vec<RedisMessage>;

/// An unbounded stream of Redis messages pulled from an asynchronous reader.
pub struct RedisMessageStream<R>
where
    R: AsyncRead,
{
    rx: R,
    rd: BytesMut,
}

/// A future that pulls multiple Redis messages from an asynchronous reader.
pub struct RedisMultipleMessages<R>
where
    R: AsyncRead,
{
    rx: Option<R>,
    rd: BytesMut,
    bytes_read: usize,
    msg_count: usize,
    msg_idx: usize,
    msgs: Option<RedisOrderedMessages>,
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
    Status(BytesMut, usize),
    Error(BytesMut, usize),
    Integer(BytesMut, i64),
    Data(BytesMut, usize),
    Bulk(BytesMut, Vec<RedisMessage>),
    Raw(BytesMut),
}

impl RedisMessage {
    pub fn from_inline(cmd: &str) -> RedisMessage {
        let s = cmd.clone();
        let buf = s.as_bytes();
        let mut rd = BytesMut::with_capacity(buf.len());
        rd.put_slice(&buf[..]);

        let bulk = RedisMessage::Data(rd.clone(), 0);
        let mut args = Vec::new();
        args.push(bulk);

        RedisMessage::Bulk(rd, args)
    }

    pub fn from_error(err: io::Error) -> RedisMessage {
        let error_str = err.description();

        // Full length is sigil (1) + header (ERR, 3) + space (1) + the message itself (N) + teh
        // trailing CRLF (2).
        let mut rd = BytesMut::with_capacity(error_str.len() + 7);
        rd.put_slice(&REDIS_ERR_BUF[..]);
        rd.put_slice(&error_str.as_bytes());
        rd.put_slice(&REDIS_CRLF[..]);

        RedisMessage::Error(rd, 5)
    }

    pub fn as_resp(self) -> BytesMut {
        match self {
            RedisMessage::Null => BytesMut::from(&REDIS_NULL_BUF[..]),
            RedisMessage::OK => BytesMut::from(&REDIS_OK_BUF[..]),
            RedisMessage::Status(buf, _) => buf,
            RedisMessage::Error(buf, _) => buf,
            RedisMessage::Integer(buf, _) => buf,
            RedisMessage::Data(buf, _) => buf,
            RedisMessage::Bulk(buf, _) => buf,
            RedisMessage::Raw(buf) => buf,
        }
    }
}

impl<R> RedisMessageStream<R>
where
    R: AsyncRead,
{
    pub fn new(rx: R) -> Self {
        RedisMessageStream {
            rx: rx,
            rd: BytesMut::new(),
        }
    }

    fn fill_read_buf(&mut self) -> Poll<(), io::Error> {
        loop {
            self.rd.reserve(1024);

            let n = try_ready!(self.rx.read_buf(&mut self.rd));
            if n == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl<R> Stream for RedisMessageStream<R>
where
    R: AsyncRead,
{
    type Item = RedisMessage;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let socket_closed = self.fill_read_buf()?.is_ready();

        match read_message(&mut self.rd) {
            Ok(Async::Ready((bytes_read, cmd))) => {
                trace!("[protocol] got message from client! ({} bytes)", bytes_read);
                Ok(Async::Ready(Some(cmd)))
            }
            Err(e) => Err(e),
            _ => match socket_closed {
                // If the socket is closed, let's also close up shop.
                true => Ok(Async::Ready(None)),
                false => Ok(Async::NotReady),
            },
        }
    }
}

impl<R> RedisMultipleMessages<R>
where
    R: AsyncRead,
{
    pub fn new(rx: R, msgs: RedisOrderedMessages) -> Self {
        let msgs_len = msgs.len();

        RedisMultipleMessages {
            rx: Some(rx),
            rd: BytesMut::new(),
            bytes_read: 0,
            msgs: Some(msgs),
            msg_count: msgs_len,
            msg_idx: 0,
        }
    }

    fn fill_read_buf(&mut self) -> Poll<(), io::Error> {
        loop {
            self.rd.reserve(1024);

            let n = try_ready!(self.rx.as_mut().unwrap().read_buf(&mut self.rd));
            self.bytes_read += n;

            if n == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl<R> Future for RedisMultipleMessages<R>
where
    R: AsyncRead,
{
    type Item = (R, usize, RedisOrderedMessages);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let socket_closed = self.fill_read_buf()?.is_ready();

        loop {
            // We've collected all the messages, time to return.
            if self.msg_count == self.msg_idx {
                return Ok(Async::Ready((
                    self.rx.take().unwrap(),
                    self.bytes_read,
                    self.msgs.take().unwrap(),
                )));
            }

            let result = read_message(&mut self.rd);
            match result {
                Ok(Async::Ready((bytes_read, msg))) => {
                    // We read a message successfully, store it and continue on.
                    trace!("[protocol] got message from server! ({} bytes)", bytes_read);

                    let mut msgs = self.msgs.as_mut().unwrap();
                    let (_, msg_slot) = &mut msgs[self.msg_idx];
                    let _ = mem::replace(msg_slot, msg);
                    self.msg_idx += 1;
                }
                Err(e) => return Err(e),
                _ => {
                    return match socket_closed {
                        // If the socket is closed, let's also close up shop.
                        true => Err(io::Error::new(
                            io::ErrorKind::Other,
                            "backend closed before sending response",
                        )),
                        false => Ok(Async::NotReady),
                    };
                }
            }
        }
    }
}

pub fn read_messages_stream<R>(rx: R) -> RedisMessageStream<R>
where
    R: AsyncRead,
{
    RedisMessageStream::new(rx)
}

pub fn read_messages<R>(rx: R, msgs: RedisOrderedMessages) -> RedisMultipleMessages<R>
where
    R: AsyncRead,
{
    RedisMultipleMessages::new(rx, msgs)
}

fn read_message(rd: &mut BytesMut) -> Poll<(usize, RedisMessage), io::Error> {
    // The only command we handle in non-RESP format is PING.  This is for simplicity
    // and compatibility with redis-benchmark, and also allowing health checks that don't need to
    // follow the RESP structure.
    if rd.starts_with(&b"ping\r\n"[..]) || rd.starts_with(&b"PING\r\n"[..]) {
        let _ = rd.split_to(6);
        return Ok(Async::Ready((6, RedisMessage::from_inline("ping\r\n"))));
    }

    // See if this is an OK response.
    if rd.starts_with(&b"+OK\r\n"[..]) {
        let _ = rd.split_to(5);
        return Ok(Async::Ready((5, RedisMessage::OK)));
    }

    // See if this is a NULL response.
    if rd.starts_with(&b"$-1\r\n"[..]) {
        let _ = rd.split_to(5);
        return Ok(Async::Ready((5, RedisMessage::Null)));
    }

    read_message_internal(rd)
}

fn read_message_internal(rd: &mut BytesMut) -> Poll<(usize, RedisMessage), io::Error> {
    // Try reading a single byte to see if we have a message.  Match it against known
    // message types, and process accordingly.
    let first = match rd.len() {
        0 => None,
        _ => Some(rd[0]),
    };

    match first {
        None => Ok(Async::NotReady),
        Some(t) => match &t {
            &REDIS_COMMAND_BULK => read_bulk(rd),
            &REDIS_COMMAND_DATA => read_data(rd),
            &REDIS_COMMAND_STATUS => read_status(rd),
            &REDIS_COMMAND_ERROR => read_error(rd),
            &REDIS_COMMAND_INTEGER => read_integer(rd),
            x => {
                debug!("got unknown type sigil: {:?}", x);
                Ok(Async::NotReady)
            }
        },
    }
}

fn read_line(rd: &BytesMut) -> Poll<usize, io::Error> {
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

fn read_bulk_count(rd: &mut BytesMut) -> Poll<(usize, usize), io::Error> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let pos = try_ready!(read_line(rd));

    // Try to extract the bulk count integer, leaving the rest.
    let buf = rd.split_to(pos + 2);
    match btoi::<usize>(&buf[1..pos]) {
        Ok(count) => Ok(Async::Ready((pos + 2, count))),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::Other,
            REDIS_CLIENT_ERROR_PROTOCOL,
        )),
    }
}

fn read_integer(rd: &mut BytesMut) -> Poll<(usize, RedisMessage), io::Error> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let crlf_pos = try_ready!(read_line(rd));

    // Try to extract the integer, leaving the rest.
    let value = btoi::<i64>(&rd[1..crlf_pos])
        .map_err(|_| io::Error::new(io::ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL))?;

    // Slice off the entire message.
    let total = crlf_pos + 2;
    let buf = rd.split_to(total);

    Ok(Async::Ready((total, RedisMessage::Integer(buf, value))))
}

fn read_status(rd: &mut BytesMut) -> Poll<(usize, RedisMessage), io::Error> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let crlf_pos = try_ready!(read_line(rd));

    // Slice off the entire message.
    let total = crlf_pos + 2;
    let buf = rd.split_to(total);

    Ok(Async::Ready((total, RedisMessage::Status(buf, 1))))
}

fn read_error(rd: &mut BytesMut) -> Poll<(usize, RedisMessage), io::Error> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let crlf_pos = try_ready!(read_line(rd));

    // Slice off the entire message.
    let total = crlf_pos + 2;
    let buf = rd.split_to(total);

    Ok(Async::Ready((total, RedisMessage::Error(buf, 1))))
}

fn read_data(rd: &mut BytesMut) -> Poll<(usize, RedisMessage), io::Error> {
    // Make sure there's at least a CRLF-terminated line in the buffer.
    let len_crlf_pos = try_ready!(read_line(rd));

    match &rd[1] {
        b'-' => {
            // See if this is just a null datum.
            let null_len = btoi::<i8>(&rd[1..len_crlf_pos])
                .map_err(|_| io::Error::new(io::ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL))?;

            match null_len {
                -1 => return Ok(Async::Ready((len_crlf_pos + 2, RedisMessage::Null))),
                _ => Err(io::Error::new(
                    io::ErrorKind::Other,
                    REDIS_CLIENT_ERROR_PROTOCOL,
                )),
            }
        }
        _ => {
            // Try to extract the data length integer, leaving the rest.
            let len = btoi::<usize>(&rd[1..len_crlf_pos]).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, REDIS_CLIENT_ERROR_PROTOCOL)
            })?;

            // See if the actual data is available in the buffer.
            if rd.len() < len_crlf_pos + 2 + len + 2 {
                return Ok(Async::NotReady);
            }

            // Slice off the entire message.
            let total = len_crlf_pos + 2 + len + 2;
            let buf = rd.split_to(total);

            Ok(Async::Ready((
                total,
                RedisMessage::Data(buf, len_crlf_pos + 2),
            )))
        }
    }
}

fn read_bulk(rd: &mut BytesMut) -> Poll<(usize, RedisMessage), io::Error> {
    let mut total = 0;
    let mut buf = rd.clone();

    // Get the number of items in the command.
    let (n, count) = try_ready!(read_bulk_count(&mut buf));
    if count < 1 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            REDIS_CLIENT_ERROR_PROTOCOL,
        ));
    }
    total = total + n;

    // Loop through, trying to read the number of arguments we were told exist in the message.
    // This can legitimately fail because, at this point, buf might not contain the full message.
    let mut args = Vec::new();
    for _ in 0..count {
        let (n, msg) = try_ready!(read_message_internal(&mut buf));
        total = total + n;

        args.push(msg);
    }

    // Slice off all the bytes we "read", updating the original in the process, and pass them along.
    let buf = rd.split_to(total);
    Ok(Async::Ready((total, RedisMessage::Bulk(buf, args))))
}

pub fn write_messages<T>(
    tx: T,
    mut msgs: RedisUnorderedMessages,
) -> impl Future<Item = (T, usize), Error = io::Error>
where
    T: AsyncWrite,
{
    let msgs_len = msgs.len();
    let buf = match msgs_len {
        1 => msgs.remove(0).as_resp(),
        _ => {
            let mut buf = BytesMut::new();
            for msg in msgs {
                let msg_buf = msg.as_resp();
                buf.extend_from_slice(&msg_buf[..]);
            }
            buf
        }
    };

    let buf_len = buf.len();
    write_all(tx, buf).map(move |(tx, _buf)| (tx, buf_len))
}

pub fn write_ordered_messages<T>(
    tx: T,
    mut msgs: RedisOrderedMessages,
) -> impl Future<Item = (T, RedisOrderedMessages, usize), Error = io::Error>
where
    T: AsyncWrite,
{
    let msgs_len = msgs.len();
    let buf = match msgs_len {
        1 => {
            let (_, msg) = &mut msgs[0];
            let msg2 = mem::replace(msg, RedisMessage::Null);
            msg2.as_resp()
        }
        _ => {
            let mut buf = BytesMut::new();
            for (_id, msg) in &mut msgs {
                let msg2 = mem::replace(msg, RedisMessage::Null);
                let msg_buf = msg2.as_resp();
                buf.extend_from_slice(&msg_buf[..]);
            }
            buf
        }
    };

    let buf_len = buf.len();
    write_all(tx, buf).map(move |(tx, _buf)| (tx, msgs, buf_len))
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

    fn get_message_from_buf(buf: &[u8]) -> Poll<RedisMessage, io::Error> {
        let mut rd = BytesMut::with_capacity(buf.len());
        rd.put_slice(&buf[..]);
        read_message(&mut rd).map(|res| res.map(|(_, msg)| msg))
    }

    fn check_data_matches(msg: RedisMessage, data: &[u8]) {
        match msg {
            RedisMessage::Data(ref buf, offset) => {
                let data_len = buf.len() - 2;
                assert_eq!(&buf[offset..data_len], data);
            }
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
            }
            _ => panic!("message is not status"),
        }
    }

    fn check_error_matches(msg: RedisMessage, data: &[u8]) {
        match msg {
            RedisMessage::Error(ref buf, offset) => {
                let data_len = buf.len() - 2;
                assert_eq!(&buf[offset..data_len], data);
            }
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
            }
            _ => panic!("message is not bulk"),
        }
    }

    #[test]
    fn parse_get_simple() {
        let res = get_message_from_buf(&DATA_GET_SIMPLE);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(msg) => check_bulk_matches(msg, vec![b"get", b"foobar"]),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_ok() {
        let res = get_message_from_buf(&DATA_OK);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(msg) => assert_eq!(msg, RedisMessage::OK),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_status() {
        let res = get_message_from_buf(&DATA_STATUS);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(msg) => check_status_matches(msg, b"LIMITED"),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_error() {
        let res = get_message_from_buf(&DATA_ERROR);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(msg) => check_error_matches(msg, b"ERR warning limit exceeded"),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_null() {
        let res = get_message_from_buf(&DATA_NULL);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(msg) => assert_eq!(msg, RedisMessage::Null),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_bulk_with_null() {
        let res = get_message_from_buf(&DATA_BULK_WITH_NULL);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(mut msg) => match msg {
                RedisMessage::Bulk(_, ref mut args) => {
                    assert_that(args).has_length(2);
                    let arg0 = args.remove(0);
                    let arg1 = args.remove(0);

                    check_data_matches(arg0, b"boo");
                    assert_eq!(arg1, RedisMessage::Null);
                }
                _ => panic!("message is not bulk"),
            },
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_integer() {
        let res = get_message_from_buf(&DATA_INTEGER_1337);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(msg) => check_integer_matches(msg, 1337),
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
    fn parse_ping_lower() {
        let res = get_message_from_buf(&DATA_PING_LOWER);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(msg) => check_bulk_matches(msg, vec![b"ping"]),
            _ => panic!("should have had message"),
        }
    }

    #[test]
    fn parse_ping_upper() {
        let res = get_message_from_buf(&DATA_PING_UPPER);
        assert_that(&res).is_ok().matches(|val| val.is_ready());

        match res.unwrap() {
            Async::Ready(msg) => check_bulk_matches(msg, vec![b"ping"]),
            _ => panic!("should have had message"),
        }
    }

    #[bench]
    fn bench_parse_get_simple(b: &mut Bencher) {
        b.iter(|| get_message_from_buf(&DATA_GET_SIMPLE));
    }

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
    fn bench_ping_lower(b: &mut Bencher) {
        b.iter(|| get_message_from_buf(&DATA_PING_LOWER));
    }

    #[bench]
    fn bench_ping_upper(b: &mut Bencher) {
        b.iter(|| get_message_from_buf(&DATA_PING_UPPER));
    }
}
