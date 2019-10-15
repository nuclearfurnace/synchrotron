#[inline]
pub fn to_upper(s: &mut [u8]) {
    let count = s.len();
    let mut offset = 0;

    while offset < count {
        s[offset] &= 0b1101_1111;
        offset += 1;
    }
}

pub fn to_upper_owned(s: &[u8]) -> Vec<u8> {
    let count = s.len();
    let mut offset = 0;
    let mut buf = Vec::with_capacity(count);

    // Invariant: We're going to overwrite each byte, and the capacity matches our
    // iterator count, so we're good.
    unsafe { buf.set_len(count); }

    while offset < count {
        buf[offset] = s[offset] & 0b1101_1111;
        offset += 1;
    }

    buf
}
