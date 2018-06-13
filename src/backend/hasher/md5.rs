use crypto::digest::Digest;
use crypto::md5::Md5;

use super::KeyHasher;

pub struct MD5Hasher;

impl MD5Hasher {
    pub fn new() -> MD5Hasher {
        MD5Hasher {}
    }
}

impl KeyHasher for MD5Hasher {
    fn hash(&self, buf: &[u8]) -> u64 {
        let mut hasher = Md5::new();
        hasher.input(buf);

        let mut result = [0; 16];
        hasher.result(&mut result);

        (((result[3] as u32) << 24)
            + ((result[2] as u32) << 16)
            + ((result[1] as u32) << 8)
            + result[0] as u32) as u64
    }
}
