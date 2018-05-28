mod md5;
pub use self::md5::MD5Hasher;

/// Basic hashing capabilities.
///
/// The hash output is a 64-bit integer so that it can be used with mapping hashed keys to specific
/// backend servers by index.
pub trait Hasher {
    fn hash(&self, buf: &[u8]) -> u64;
}
