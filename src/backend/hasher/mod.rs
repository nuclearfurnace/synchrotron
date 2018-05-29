mod md5;
pub use self::md5::MD5Hasher;

/// Basic hashing capabilities.
///
/// The hash output is a 64-bit integer so that it can be used with mapping hashed keys to specific
/// backend servers by index.
pub trait Hasher {
    fn hash(&self, buf: &[u8]) -> u64;
}

pub fn configure_hasher(hash_type: String) -> impl Hasher {
    match hash_type.as_str() {
        "md5" => MD5Hasher::new(),
        s => panic!("unknown hash type {}", s),
    }
}
