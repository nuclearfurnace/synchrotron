mod md5;
mod fnv64a;

pub use self::md5::MD5Hasher;
pub use self::fnv64a::Fnv64aHasher;

/// Basic hashing capabilities.
///
/// The hash output is a 64-bit integer so that it can be used with mapping hashed keys to specific
/// backend servers by index.
pub trait KeyHasher {
    fn hash(&self, buf: &[u8]) -> u64;
}

pub fn configure_hasher(hash_type: String) -> Box<KeyHasher + Send + Sync> {
    match hash_type.as_str() {
        "md5" => Box::new(MD5Hasher::new()),
        "fnv1a_64" => Box::new(Fnv64aHasher::new()),
        s => panic!("unknown hash type {}", s),
    }
}
