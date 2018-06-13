use pruefung::fnv::fnv64::Fnv64a;
use std::hash::Hasher;

use super::KeyHasher;

pub struct Fnv64aHasher;

impl Fnv64aHasher {
    pub fn new() -> Fnv64aHasher {
        Fnv64aHasher {}
    }
}

impl KeyHasher for Fnv64aHasher {
    fn hash(&self, buf: &[u8]) -> u64 {
        let mut hasher = Fnv64a::default();
        hasher.write(buf);
        hasher.finish()
    }
}
