// Copyright (c) 2018 Nuclear Furnace
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use crypto::{digest::Digest, md5::Md5};

use super::KeyHasher;

pub struct MD5Hasher;

impl MD5Hasher {
    pub fn new() -> MD5Hasher { MD5Hasher {} }
}

impl KeyHasher for MD5Hasher {
    fn hash(&self, buf: &[u8]) -> u64 {
        let mut hasher = Md5::new();
        hasher.input(buf);

        let mut result = [0; 16];
        hasher.result(&mut result);

        (((result[3] as u32) << 24) + ((result[2] as u32) << 16) + ((result[1] as u32) << 8) + result[0] as u32) as u64
    }
}
