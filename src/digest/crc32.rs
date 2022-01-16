use std::marker::PhantomData;

use bytes::BufMut;
use crc32fast::Hasher;

use super::traits::{Algorithm, Digester};

pub struct Crc32Digester {
    hasher: Hasher,
}

#[derive(Clone)]
pub struct Crc32Algorithm {
    _marker: PhantomData<()>,
}

impl Crc32Algorithm {
    pub fn new() -> Crc32Algorithm {
        Crc32Algorithm { _marker: PhantomData }
    }
}

impl Algorithm for Crc32Algorithm {
    type Digester = Crc32Digester;

    fn digester(&self) -> Self::Digester {
        Crc32Digester { hasher: Hasher::new() }
    }

    fn digest_length(&self) -> usize {
        8
    }
}

impl Digester for Crc32Digester {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    fn digest(self, buf: &mut impl BufMut) {
        let checksum = self.hasher.finalize() as u64;
        buf.put_u64(checksum);
    }

    fn digest_length(&self) -> usize {
        8
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use pretty_assertions::assert_eq;

    use super::super::traits::{Algorithm, Digester};
    use super::{Crc32Algorithm, Crc32Digester};

    fn digest(digester: Crc32Digester) -> u64 {
        let mut vec = Vec::new();
        digester.digest(&mut vec);
        assert_eq!(8, vec.len());

        let mut buf = vec.as_slice();
        buf.get_u64()
    }

    #[test]
    fn test_crc32() {
        let algorithm = Crc32Algorithm::new();
        let digester = algorithm.digester();

        assert_eq!(8, algorithm.digest_length());
        assert_eq!(8, digester.digest_length());

        assert_eq!(0, digest(digester));

        let mut digester = algorithm.digester();
        digester.update(b"abcdefg");
        assert_eq!(0x312a6aa6, digest(digester));

        let mut digester = algorithm.digester();
        digester.update(b"abcdefg");
        digester.update(b"abcdefg");
        assert_eq!(0x7fd808af, digest(digester));
    }
}
