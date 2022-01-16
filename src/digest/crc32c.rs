use std::marker::PhantomData;

use bytes::BufMut;

use super::traits::{Algorithm, Digester};

#[derive(Clone)]
pub struct Crc32cAlgorithm {
    marker: PhantomData<()>,
}

impl Crc32cAlgorithm {
    pub fn new() -> Crc32cAlgorithm {
        Crc32cAlgorithm { marker: PhantomData }
    }
}

impl Algorithm for Crc32cAlgorithm {
    type Digester = Crc32cDigester;

    fn digester(&self) -> Self::Digester {
        Crc32cDigester { crc: 0 }
    }

    fn digest_length(&self) -> usize {
        4
    }
}

pub struct Crc32cDigester {
    crc: u32,
}

impl Digester for Crc32cDigester {
    fn update(&mut self, bytes: &[u8]) {
        self.crc = crc32c::crc32c_append(self.crc, bytes);
    }

    fn digest(self, buf: &mut impl BufMut) {
        buf.put_u32(self.crc);
    }

    fn digest_length(&self) -> usize {
        4
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use pretty_assertions::assert_eq;

    use super::super::traits::{Algorithm, Digester};
    use super::{Crc32cAlgorithm, Crc32cDigester};

    fn digest(digester: Crc32cDigester) -> u32 {
        let mut vec = Vec::new();
        digester.digest(&mut vec);
        assert_eq!(4, vec.len());

        let mut buf = vec.as_slice();
        buf.get_u32()
    }

    #[test]
    fn test_crc32c() {
        let algorithm = Crc32cAlgorithm::new();
        let digester = algorithm.digester();

        assert_eq!(4, algorithm.digest_length());
        assert_eq!(4, digester.digest_length());

        assert_eq!(0, digest(digester));

        let mut digester = algorithm.digester();
        digester.update(b"abcdefg");
        assert_eq!(0xE627F441, digest(digester));

        let mut digester = algorithm.digester();
        digester.update(b"abcdefg");
        digester.update(b"abcdefg");
        assert_eq!(0x482A0938, digest(digester));
    }
}
