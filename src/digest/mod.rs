use bytes::BufMut;
use lazy_static::lazy_static;

mod crc32;
mod crc32c;
mod dummy;
mod hmac;
pub mod traits;

use crc32::{Crc32Algorithm, Crc32Digester};
use dummy::{DummyAlgorithm, DummyDigester};

use self::crc32c::{Crc32cAlgorithm, Crc32cDigester};
use self::hmac::{HmacAlgorithm, HmacDigester};
use crate::client::DigestType;

enum DigesterRepr {
    Hmac(HmacDigester),
    Crc32(Crc32Digester),
    Crc32c(Crc32cDigester),
    Dummy(DummyDigester),
}

pub struct Digester {
    repr: DigesterRepr,
}

#[derive(Clone)]
enum AlgorithmRepr {
    Hmac(HmacAlgorithm),
    Crc32(Crc32Algorithm),
    Crc32c(Crc32cAlgorithm),
    Dummy(DummyAlgorithm),
}

#[derive(Clone)]
pub struct Algorithm {
    repr: AlgorithmRepr,
}

lazy_static! {
    static ref EMPTY_PASSWORD_MASTER_KEY: [u8; 20] = hmac::sha1_digest(b"ledger", &Vec::default());
}

pub fn generate_master_key(password: &[u8]) -> [u8; 20] {
    if password.is_empty() {
        return *EMPTY_PASSWORD_MASTER_KEY;
    }
    hmac::sha1_digest(b"ledger", password)
}

impl Algorithm {
    pub fn new(digest_type: DigestType, password: &[u8]) -> Algorithm {
        let repr = match digest_type {
            DigestType::CRC32 => AlgorithmRepr::Crc32(Crc32Algorithm::new()),
            DigestType::CRC32C => AlgorithmRepr::Crc32c(Crc32cAlgorithm::new()),
            DigestType::MAC => AlgorithmRepr::Hmac(HmacAlgorithm::new(password)),
            DigestType::DUMMY => AlgorithmRepr::Dummy(DummyAlgorithm::new()),
        };
        Algorithm { repr }
    }
}

impl traits::Algorithm for Algorithm {
    type Digester = Digester;

    fn digester(&self) -> Self::Digester {
        let repr = match &self.repr {
            AlgorithmRepr::Hmac(algorithm) => DigesterRepr::Hmac(algorithm.digester()),
            AlgorithmRepr::Crc32(algorithm) => DigesterRepr::Crc32(algorithm.digester()),
            AlgorithmRepr::Crc32c(algorithm) => DigesterRepr::Crc32c(algorithm.digester()),
            AlgorithmRepr::Dummy(algorithm) => DigesterRepr::Dummy(algorithm.digester()),
        };
        Digester { repr }
    }

    fn digest_length(&self) -> usize {
        match &self.repr {
            AlgorithmRepr::Hmac(algorithm) => algorithm.digest_length(),
            AlgorithmRepr::Crc32(algorithm) => algorithm.digest_length(),
            AlgorithmRepr::Crc32c(algorithm) => algorithm.digest_length(),
            AlgorithmRepr::Dummy(algorithm) => algorithm.digest_length(),
        }
    }
}

impl traits::Digester for Digester {
    fn update(&mut self, bytes: &[u8]) {
        match &mut self.repr {
            DigesterRepr::Hmac(digester) => digester.update(bytes),
            DigesterRepr::Crc32(digester) => digester.update(bytes),
            DigesterRepr::Dummy(digester) => digester.update(bytes),
            DigesterRepr::Crc32c(digester) => digester.update(bytes),
        }
    }

    fn digest(self, buf: &mut impl BufMut) {
        match self.repr {
            DigesterRepr::Hmac(digester) => digester.digest(buf),
            DigesterRepr::Crc32(digester) => digester.digest(buf),
            DigesterRepr::Dummy(digester) => digester.digest(buf),
            DigesterRepr::Crc32c(digester) => digester.digest(buf),
        }
    }

    fn digest_length(&self) -> usize {
        match &self.repr {
            DigesterRepr::Hmac(digester) => digester.digest_length(),
            DigesterRepr::Crc32(digester) => digester.digest_length(),
            DigesterRepr::Dummy(digester) => digester.digest_length(),
            DigesterRepr::Crc32c(digester) => digester.digest_length(),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use pretty_assertions::assert_eq;

    use super::traits::{Algorithm as _, Digester as _};
    use super::*;

    #[test]
    fn test_master_key() {
        assert_eq!(hex::encode(generate_master_key(b"")), "850bf1071c5e3d8c24235676f8816ae0cbe2f14f");
        assert_eq!(hex::encode(generate_master_key(b"1234567")), "53c0501bd64bdd5e9efae5d851bf6eafe755ff82");
        assert_eq!(hex::encode(generate_master_key(b"abcdefg")), "f870c78a9189c08645593a360710da548a3630dd");
    }

    #[test]
    fn test_dummy() {
        let algorithm = Algorithm::new(DigestType::DUMMY, b"1234567");

        assert_eq!(0, algorithm.digest_length());

        let mut digester = algorithm.digester();
        assert_eq!(0, digester.digest_length());

        let mut vec = Vec::new();

        digester.update(b"abcdefg");
        digester.digest(&mut vec);

        assert_eq!(0, vec.len());
    }

    #[test]
    fn test_crc32() {
        let algorithm = Algorithm::new(DigestType::CRC32, b"1234567");

        assert_eq!(8, algorithm.digest_length());

        let mut digester = algorithm.digester();
        assert_eq!(8, digester.digest_length());

        let mut vec = Vec::new();
        digester.update(b"abcdefg");
        digester.digest(&mut vec);

        assert_eq!(8, vec.len());

        let mut buf = vec.as_slice();
        assert_eq!(0x312a6aa6, buf.get_u64());
    }

    #[test]
    fn test_crc32c() {
        let algorithm = Algorithm::new(DigestType::CRC32C, b"1234567");

        assert_eq!(4, algorithm.digest_length());

        let mut digester = algorithm.digester();
        assert_eq!(4, digester.digest_length());

        let mut vec = Vec::new();
        digester.update(b"abcdefg");
        digester.digest(&mut vec);

        assert_eq!(4, vec.len());

        let mut buf = vec.as_slice();
        assert_eq!(0xE627F441, buf.get_u32());
    }

    #[test]
    fn test_hmac() {
        let algorithm = Algorithm::new(DigestType::MAC, b"1234567");

        assert_eq!(20, algorithm.digest_length());

        let mut digester = algorithm.digester();
        assert_eq!(20, digester.digest_length());

        let mut vec = Vec::new();
        digester.update(b"abcdefg");
        digester.digest(&mut vec);

        assert_eq!(20, vec.len());

        assert_eq!("c7c223ddde5f58eed02c1d0c8b82919e76cb4d01", hex::encode(vec.as_slice()));
    }
}
