use bytes::BufMut;
use hmac::{Hmac, Mac};
use sha1::{Digest, Sha1};

use super::traits::{Algorithm, Digester};

#[derive(Clone)]
pub struct HmacAlgorithm {
    key: [u8; 20],
}

impl HmacAlgorithm {
    pub fn new(password: &[u8]) -> HmacAlgorithm {
        HmacAlgorithm { key: sha1_digest(b"mac", password) }
    }
}

impl Algorithm for HmacAlgorithm {
    type Digester = HmacDigester;

    fn digester(&self) -> Self::Digester {
        HmacDigester::new_from_slice(&self.key)
    }

    fn digest_length(&self) -> usize {
        20
    }
}

pub struct HmacDigester {
    hmac: Hmac<Sha1>,
}

impl HmacDigester {
    fn new_from_slice(key: &[u8]) -> HmacDigester {
        HmacDigester { hmac: Hmac::new_from_slice(key).unwrap() }
    }
}

impl Digester for HmacDigester {
    fn update(&mut self, bytes: &[u8]) {
        self.hmac.update(bytes);
    }

    fn digest(self, buf: &mut impl BufMut) {
        let checksum = self.hmac.finalize();
        buf.put_slice(&checksum.into_bytes());
    }

    fn digest_length(&self) -> usize {
        20
    }
}

pub fn sha1_digest(prefix: &[u8], bytes: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(prefix);
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_hmac() {
        let algorithm = HmacAlgorithm::new(b"1234567");
        let mut digester = algorithm.digester();

        digester.update(b"abcdefg");
        let mut buf = Vec::new();
        digester.digest(&mut buf);
        assert_eq!(20, buf.len());

        let hex_string = hex::encode(buf.as_slice());
        assert_eq!(hex_string, "c7c223ddde5f58eed02c1d0c8b82919e76cb4d01");
    }

    #[test]
    fn test_sha1() {
        assert_eq!(hex::encode(sha1_digest(b"mac", b"1234567")), "d355c66c4b1305540f1b87d82ff599b3b1e586aa");
        assert_eq!(hex::encode(sha1_digest(b"mac", b"abcdefg")), "1e97a5b44d84b07cdc8fa82afcf0c95ba2f62057");
    }
}
