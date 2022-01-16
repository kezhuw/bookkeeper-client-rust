use bytes::BufMut;

pub trait Algorithm {
    type Digester: Digester;

    fn digester(&self) -> Self::Digester;

    fn digest_length(&self) -> usize;
}

pub trait Digester {
    fn update(&mut self, bytes: &[u8]);

    fn digest(self, buf: &mut impl BufMut);

    fn digest_length(&self) -> usize;
}
