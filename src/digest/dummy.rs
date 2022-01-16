use std::marker::PhantomData;

use bytes::BufMut;

use super::traits::{Algorithm, Digester};

#[derive(Clone)]
pub struct DummyAlgorithm {
    marker: PhantomData<()>,
}

pub struct DummyDigester {
    marker: PhantomData<()>,
}

impl DummyAlgorithm {
    pub fn new() -> DummyAlgorithm {
        DummyAlgorithm { marker: PhantomData }
    }
}

impl Algorithm for DummyAlgorithm {
    type Digester = DummyDigester;

    fn digester<'a>(&self) -> Self::Digester {
        DummyDigester { marker: PhantomData }
    }

    fn digest_length(&self) -> usize {
        0
    }
}

impl Digester for DummyDigester {
    fn update(&mut self, _bytes: &[u8]) {}

    fn digest(self, _buf: &mut impl BufMut) {}

    fn digest_length(&self) -> usize {
        0
    }
}
