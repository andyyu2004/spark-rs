use std::hash::{BuildHasher, Hasher};

/// A hasher intended to be used with [indexed_vec::Idx]
/// which can be used to hash a usize exactly one.
#[derive(Default)]
pub struct IdxHasher {
    hash: Option<u64>,
}

impl Hasher for IdxHasher {
    fn finish(&self) -> u64 {
        self.hash.expect("hasher finished before writing anything")
    }

    fn write(&mut self, _bytes: &[u8]) {
        panic!("only write_usize implemented")
    }

    fn write_usize(&mut self, i: usize) {
        *self.hash.as_mut().expect("hasher already written to") = i as u64;
    }
}

#[derive(Default, Clone)]
pub struct IdxHash;

impl BuildHasher for IdxHash {
    type Hasher = IdxHasher;

    fn build_hasher(&self) -> Self::Hasher {
        IdxHasher::default()
    }
}
