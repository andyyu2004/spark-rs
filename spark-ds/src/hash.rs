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
        panic!("only `write_u32` implemented")
    }

    fn write_u32(&mut self, i: u32) {
        assert!(self.hash.is_none(), "hasher already written to");
        self.hash = Some(i as u64);
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
