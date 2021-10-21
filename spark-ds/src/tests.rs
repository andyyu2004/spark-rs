use crate::hash::IdxHash;
use crate::sync::ConcurrentIndexVec;
use indexed_vec::{newtype_index, Idx};
use std::hash::{BuildHasher, Hasher};

#[test]
fn test_idx_hash() {
    let mut hasher = IdxHash::default().build_hasher();
    hasher.write_u32(42);
    assert_eq!(hasher.finish(), 42);
}

#[test]
fn test_concurrent_vec_hash() {
    newtype_index!(SomeIdx);
    let vec = ConcurrentIndexVec::default();
    vec.insert(SomeIdx::new(5), 5);
    assert_eq!(*vec.get(SomeIdx::new(5)), 5);
}
