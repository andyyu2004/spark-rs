use crate::hash::IdxHash;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use indexed_vec::Idx;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

/// Wrapper around [DashMap] that is intented to emulate a [indexed_vec::IndexVec] that is concurrent
#[derive(Debug, Clone)]
pub struct ConcurrentIndexVec<I: Eq + Hash, T>(DashMap<I, T, IdxHash>);

pub type MapRef<'a, K, V> = Ref<'a, K, V, IdxHash>;

impl<I: Eq + Hash, T> Default for ConcurrentIndexVec<I, T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<I: Idx + Hash, T> ConcurrentIndexVec<I, T> {
    pub fn get(&self, idx: I) -> MapRef<'_, I, T> {
        self.get_opt(idx).unwrap()
    }

    pub fn get_opt(&self, idx: I) -> Option<MapRef<'_, I, T>> {
        self.0.get(&idx)
    }

    pub fn contains_key(&self, idx: I) -> bool {
        self.0.contains_key(&idx)
    }
}

impl<I: Idx + Hash, T> Deref for ConcurrentIndexVec<I, T> {
    type Target = DashMap<I, T, IdxHash>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I: Idx + Hash, T> DerefMut for ConcurrentIndexVec<I, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A type which only allows its inner value to be used in one thread.
/// It will panic if it is used on multiple threads.
#[derive(Debug)]
pub struct OneThread<T> {
    thread: std::thread::ThreadId,
    inner: T,
}

unsafe impl<T> Sync for OneThread<T> {
}

unsafe impl<T> Send for OneThread<T> {
}

impl<T> OneThread<T> {
    #[inline(always)]
    fn check(&self) {
        assert_eq!(std::thread::current().id(), self.thread);
    }

    #[inline(always)]
    pub fn new(inner: T) -> Self {
        OneThread { thread: std::thread::current().id(), inner }
    }

    #[inline(always)]
    pub fn into_inner(value: Self) -> T {
        value.check();
        value.inner
    }
}

impl<T> Deref for OneThread<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.check();
        &self.inner
    }
}

impl<T> DerefMut for OneThread<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.check();
        &mut self.inner
    }
}
