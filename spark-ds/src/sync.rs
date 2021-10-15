use std::ops::{Deref, DerefMut};

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
