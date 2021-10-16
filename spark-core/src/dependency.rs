use crate::rdd::{Rdd, RddRef};
use indexed_vec::newtype_index;
use std::sync::Arc;

newtype_index!(ShuffleId);

pub enum Dependency<T> {
    Narrow(NarrowDependency<T>),
    Shuffle(ShuffleDependency<T>),
}

impl<T> Dependency<T> {
    pub fn rdd(&self) -> RddRef<T> {
        match self {
            Dependency::Narrow(narrow) => narrow.rdd(),
            Dependency::Shuffle(shuffle) => shuffle.rdd(),
        }
    }
}

pub enum NarrowDependency<T> {
    OneToOne(OneToOneDependency<T>),
}

impl<T> NarrowDependency<T> {
    fn rdd(&self) -> RddRef<T> {
        match self {
            NarrowDependency::OneToOne(dep) => Arc::clone(&dep.rdd),
        }
    }
}

pub struct OneToOneDependency<T> {
    rdd: RddRef<T>,
}

pub struct ShuffleDependency<T> {
    rdd: RddRef<T>,
    shuffle_id: ShuffleId,
}

impl<T> ShuffleDependency<T> {
    fn rdd(&self) -> RddRef<T> {
        todo!()
    }
}
