use crate::rdd::RddRef;
use indexed_vec::newtype_index;
use std::sync::Arc;

newtype_index!(ShuffleId);

pub type Dependencies = Arc<Vec<Dependency>>;

pub enum Dependency {
    Narrow(NarrowDependency),
    Shuffle(ShuffleDependency),
}

impl Dependency {
    pub fn rdd(&self) -> RddRef {
        match self {
            Dependency::Narrow(narrow) => narrow.rdd(),
            Dependency::Shuffle(shuffle) => shuffle.rdd(),
        }
    }
}

pub enum NarrowDependency {
    OneToOne(OneToOneDependency),
}

impl NarrowDependency {
    pub fn rdd(&self) -> RddRef {
        match self {
            NarrowDependency::OneToOne(dep) => dep.rdd.clone(),
        }
    }
}

pub struct OneToOneDependency {
    rdd: RddRef,
}

#[derive(Hash, Clone, PartialEq, Eq)]
pub struct ShuffleDependency {
    pub(crate) rdd: RddRef,
    pub(crate) shuffle_id: ShuffleId,
}

impl ShuffleDependency {
    pub fn rdd(&self) -> RddRef {
        RddRef::clone(&self.rdd)
    }
}
