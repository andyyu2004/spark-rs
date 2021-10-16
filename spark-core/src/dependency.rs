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
    fn rdd(&self) -> RddRef {
        match self {
            NarrowDependency::OneToOne(dep) => Arc::clone(&dep.rdd),
        }
    }
}

pub struct OneToOneDependency {
    rdd: RddRef,
}

pub struct ShuffleDependency {
    rdd: RddRef,
    shuffle_id: ShuffleId,
}

impl ShuffleDependency {
    fn rdd(&self) -> RddRef {
        Arc::clone(&self.rdd)
    }
}
