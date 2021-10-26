use super::*;
use crate::rdd::RddRef;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

crate::newtype_index!(ShuffleId);

pub type Dependencies = Arc<Vec<Arc<Dependency>>>;

#[derive(Debug, Serialize, Deserialize)]
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

    pub fn new_narrow(narrow: NarrowDependency) -> Arc<Self> {
        Arc::new(Self::Narrow(narrow))
    }

    pub fn new_one_to_one(rdd: RddRef) -> Arc<Self> {
        Self::new_narrow(NarrowDependency::OneToOne(OneToOneDependency { rdd }))
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct OneToOneDependency {
    rdd: RddRef,
}

#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShuffleDependency {
    pub(crate) rdd: RddRef,
    pub(crate) shuffle_id: ShuffleId,
}

impl ShuffleDependency {
    pub fn rdd(&self) -> RddRef {
        RddRef::clone(&self.rdd)
    }
}
