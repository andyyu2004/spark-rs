use std::hash::{Hash, Hasher};

use super::*;

newtype_index!(StageId);

pub struct Stage {
    pub(super) rdd: RddRef,
    pub(super) stage_id: StageId,
    pub(super) parents: Vec<StageId>,
    pub(super) job_id: JobId,
    pub(super) kind: StageKind,
}

impl Stage {
    pub fn rdd(&self) -> RddRef {
        RddRef::clone(&self.rdd)
    }
}

pub enum StageKind {
    ShuffleMap,
    Result { partitions: Partitions },
}

impl Eq for Stage {
}

impl PartialEq for Stage {
    fn eq(&self, other: &Self) -> bool {
        self.stage_id == other.stage_id
    }
}

impl Hash for Stage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.stage_id.hash(state)
    }
}

pub struct ResultStage {
    pub(super) partitions: Partitions,
}
