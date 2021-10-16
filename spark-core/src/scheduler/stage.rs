use super::*;
use std::any::Any;

newtype_index!(StageId);

pub enum Stage {
    ShuffleMap(ShuffleMapStage),
    Result(ResultStage),
}

#[derive(Clone)]
pub struct ShuffleMapStage {
    pub(super) stage_id: StageId,
    pub(super) rdd: RddRef,
    pub(super) parents: Vec<StageId>,
}

pub struct ResultStage {
    pub(super) stage_id: StageId,
    pub(super) rdd: RddRef,
    pub(super) partitions: Partitions,
    pub(super) parents: Vec<StageId>,
    pub(super) job_id: JobId,
}
