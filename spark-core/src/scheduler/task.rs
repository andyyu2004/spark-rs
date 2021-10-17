use super::*;

pub struct Task<T, U> {
    pub(super) stage_id: StageId,
    pub(super) partition: PartitionIdx,
    pub(super) job_id: JobId,
    pub(super) kind: TaskKind<T, U>,
}

pub enum TaskKind<T, U> {
    Shuffle(),
    /// The `RddRef` should implement `TypedRddRef<T>`
    Result(RddRef, PartitionMapperRef<T, U>),
}

pub struct TaskSet {}
