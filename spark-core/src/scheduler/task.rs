use super::*;

pub struct Task {
    pub(super) stage_id: StageId,
    pub(super) partition: PartitionIdx,
    pub(super) job_id: JobId,
    pub(super) kind: TaskKind,
}

pub enum TaskKind {
    Shuffle(),
    /// The bytes should be deserialized as (RddRef, PartitionMapperRef<T, U>)
    /// where`RddRef` implements `TypedRddRef<T>`
    Result(Arc<Vec<u8>>),
}

pub struct TaskSet {}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ResultTask<T: Datum, U: Datum, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + serde_traitobject::Serialize
        + serde_traitobject::Deserialize
        + Clone,
{
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pinned: bool,
    #[serde(with = "serde_traitobject")]
    pub rdd: Arc<dyn crate::rdd::TypedRdd<Output = T>>,
    pub func: Arc<F>,
    pub partition: usize,
    pub output_id: usize,
    _marker: std::marker::PhantomData<T>,
}
