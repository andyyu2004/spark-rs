use super::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Task {
    pub(super) stage_id: StageId,
    pub(super) partition: PartitionIdx,
    pub(super) job_id: JobId,
    pub(super) kind: TaskKind,
}

impl Task {
    pub fn run(&self) {
        match &self.kind {
            TaskKind::Shuffle() => todo!(),
            TaskKind::Result(bytes) => {
                let (rdd, mapper) =
                    bincode::deserialize::<(RddRef, Arc<dyn serde_traitobject::Fn()>)>(bytes)
                        .expect("unexpectedly failed to deserialize task contents");
                todo!();
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
pub enum TaskKind {
    Shuffle(),
    /// The bytes should be deserialized as (RddRef, PartitionMapperRef<T, U>)
    /// where`RddRef` implements `TypedRddRef<T>`
    Result(Arc<Vec<u8>>),
}

pub struct TaskSet<I> {
    pub(super) job_id: JobId,
    pub(super) stage_id: StageId,
    pub(super) tasks: I,
}

impl<I> TaskSet<I>
where
    I: IntoIterator<Item = Task>,
{
    pub fn new(job_id: JobId, stage_id: StageId, tasks: I) -> Self {
        Self { job_id, stage_id, tasks }
    }
}

#[derive(Serialize, Deserialize)]
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
    pub rdd: Arc<dyn crate::rdd::TypedRdd<Element = T>>,
    pub func: Arc<F>,
    pub partition: usize,
    pub output_id: usize,
    _marker: std::marker::PhantomData<T>,
}
