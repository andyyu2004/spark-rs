use super::*;
use crate::serialize::SerdeBox;
use downcast_rs::{impl_downcast, DowncastSync};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Any, Deserialize, Serialize};
use std::fmt::Debug;
use std::marker::PhantomData;

static_assertions::assert_impl_all!(Task: Serialize, Deserialize);
static_assertions::assert_impl_all!(TaskOutput: Serialize, Deserialize);

newtype_index!(TaskId);

pub type Task = SerdeBox<dyn ErasedTask>;

pub type TaskOutput = (TaskId, TaskResult<SerdeBox<dyn TaskOutputData>>);

pub type TaskResult<T> = Result<T, TaskError>;

#[derive(Debug, Serialize, Deserialize)]
pub enum TaskError {}

pub trait TaskOutputData: Any + Debug + Send + DowncastSync + 'static {}

impl_downcast!(TaskOutputData);

impl<T> TaskOutputData for T where T: Any + Debug + DowncastSync + Send + 'static
{
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TaskMeta {
    pub(super) task_id: TaskId,
    pub(super) stage_id: StageId,
    pub(super) partition: PartitionIdx,
    pub(super) job_id: JobId,
}

impl TaskMeta {
    pub fn new(task_id: TaskId, stage_id: StageId, partition: PartitionIdx, job_id: JobId) -> Self {
        Self { task_id, stage_id, partition, job_id }
    }
}

pub trait ErasedTask: Debug + Serialize + Deserialize + Send + Sync + 'static {
    fn id(&self) -> TaskId;
    fn exec(self: Box<Self>) -> TaskOutput;

    fn boxed(self) -> SerdeBox<Self>
    where
        Self: Sized,
    {
        SerdeBox::new(self)
    }
}

#[derive(Deserialize, Serialize)]
pub struct ResultTask<T: 'static, U, F> {
    meta: TaskMeta,
    rdd: TypedRddRef<T>,
    mapper: Arc<F>,
    _t: PhantomData<T>,
    _u: PhantomData<U>,
}

impl<T, U, F> Debug for ResultTask<T, U, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultTask")
            .field("meta", &self.meta)
            .field("rdd", &self.rdd)
            .finish_non_exhaustive()
    }
}

impl<T, U, F> ResultTask<T, U, F>
where
    T: CloneDatum,
    U: Datum,
    F: PartitionMapper<T, U>,
{
    pub fn new(meta: TaskMeta, rdd: TypedRddRef<T>, mapper: Arc<F>) -> Self {
        Self { meta, rdd, mapper, _t: PhantomData, _u: PhantomData }
    }
}

impl<T, U, F> ErasedTask for ResultTask<T, U, F>
where
    T: CloneDatum,
    U: Datum,
    F: PartitionMapper<T, U>,
{
    fn id(&self) -> TaskId {
        self.meta.task_id
    }

    fn exec(self: Box<Self>) -> TaskOutput {
        let cx = &mut TaskContext {};
        let iter = self.rdd.into_inner().compute(cx, self.meta.partition);
        let output = SerdeBox::new((self.mapper)(cx, iter));
        (self.meta.task_id, Ok(output))
    }
}

#[derive(Serialize, Deserialize)]
pub struct TaskSet<I> {
    pub(super) job_id: JobId,
    pub(super) stage_id: StageId,
    pub(super) tasks: I,
}

impl<I> Debug for TaskSet<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskSet")
            .field("job_id", &self.job_id)
            .field("stage_id", &self.stage_id)
            .finish_non_exhaustive()
    }
}

impl<I> TaskSet<I>
where
    I: ExactSizeIterator<Item = Task>,
{
    pub fn new(job_id: JobId, stage_id: StageId, tasks: impl IntoIterator<IntoIter = I>) -> Self {
        let tasks = tasks.into_iter();
        assert!(!tasks.is_empty());
        Self { job_id, stage_id, tasks }
    }
}
