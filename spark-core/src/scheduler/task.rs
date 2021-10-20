use super::*;
use crate::serialize::SerdeBox;
use downcast_rs::{impl_downcast, DowncastSync};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Any, Deserialize, Serialize};
use std::fmt::Debug;
use std::marker::PhantomData;

static_assertions::assert_impl_all!(Task: Serialize, Deserialize);

pub type Task = SerdeBox<dyn ErasedTask>;

pub type TaskOutput = SerdeBox<dyn TaskOutputData>;

pub trait TaskOutputData: Any + Debug + Send + DowncastSync + 'static {}

impl_downcast!(TaskOutputData);

impl<T> TaskOutputData for T where T: Any + Debug + DowncastSync + Send + 'static
{
}

#[derive(Deserialize, Serialize)]
pub struct TaskMeta {
    pub(super) stage_id: StageId,
    pub(super) partition: PartitionIdx,
    pub(super) job_id: JobId,
}

pub trait ErasedTask: Serialize + Deserialize + Send + Sync + 'static {
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
    fn exec(self: Box<Self>) -> TaskOutput {
        let cx = &mut TaskContext {};
        let iter = self.rdd.into_inner().compute(cx, self.meta.partition);
        SerdeBox::new((self.mapper)(cx, iter))
    }
}

#[derive(Serialize, Deserialize)]
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
