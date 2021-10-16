mod event;
mod job;

use self::event::*;
use self::job::JobHandle;
use crate::rdd::{TypedRdd, TypedRddRef};
use crate::*;
use indexed_vec::{newtype_index, Idx};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Handler function that is called when a partition has been computed.
/// It is passed the partition index and the output computation.
pub trait HandlerFn<T> = FnOnce(PartitionIndex, T);

pub trait PartitionMapper<T, U> = FnOnce(TaskContext, SparkIterator<T>) -> U + Send + 'static;
pub type PartitionMapperRef<T, U> = Box<dyn PartitionMapper<T, U>>;

pub struct TaskScheduler {
    backend: Box<dyn TaskSchedulerBackend>,
}

impl TaskScheduler {
    pub fn default_parallelism(&self) -> usize {
        num_cpus::get()
    }
}

pub trait TaskSchedulerBackend: Send + Sync {
    // Local(LocalScheduler),
    // Distributed(DistributedScheduler),
}

pub struct LocalScheduler {
    cores: usize,
}

impl LocalScheduler {
    pub fn new(cores: usize) -> Self {
        Self { cores }
    }

    pub fn run<T>(&self, rdd: impl TypedRdd<Output = T>) -> SparkResult<Vec<T>> {
        todo!()
    }
}

pub struct DistributedScheduler;

newtype_index!(JobId);
newtype_index!(StageId);

static_assertions::assert_impl_all!(Arc<DagScheduler>: Send, Sync);

pub struct DagScheduler {
    job_idx: AtomicUsize,
    stage_idx: AtomicUsize,
}

impl DagScheduler {
    pub fn new() -> Self {
        Self { job_idx: Default::default(), stage_idx: Default::default() }
    }

    pub(crate) async fn run<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        f: impl PartitionMapper<T, U>,
        handler: impl HandlerFn<U>,
    ) -> SparkResult<()>
    where
        T: Datum,
        U: Send + 'static,
    {
        assert!(!partitions.is_empty(), "handle empty partitions");
        let handle = self.submit(rdd, partitions, Box::new(f), handler).await?;
        todo!()
    }

    pub(crate) async fn submit<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        mapper: PartitionMapperRef<T, U>,
        handler: impl HandlerFn<U>,
    ) -> SparkResult<JobHandle<U>>
    where
        T: Datum,
        U: Send + 'static,
    {
        let job_id = self.next_job_id();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let jobcx = JobContext::new(Arc::clone(&self), rx);
        let _handle_error = tx.send(SchedulerEvent::JobSubmitted(JobSubmittedEvent {
            rdd,
            partitions,
            mapper: Box::new(mapper),
            job_id,
        }));
        Ok(JobHandle::new(self, job_id, handler))
    }

    fn next_stage_id(&self) -> StageId {
        StageId::new(self.stage_idx.fetch_add(1, Ordering::SeqCst))
    }

    fn next_job_id(&self) -> JobId {
        JobId::new(self.job_idx.fetch_add(1, Ordering::SeqCst))
    }
}

pub struct JobContext<T, U> {
    scheduler: Arc<DagScheduler>,
    rx: SchedulerEventReceiver<T, U>,
}

impl<T: Datum, U: Send + 'static> JobContext<T, U> {
    pub fn new(scheduler: Arc<DagScheduler>, rx: SchedulerEventReceiver<T, U>) -> Self {
        Self { scheduler, rx }
    }

    fn start(self, rx: SchedulerEventReceiver<T, U>) {
        std::thread::spawn(|| self.event_loop(rx));
    }

    #[tokio::main(flavor = "current_thread")]
    async fn event_loop(self, mut rx: SchedulerEventReceiver<T, U>) {
        while let Some(event) = rx.recv().await {
            self.handle_event(event).await
        }
    }

    async fn handle_event(&self, event: SchedulerEvent<T, U>) {
        match event {
            SchedulerEvent::JobSubmitted(event) => self.handle_job_submitted(event),
        }
    }

    fn handle_job_submitted(
        &self,
        JobSubmittedEvent { rdd, partitions, mapper, job_id }: JobSubmittedEvent<T, U>,
    ) {
        self.create_result_stage(rdd, partitions, mapper, job_id);
    }

    fn create_result_stage(
        &self,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        mapper: PartitionMapperRef<T, U>,
        job_id: JobId,
    ) {
        todo!()
    }
}
