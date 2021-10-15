mod event;
mod job;

use self::event::*;
use self::job::JobHandle;
use crate::rdd::Rdd;
use crate::*;
use indexed_vec::{newtype_index, Idx};
use spark_ds::sync::OneThread;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Handler function that is called when a partition has been computed.
/// It is passed the partition index and the output computation.
pub trait HandlerFn<T> = FnOnce(PartitionIndex, T);

pub trait PartitionMapper<T, U> = FnOnce(TaskContext, SparkIterator<T>) -> U;

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

    pub fn run<T>(&self, rdd: impl Rdd<Output = T>) -> SparkResult<Vec<T>> {
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

    pub(crate) async fn run<T: Datum, U>(
        self: Arc<Self>,
        rdd: impl Rdd<Output = T>,
        partitions: Partitions,
        f: impl PartitionMapper<T, U>,
        handler: impl HandlerFn<U>,
    ) -> SparkResult<()> {
        assert!(!partitions.is_empty(), "handle empty partitions");
        let handle = self.submit(rdd, partitions, f, handler).await?;
        todo!()
    }

    pub(crate) async fn submit<T: Datum, U>(
        self: Arc<Self>,
        rdd: impl Rdd<Output = T>,
        partitions: Partitions,
        f: impl PartitionMapper<T, U>,
        handler: impl HandlerFn<U>,
    ) -> SparkResult<JobHandle<U>> {
        let job_id = self.next_job_id();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Arc::clone(&self).spawn_event_loop_for_job(rx);
        let _todo = tx.send(SchedulerEvent::JobSubmitted { job_id, rdd: Box::new(rdd) });
        Ok(JobHandle::new(self, job_id, handler))
    }

    fn spawn_event_loop_for_job<T: Datum>(self: Arc<Self>, mut rx: SchedulerEventReceiver<T>) {
        std::thread::spawn(|| self.start_event_loop_for_job(rx));
    }

    #[tokio::main(flavor = "current_thread")]
    async fn start_event_loop_for_job<T>(self: Arc<Self>, mut rx: SchedulerEventReceiver<T>) {
        while let Some(event) = rx.recv().await {
            self.handle_event(event)
        }
    }

    fn handle_event<T>(&self, event: SchedulerEvent<T>) {
        match event {
            SchedulerEvent::JobSubmitted { .. } => todo!(),
        }
    }

    fn next_stage_id(&self) -> StageId {
        StageId::new(self.stage_idx.fetch_add(1, Ordering::SeqCst))
    }

    fn next_job_id(&self) -> JobId {
        JobId::new(self.job_idx.fetch_add(1, Ordering::SeqCst))
    }
}
