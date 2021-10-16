mod event;
mod job;
mod stage;

pub use self::job::JobOutput;

use self::event::*;
use self::job::*;
use self::stage::*;
use crate::rdd::{RddRef, TypedRddRef};
use crate::*;
use dashmap::DashMap;
use indexed_vec::{newtype_index, Idx, IndexVec};
use spark_ds::hash::IdxHash;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::task::{JoinHandle, LocalSet};

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

static_assertions::assert_impl_all!(Arc<DagScheduler>: Send, Sync);

pub struct DagScheduler {
    job_idx: AtomicUsize,
    stage_idx: AtomicUsize,
    job_txs: IndexVec<JobId, SchedulerEventSender>,
    shuffle_id_to_map_stage: DashMap<ShuffleId, ShuffleMapStage, IdxHash>,
}

impl DagScheduler {
    pub fn new() -> Self {
        Self {
            job_txs: Default::default(),
            job_idx: Default::default(),
            stage_idx: Default::default(),
            shuffle_id_to_map_stage: Default::default(),
        }
    }

    pub(crate) async fn run<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        f: impl PartitionMapper<T, U>,
    ) -> SparkResult<JobOutput<U>>
    where
        T: Datum,
        U: Send + 'static,
    {
        assert!(!partitions.is_empty(), "handle empty partitions");
        let handle = self.submit(rdd, partitions, Box::new(f)).await?;
        Ok(handle.join_handle.await?)
    }

    pub(crate) async fn submit<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        mapper: PartitionMapperRef<T, U>,
    ) -> SparkResult<JobHandle<JobOutput<U>>>
    where
        T: Datum,
        U: Send + 'static,
    {
        let job_id = self.next_job_id();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(SchedulerEvent::JobSubmitted(JobSubmittedEvent {
            rdd: rdd.as_untyped(),
            partitions,
            job_id,
        }))
        .map_err(|_| anyhow!("failed to send `JobSubmitted` event"))?;
        let join_handle = JobContext::new(self, rx, mapper).start();
        Ok(JobHandle::new(job_id, join_handle))
    }

    fn next_stage_id(&self) -> StageId {
        StageId::new(self.stage_idx.fetch_add(1, Ordering::SeqCst))
    }

    fn next_job_id(&self) -> JobId {
        JobId::new(self.job_idx.fetch_add(1, Ordering::SeqCst))
    }
}
