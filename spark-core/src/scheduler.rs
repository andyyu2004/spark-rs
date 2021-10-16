mod event;
mod job;
mod stage;

use self::event::*;
use self::job::*;
use self::stage::*;
use crate::rdd::{RddRef, TypedRdd, TypedRddRef};
use crate::*;
use dashmap::DashMap;
use indexed_vec::{newtype_index, Idx, IndexVec};
use spark_ds::hash::IdxHash;
use std::collections::HashSet;
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
        let jobcx = JobContext::new(Arc::clone(&self), rx, mapper);
        let _handle_error = tx.send(SchedulerEvent::JobSubmitted(JobSubmittedEvent {
            rdd: rdd.as_untyped(),
            partitions,
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
    rx: SchedulerEventReceiver,
    mapper: PartitionMapperRef<T, U>,
}

impl<T: Datum, U: Send + 'static> JobContext<T, U> {
    pub fn new(
        scheduler: Arc<DagScheduler>,
        rx: SchedulerEventReceiver,
        mapper: PartitionMapperRef<T, U>,
    ) -> Self {
        Self { scheduler, rx, mapper }
    }

    fn start(self, rx: SchedulerEventReceiver) {
        std::thread::spawn(|| self.event_loop(rx));
    }

    #[tokio::main(flavor = "current_thread")]
    async fn event_loop(self, mut rx: SchedulerEventReceiver) {
        while let Some(event) = rx.recv().await {
            self.handle_event(event).await
        }
    }

    async fn handle_event(&self, event: SchedulerEvent) {
        match event {
            SchedulerEvent::JobSubmitted(event) => self.handle_job_submitted(event),
        }
    }

    fn handle_job_submitted(
        &self,
        JobSubmittedEvent { rdd, partitions, job_id }: JobSubmittedEvent,
    ) {
        self.create_result_stage(rdd, partitions, job_id);
    }

    fn create_result_stage(
        &self,
        rdd: RddRef,
        partitions: Partitions,
        job_id: JobId,
    ) -> ResultStage {
        let shuffle_deps = rdd.immediate_shuffle_dependencies();
        let parents = self.create_parent_stages(job_id, &shuffle_deps);
        let stage_id = self.scheduler.next_stage_id();
        ResultStage { stage_id, rdd, partitions, parents, job_id }
    }

    fn create_shuffle_map_stage(&self, job_id: JobId, shuffle_dep: &ShuffleDependency) -> StageId {
        let shuffle_id = shuffle_dep.shuffle_id;
        if let Some(stage) = self.scheduler.shuffle_id_to_map_stage.get(&shuffle_id) {
            return stage.stage_id;
        }

        let rdd = shuffle_dep.rdd();
        let parent_deps = rdd.immediate_shuffle_dependencies();
        for shuffle_dep in &parent_deps {
            self.create_shuffle_map_stage(job_id, shuffle_dep);
        }

        let stage_id = self.scheduler.next_stage_id();
        let shuffle_stage = ShuffleMapStage {
            rdd,
            stage_id,
            parents: self.create_parent_stages(job_id, &parent_deps),
        };
        self.scheduler.shuffle_id_to_map_stage.insert(shuffle_id, shuffle_stage);
        stage_id
    }

    fn create_parent_stages(
        &self,
        job_id: JobId,
        shuffle_deps: &HashSet<ShuffleDependency>,
    ) -> Vec<StageId> {
        shuffle_deps
            .iter()
            .map(|shuffle_dep| self.create_shuffle_map_stage(job_id, shuffle_dep))
            .collect()
    }
}
