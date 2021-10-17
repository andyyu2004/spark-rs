mod event;
mod job;
mod stage;

pub use self::job::JobOutput;

use self::event::*;
use self::job::*;
use self::stage::*;
use crate::rdd::{RddRef, TypedRddRef};
use crate::*;
use dashmap::{DashMap, DashSet};
use indexed_vec::{newtype_index, Idx};
use spark_ds::hash::IdxHash;
use spark_ds::sync::ConcurrentIndexVec;
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

pub trait TaskSchedulerBackend: Send + Sync {}

static_assertions::assert_impl_all!(Arc<DagScheduler>: Send, Sync);

pub struct DagScheduler {
    job_idx: AtomicUsize,
    stage_idx: AtomicUsize,
    job_txs: ConcurrentIndexVec<JobId, SchedulerEventSender>,
    stages: ConcurrentIndexVec<StageId, Stage>,
    active_jobs: ConcurrentIndexVec<JobId, Vec<ActiveJob>>,
    shuffle_id_to_map_stage: ConcurrentIndexVec<ShuffleId, StageId>,
    /// Map from `StageId` to the active jobs that require the stage
    stage_to_active_jobs: ConcurrentIndexVec<StageId, Vec<JobId>>,
    waiting_stages: DashSet<StageId>,
    running_stages: DashSet<StageId>,
    failed_stages: DashSet<StageId>,
}

impl DagScheduler {
    pub fn new() -> Self {
        Self {
            job_txs: Default::default(),
            job_idx: Default::default(),
            stage_idx: Default::default(),
            shuffle_id_to_map_stage: Default::default(),
            active_jobs: Default::default(),
            stages: Default::default(),
            stage_to_active_jobs: Default::default(),
            waiting_stages: Default::default(),
            running_stages: Default::default(),
            failed_stages: Default::default(),
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
        let handle = self.submit_job(rdd, partitions, Box::new(f)).await?;
        Ok(handle.join_handle.await?)
    }

    pub(crate) async fn submit_job<T, U>(
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

    /// Submits a stage to be executed
    /// This is a noop if the stage already exists
    fn submit_stage(&self, stage_id: StageId) {
        if self.stage_exists(stage_id) {
            return;
        }

        let oldest_active_job_id = match self.stage_to_active_jobs.get(stage_id).unwrap().first() {
            Some(job_id) => job_id,
            None => todo!("abort stage"),
        };

        self.submit_parent_stages(stage_id);
        self.running_stages.insert(stage_id);
        todo!()
    }

    fn stage_exists(&self, stage_id: StageId) -> bool {
        self.stages.contains_key(stage_id)
    }

    pub(crate) fn submit_parent_stages(&self, stage_id: StageId) {
        let stage = &self.stages.get(stage_id).unwrap();
        let mut rdds = vec![stage.rdd()];
        let mut visited = HashSet::new();
        while let Some(rdd) = rdds.pop() {
            if visited.contains(&rdd.id()) {
                continue;
            }
            visited.insert(rdd.id());
            for dep in rdd.dependencies().iter() {
                match dep {
                    Dependency::Narrow(narrow_dep) => rdds.push(narrow_dep.rdd()),
                    Dependency::Shuffle(shuffle_dep) => {
                        let stage_id = self.create_shuffle_map_stage(stage.job_id, shuffle_dep);
                        self.submit_stage(stage_id);
                    }
                }
            }
        }
    }

    fn handle_job_submitted(
        &self,
        JobSubmittedEvent { rdd, partitions, job_id }: JobSubmittedEvent,
    ) {
        let stage_id = self.create_result_stage(rdd, partitions, job_id);
        self.create_active_job(job_id, stage_id);
        self.submit_stage(stage_id);
    }

    fn create_result_stage(&self, rdd: RddRef, partitions: Partitions, job_id: JobId) -> StageId {
        let shuffle_deps = rdd.immediate_shuffle_dependencies();
        let parents = self.create_parent_stages(job_id, &shuffle_deps);
        let mk_stage = |stage_id| Stage {
            stage_id,
            rdd,
            parents,
            job_id,
            kind: StageKind::Result { partitions },
        };
        self.alloc_stage(mk_stage)
    }

    fn create_shuffle_map_stage(&self, job_id: JobId, shuffle_dep: &ShuffleDependency) -> StageId {
        let shuffle_id = shuffle_dep.shuffle_id;
        if let Some(stage_id) = self.shuffle_id_to_map_stage.get(shuffle_id) {
            return *stage_id;
        }

        let rdd = shuffle_dep.rdd();
        let parent_deps = rdd.immediate_shuffle_dependencies();
        for shuffle_dep in &parent_deps {
            self.create_shuffle_map_stage(job_id, shuffle_dep);
        }

        let mk_shuffle_stage = |stage_id| Stage {
            rdd,
            stage_id,
            job_id,
            parents: self.create_parent_stages(job_id, &parent_deps),
            kind: StageKind::ShuffleMap,
        };
        let stage_id = self.alloc_stage(mk_shuffle_stage);
        self.shuffle_id_to_map_stage.insert(shuffle_id, stage_id);
        stage_id
    }

    fn alloc_stage(&self, mk_stage: impl FnOnce(StageId) -> Stage) -> StageId {
        let stage_id = self.next_stage_id();
        assert!(self.stages.insert(stage_id, mk_stage(stage_id).into()).is_none());
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

    fn create_active_job(&self, job_id: JobId, stage_id: StageId) {
        let active_job = ActiveJob::new(job_id, stage_id);
        self.create_active_job(job_id, stage_id);
        self.stage_to_active_jobs.entry(stage_id).or_default().push(job_id);
        self.active_jobs.entry(job_id).or_default().push(active_job);
    }

    fn next_stage_id(&self) -> StageId {
        StageId::new(self.stage_idx.fetch_add(1, Ordering::SeqCst))
    }

    fn next_job_id(&self) -> JobId {
        JobId::new(self.job_idx.fetch_add(1, Ordering::SeqCst))
    }
}
