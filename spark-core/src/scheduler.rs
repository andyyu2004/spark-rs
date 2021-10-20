mod event;
mod job;
mod stage;
mod task;
mod task_scheduler;

pub use self::job::JobOutput;
pub use self::task::*;
pub use self::task_scheduler::*;

use self::event::*;
use self::job::*;
use self::stage::*;
use crate::data::Datum;
use crate::rdd::{RddRef, TypedRddRef};
use crate::serialize::{SerdeArc, SerdeFn};
use crate::*;
use dashmap::DashSet;
use indexed_vec::Idx;
use spark_ds::sync::{ConcurrentIndexVec, MapRef};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

pub trait PartitionMapper<T, U> = SerdeFn(&mut TaskContext, SparkIteratorRef<T>) -> U;

static_assertions::assert_impl_all!(Arc<DagScheduler>: Send, Sync);

pub struct DagScheduler {
    task_scheduler: Arc<TaskScheduler>,
    job_idx: AtomicUsize,
    stage_idx: AtomicUsize,
    job_txs: ConcurrentIndexVec<JobId, SchedulerEventSender>,
    stages: ConcurrentIndexVec<StageId, Stage>,
    active_jobs: ConcurrentIndexVec<JobId, ActiveJob>,
    shuffle_id_to_map_stage: ConcurrentIndexVec<ShuffleId, StageId>,
    /// Map from `StageId` to all active jobs that require the stage
    stage_to_active_jobs: ConcurrentIndexVec<StageId, Vec<JobId>>,
    /// Map from a `ResultStage` to its `ActiveJob`
    result_stage_to_active_job: ConcurrentIndexVec<StageId, JobId>,
    waiting_stages: DashSet<StageId>,
    running_stages: DashSet<StageId>,
    failed_stages: DashSet<StageId>,
}

impl DagScheduler {
    pub fn new(task_scheduler: Arc<TaskScheduler>) -> Self {
        Self {
            task_scheduler,
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
            result_stage_to_active_job: Default::default(),
        }
    }

    pub(crate) async fn run<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        f: impl PartitionMapper<T, U>,
    ) -> SparkResult<JobOutput<U>>
    where
        T: CloneDatum,
        U: Datum,
    {
        assert!(!partitions.is_empty(), "handle empty partitions");
        let handle = self.submit_job(rdd, partitions, Arc::new(f)).await;
        handle.join_handle.await?
    }

    pub(crate) async fn submit_job<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        mapper: Arc<impl PartitionMapper<T, U>>,
    ) -> JobHandle<SparkResult<JobOutput<U>>>
    where
        T: CloneDatum,
        U: Datum,
    {
        let job_id = self.next_job_id();
        let join_handle =
            JobContext::new(self, SerdeArc::clone(&rdd), mapper).run_job(rdd, partitions, job_id);
        JobHandle::new(job_id, join_handle)
    }

    fn active_job(&self, stage: StageId) -> MapRef<'_, JobId, ActiveJob> {
        let job_id = self.result_stage_to_active_job.get(stage);
        self.active_jobs.get(*job_id)
    }

    #[instrument(skip(self))]
    fn find_uncomputed_partitions(&self, stage_id: StageId) -> Partitions {
        let stage = self.stages.get(stage_id);
        let active_job = self.active_job(stage_id);
        match &stage.kind {
            StageKind::ShuffleMap => todo!(),
            StageKind::Result { partitions } => (0..partitions.len())
                .filter(|&i| !active_job.finished.contains(i))
                .map(PartitionIdx::new)
                .collect(),
        }
    }

    #[instrument(skip(self))]
    fn create_result_stage(&self, rdd: RddRef, partitions: Partitions, job_id: JobId) -> StageId {
        assert!(!partitions.is_empty());
        let shuffle_deps = rdd.immediate_shuffle_dependencies();
        let parents = self.create_parent_stages(job_id, &shuffle_deps);
        let mk_stage = |stage_id| Stage {
            stage_id,
            rdd,
            parents,
            job_id,
            kind: StageKind::Result { partitions },
        };
        let stage_id = self.alloc_stage(mk_stage);
        self.result_stage_to_active_job.insert(stage_id, job_id);
        stage_id
    }

    fn create_shuffle_map_stage(&self, job_id: JobId, shuffle_dep: &ShuffleDependency) -> StageId {
        let shuffle_id = shuffle_dep.shuffle_id;
        if let Some(stage_id) = self.shuffle_id_to_map_stage.get_opt(shuffle_id) {
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

    #[instrument(skip(self, mk_stage))]
    fn alloc_stage(&self, mk_stage: impl FnOnce(StageId) -> Stage) -> StageId {
        let stage_id = self.next_stage_id();
        trace!(next_stage_id = ?stage_id);
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

    #[instrument(skip(self))]
    fn create_active_job(self: &Arc<Self>, job_id: JobId, stage_id: StageId) {
        trace!("creating active job");
        self.stage_to_active_jobs.entry(stage_id).or_default().push(job_id);
        let active_job = ActiveJob::new(Arc::clone(self), job_id, stage_id);
        assert!(self.active_jobs.insert(job_id, active_job).is_none());
    }

    fn next_stage_id(&self) -> StageId {
        StageId::new(self.stage_idx.fetch_add(1, Ordering::SeqCst))
    }

    fn next_job_id(&self) -> JobId {
        JobId::new(self.job_idx.fetch_add(1, Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests;
