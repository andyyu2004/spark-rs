use fixedbitset::FixedBitSet;

use crate::rdd::Rdd;

use super::*;
use std::ops::Deref;
use std::sync::Arc;

newtype_index!(JobId);

pub type JobOutput<T> = Vec<T>;

pub struct ActiveJob {
    pub(super) job_id: JobId,
    pub(super) final_stage_id: StageId,
    pub(super) finished: FixedBitSet,
    pub(super) partition_count: usize,
}

impl ActiveJob {
    pub fn new(scheduler: Arc<DagScheduler>, job_id: JobId, final_stage_id: StageId) -> Self {
        let stage = scheduler.stages.get(final_stage_id);
        let partition_count = match &stage.kind {
            StageKind::ShuffleMap => stage.rdd.partitions().len(),
            StageKind::Result { partitions } => partitions.len(),
        };

        Self {
            partition_count,
            job_id,
            final_stage_id,
            finished: FixedBitSet::with_capacity(partition_count),
        }
    }
}

pub struct JobHandle<T> {
    pub(super) join_handle: JoinHandle<T>,
    pub(super) job_id: JobId,
}

impl<T> JobHandle<T> {
    pub fn new(job_id: JobId, join_handle: JoinHandle<T>) -> Self {
        Self { join_handle, job_id }
    }
}

pub(super) struct JobContext<T, U> {
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

    pub fn start(self) -> JoinHandle<JobOutput<U>> {
        LocalSet::new().spawn_local(self.process_job())
    }

    async fn process_job(mut self) -> JobOutput<U> {
        while let Some(event) = self.rx.recv().await {
            self.handle_event(event).await
        }
        todo!()
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
        let stage_id = self.create_result_stage(rdd, partitions, job_id);
        Arc::clone(&self).create_active_job(job_id, stage_id);
        self.submit_stage(stage_id);
    }

    /// Submits a stage to be executed
    /// This is a noop if the stage already exists
    fn submit_stage(&self, stage_id: StageId) {
        if self.stage_exists(stage_id) {
            return;
        }

        // The id of the oldest job that depends on this stage
        let job_id =
            match self.stage_to_active_jobs.get_opt(stage_id).and_then(|jobs| jobs.get(0).copied())
            {
                Some(job_id) => job_id,
                None => todo!("abort stage"),
            };

        self.submit_parent_stages(stage_id);

        let stage = self.stages.get(stage_id);
        let uncomputed_partitions = self.find_uncomputed_partitions(stage_id);
        self.running_stages.insert(stage_id);
        let task = match &stage.kind {
            StageKind::ShuffleMap => todo!(),
            StageKind::Result { .. } => uncomputed_partitions.into_iter().map(|partition| Task {
                stage_id,
                job_id,
                partition,
                kind: TaskKind::Result(stage.rdd(), Arc::clone(&self.mapper)),
            }),
        };
        todo!()
    }

    pub(crate) fn submit_parent_stages(&self, stage_id: StageId) {
        let stage = self.stages.get(stage_id);
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
}

impl<T: Datum, U: Send + 'static> Deref for JobContext<T, U> {
    type Target = Arc<DagScheduler>;

    fn deref(&self) -> &Self::Target {
        &self.scheduler
    }
}
