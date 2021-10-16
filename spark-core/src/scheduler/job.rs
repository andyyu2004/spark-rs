use super::*;
use std::sync::Arc;

newtype_index!(JobId);

pub type JobOutput<T> = Vec<T>;

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
