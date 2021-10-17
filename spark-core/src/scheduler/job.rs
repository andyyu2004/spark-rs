use super::*;
use std::sync::Arc;

newtype_index!(JobId);

pub type JobOutput<T> = Vec<T>;

pub struct ActiveJob {
    job_id: JobId,
    final_stage: StageId,
}

impl ActiveJob {
    pub fn new(job_id: JobId, final_stage: StageId) -> Self {
        Self { job_id, final_stage }
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
            SchedulerEvent::JobSubmitted(event) => self.scheduler.handle_job_submitted(event),
        }
    }
}
