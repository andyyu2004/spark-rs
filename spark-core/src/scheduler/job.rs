use super::*;
use async_recursion::async_recursion;
use fixedbitset::FixedBitSet;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
crate::newtype_index!(JobId);

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

pub(super) struct JobContext<T, U, F> {
    scheduler: Arc<DagScheduler>,
    rx: SchedulerEventReceiver,
    mapper: Arc<F>,
    _t: PhantomData<T>,
    _u: PhantomData<U>,
}

impl<T, U, F> JobContext<T, U, F>
where
    T: Datum,
    U: Send + 'static,
    F: PartitionMapper<T, U>,
{
    pub fn new(scheduler: Arc<DagScheduler>, rx: SchedulerEventReceiver, mapper: Arc<F>) -> Self {
        Self { scheduler, rx, mapper, _t: PhantomData, _u: PhantomData }
    }

    pub fn start(self) -> JoinHandle<JobOutput<U>> {
        LocalSet::new().spawn_local(self.process_job())
    }

    async fn process_job(mut self) -> JobOutput<U> {
        while let Some(event) = self.rx.recv().await {
            if let Err(err) = self.handle_event(event).await {
                todo!("handle error while processing new job")
            }
        }
        todo!()
    }

    async fn handle_event(&self, event: SchedulerEvent) -> SparkResult<()> {
        match event {
            SchedulerEvent::JobSubmitted(event) => self.handle_job_submitted(event).await,
        }
    }

    async fn handle_job_submitted(
        &self,
        JobSubmittedEvent { rdd, partitions, job_id }: JobSubmittedEvent,
    ) -> SparkResult<()> {
        let stage_id = self.create_result_stage(rdd, partitions, job_id);
        Arc::clone(&self).create_active_job(job_id, stage_id);
        self.submit_stage(stage_id).await
    }

    /// Submits a stage to be executed
    /// This is a noop if the stage already exists
    #[async_recursion(?Send)]
    async fn submit_stage(&self, stage_id: StageId) -> SparkResult<()> {
        if self.stage_exists(stage_id) {
            return Ok(());
        }

        // The id of the oldest job that depends on this stage
        let job_id =
            match self.stage_to_active_jobs.get_opt(stage_id).and_then(|jobs| jobs.get(0).copied())
            {
                Some(job_id) => job_id,
                None => todo!("abort stage"),
            };

        self.submit_parent_stages(stage_id).await?;

        let stage = self.stages.get(stage_id);
        let uncomputed_partitions = self.find_uncomputed_partitions(stage_id);
        self.running_stages.insert(stage_id);

        let bytes = match &stage.kind {
            StageKind::ShuffleMap => todo!(),
            StageKind::Result { .. } =>
                Arc::new(bincode::serialize(&(&stage.rdd(), &self.mapper))?),
        };
        let tasks = match &stage.kind {
            StageKind::ShuffleMap => todo!(),
            StageKind::Result { partitions: _ } =>
                uncomputed_partitions.into_iter().map(move |partition| Task {
                    stage_id,
                    job_id,
                    partition,
                    kind: TaskKind::Result(Arc::clone(&bytes)),
                }),
        };

        if tasks.is_empty() {
            todo!()
        } else {
            let task_set = TaskSet::new(job_id, stage_id, tasks);
            self.task_scheduler.submit_tasks(task_set).await;
        }
        Ok(())
    }

    pub(crate) async fn submit_parent_stages(&self, stage_id: StageId) -> SparkResult<()> {
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
                        self.submit_stage(stage_id).await?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl<T, U, F> Deref for JobContext<T, U, F> {
    type Target = Arc<DagScheduler>;

    fn deref(&self) -> &Self::Target {
        &self.scheduler
    }
}
