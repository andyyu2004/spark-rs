use crate::serialize::SerdeArc;

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
    pub(super) job_id: JobId,
    pub(super) join_handle: JoinHandle<T>,
}

impl<T> JobHandle<T> {
    pub fn new(job_id: JobId, join_handle: JoinHandle<T>) -> Self {
        Self { join_handle, job_id }
    }
}

pub(super) struct JobContext<T, U, F> {
    scheduler: Arc<DagScheduler>,
    mapper: Arc<F>,
    final_rdd: TypedRddRef<T>,
    _t: PhantomData<T>,
    _u: PhantomData<U>,
}

impl<T, U, F> JobContext<T, U, F>
where
    T: CloneDatum,
    U: Datum,
    F: PartitionMapper<T, U>,
{
    pub fn new(scheduler: Arc<DagScheduler>, final_rdd: TypedRddRef<T>, mapper: Arc<F>) -> Self {
        Self { scheduler, final_rdd, mapper, _t: PhantomData, _u: PhantomData }
    }

    pub fn run_job(
        self,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        job_id: JobId,
    ) -> JoinHandle<SparkResult<JobOutput<U>>> {
        tokio::spawn(self.process_job(rdd, partitions, job_id))
    }

    #[instrument(skip(self))]
    async fn process_job(
        self,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        job_id: JobId,
    ) -> SparkResult<JobOutput<U>> {
        let stage_id = self.create_result_stage(rdd.into_inner().as_untyped(), partitions, job_id);
        self.create_active_job(job_id, stage_id);
        let outputs = self.submit_stage(stage_id).await?;
        let downcasted = outputs.into_iter().map(|output| {
            // TODO error handling
            let output = output.unwrap();
            *output.into_box().downcast::<U>().expect("expected output element to be of type `U`")
        });
        Ok(downcasted.collect())
    }

    /// Submits a stage to be executed
    /// This is a noop if the stage already exists
    #[async_recursion]
    #[instrument(skip(self))]
    async fn submit_stage(&self, stage_id: StageId) -> SparkResult<Vec<TaskOutput>> {
        trace!("submitting stage");
        // The id of the oldest job that depends on this stage
        let job_id =
            match self.stage_to_active_jobs.get_opt(stage_id).and_then(|jobs| jobs.get(0).copied())
            {
                Some(job_id) => job_id,
                None => todo!("abort stage"),
            };

        trace!(oldest_dependent_job_id = ?job_id);

        self.submit_parent_stages(stage_id).await?;

        let stage = self.stages.get(stage_id);
        let uncomputed_partitions = self.find_uncomputed_partitions(stage_id);
        trace!(?uncomputed_partitions);

        self.running_stages.insert(stage_id);

        let tasks = match &stage.kind {
            StageKind::ShuffleMap => todo!(),
            StageKind::Result { partitions: _ } =>
                uncomputed_partitions.into_iter().map(move |partition| {
                    ResultTask::new(
                        TaskMeta { stage_id, job_id, partition },
                        SerdeArc::from(Arc::clone(&self.final_rdd)),
                        Arc::clone(&self.mapper),
                    )
                    .boxed() as Task
                }),
        };

        if tasks.is_empty() {
            trace!("completed all tasks for stage");
            todo!()
        } else {
            let task_set = TaskSet::new(job_id, stage_id, tasks);
            let outputs = self.task_scheduler.submit_tasks(task_set).await?;
            Ok(outputs)
        }
    }

    #[instrument(skip(self))]
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
