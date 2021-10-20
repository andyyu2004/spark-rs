mod distributed;
mod local;

use super::*;

pub use distributed::DistributedTaskSchedulerBackend;
pub use local::LocalTaskSchedulerBackend;

pub struct TaskScheduler {
    backend: Box<dyn TaskSchedulerBackend>,
}

impl TaskScheduler {
    pub fn new(backend: Box<dyn TaskSchedulerBackend>) -> Self {
        Self { backend }
    }

    pub async fn submit_tasks<I: IntoIterator<Item = Task>>(
        &self,
        task_set: TaskSet<I>,
    ) -> SparkResult<Vec<TaskOutput>> {
        // This is fairly convuluted
        // - Firstly, we iterate over each task in the task_set
        //   yielding an iterator over futures that each yield a `SparkResult<TaskHandle>`
        let results = task_set.tasks.into_iter().map(|task| self.backend.run_task(task));
        // - Seondly, we `try_join_all` over this yielding `Vec<TaskHandle>` (where `TaskHandle` is a future)
        let rxs = futures::future::try_join_all(results).await?;
        // - Lastly, we `try_join_all` over this again yielding `Vec<TaskOutput>`
        Ok(futures::future::try_join_all(rxs).await?)
    }

    pub fn default_parallelism(&self) -> usize {
        num_cpus::get()
    }
}

pub type TaskHandle = tokio::sync::oneshot::Receiver<TaskOutput>;

#[async_trait]
pub trait TaskSchedulerBackend: Send + Sync {
    async fn run_task(&self, task: Task) -> SparkResult<TaskHandle>;
}
