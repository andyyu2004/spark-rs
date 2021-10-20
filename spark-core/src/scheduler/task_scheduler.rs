mod distributed;
mod local;

pub use distributed::DistributedTaskSchedulerBackend;
pub use local::LocalSchedulerBackend;

use super::*;

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
        let rxs = task_set.tasks.into_iter().map(|task| self.backend.run_task(task));
        Ok(futures::future::try_join_all(rxs).await?)
    }

    pub fn default_parallelism(&self) -> usize {
        num_cpus::get()
    }
}

pub type TaskHandle = tokio::sync::oneshot::Receiver<TaskOutput>;

pub trait TaskSchedulerBackend: Send + Sync {
    fn run_task(&self, task: Task) -> TaskHandle;
}
