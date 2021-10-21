mod distributed;
mod local;

use super::*;

pub use distributed::DistributedTaskSchedulerBackend;
use futures::future;
pub use local::LocalTaskSchedulerBackend;

pub struct TaskScheduler {
    task_idx: AtomicUsize,
    backend: Arc<dyn TaskSchedulerBackend>,
}

impl TaskScheduler {
    pub fn new(backend: Arc<dyn TaskSchedulerBackend>) -> Self {
        Self { backend, task_idx: Default::default() }
    }

    pub(super) fn next_task_id(&self) -> TaskId {
        TaskId::new(self.task_idx.fetch_add(1, Ordering::SeqCst))
    }

    #[instrument(skip(self))]
    pub async fn submit_tasks<I: ExactSizeIterator<Item = Task>>(
        &self,
        task_set: TaskSet<I>,
    ) -> SparkResult<Vec<TaskOutput>> {
        trace!("start submit_tasks");
        // This is a fairly convoluted section.
        // - Firstly, we iterate over each task in the task_set
        //   yielding an iterator over futures that each yield a `SparkResult<TaskHandle>`
        let iter = task_set.tasks.into_iter().map(|task| Arc::clone(&self.backend).run_task(task));
        // - Seondly, we `try_join_all` over this yielding `Vec<TaskHandle>` (where `TaskHandle` is itself a future)
        let handles = future::try_join_all(iter).await?;
        trace!("recv task_handles");
        // - Lastly, we `try_join_all` over this again yielding `Vec<TaskOutput>`
        let task_outputs = future::try_join_all(handles).await?;
        trace!("recv task_outputs");
        Ok(task_outputs)
    }

    pub fn default_parallelism(&self) -> usize {
        num_cpus::get()
    }
}

pub type TaskHandle = tokio::sync::oneshot::Receiver<TaskOutput>;
pub type TaskSender = tokio::sync::oneshot::Sender<TaskOutput>;

#[async_trait]
pub trait TaskSchedulerBackend: Send + Sync {
    async fn run_task(self: Arc<Self>, task: Task) -> SparkResult<TaskHandle>;
}
