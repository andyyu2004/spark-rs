use super::*;
use rayon::{ThreadPool, ThreadPoolBuilder};

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

pub struct LocalSchedulerBackend {
    pool: ThreadPool,
}

impl LocalSchedulerBackend {
    pub fn new() -> Self {
        Self { pool: ThreadPoolBuilder::default().build().unwrap() }
    }
}

impl TaskSchedulerBackend for LocalSchedulerBackend {
    fn run_task(&self, task: Task) -> TaskHandle {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            let output = task.into_box().exec();
            if tx.send(output).is_err() {
                panic!("receiver unexpectedly hung up");
            }
        });
        rx
    }
}
