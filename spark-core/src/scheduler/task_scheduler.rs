use super::*;
use rayon::{ThreadPool, ThreadPoolBuilder};

pub struct TaskScheduler {
    backend: Box<dyn TaskSchedulerBackend>,
}

impl TaskScheduler {
    pub fn new(backend: Box<dyn TaskSchedulerBackend>) -> Self {
        Self { backend }
    }

    pub async fn submit_tasks<I: IntoIterator<Item = Task>>(&self, task_set: TaskSet<I>) {
        let rxs =
            task_set.tasks.into_iter().map(|task| self.backend.run_task(task)).collect::<Vec<_>>();
        futures::future::join_all(rxs).await;
    }

    pub fn default_parallelism(&self) -> usize {
        num_cpus::get()
    }
}

pub type TaskHandle = tokio::sync::oneshot::Receiver<()>;

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
            task.run();
            tx.send(()).expect("receiver hang up");
        });
        rx
    }
}
