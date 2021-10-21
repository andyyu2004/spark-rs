use std::sync::Once;

use super::*;
use rayon::{ThreadPool, ThreadPoolBuilder};

/// A [TaskSchedulerBackend] implemented by using a local threadpool.
pub struct LocalTaskSchedulerBackend {
    pool: ThreadPool,
}

impl LocalTaskSchedulerBackend {
    pub fn new(num_threads: usize) -> Self {
        Self { pool: ThreadPoolBuilder::default().num_threads(num_threads).build().unwrap() }
    }
}

#[async_trait]
impl TaskSchedulerBackend for LocalTaskSchedulerBackend {
    #[instrument(skip(self, task))]
    async fn run_task(self: Arc<Self>, task: Task) -> SparkResult<TaskHandle> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            let output = task.into_box().exec();
            if tx.send(output).is_err() {
                panic!("receiver unexpectedly hung up");
            }
        });
        Ok(rx)
    }
}
