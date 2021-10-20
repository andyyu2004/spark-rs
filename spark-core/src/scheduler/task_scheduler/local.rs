use super::*;
use rayon::{ThreadPool, ThreadPoolBuilder};

/// A [TaskSchedulerBackend] implemented by using a local threadpool.
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
