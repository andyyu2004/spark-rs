use super::*;
use rayon::{ThreadPool, ThreadPoolBuilder};

pub struct LocalExecutor {
    pool: ThreadPool,
}

impl LocalExecutor {
    pub fn new(num_threads: usize) -> Self {
        Self { pool: ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap() }
    }
}

#[async_trait]
impl ExecutorBackend for LocalExecutor {
    async fn execute_task(&self, task: Task) -> ExecutorResult<()> {
        Ok(())
    }
}
