use super::*;
use crate::scheduler::TaskOutput;
use rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct LocalExecutorBackend {
    pool: ThreadPool,
    // TODO include some form of task id so the scheduler knows what the response is for
    tx: UnboundedSender<TaskOutput>,
    rx: UnboundedReceiver<TaskOutput>,
}

impl LocalExecutorBackend {
    pub fn new(num_threads: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self { pool: ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap(), tx, rx }
    }
}

#[async_trait]
impl ExecutorBackend for LocalExecutorBackend {
    async fn execute_task(&self, task: Task) -> ExecutorResult<()> {
        let tx = self.tx.clone();
        self.pool.spawn(move || {
            let output = task.into_box().exec();
            tx.send(output).unwrap();
        });
        Ok(())
    }
}
