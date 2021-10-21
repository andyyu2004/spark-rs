use super::*;
use crate::scheduler::TaskOutput;
use rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct LocalExecutorBackend {
    pool: ThreadPool,
    // TODO include some form of task id so the scheduler knows what the response is for
    tx: UnboundedSender<TaskOutput>,
    // TODO can we get rid of this mutex
    rx: Mutex<UnboundedReceiver<TaskOutput>>,
}

impl LocalExecutorBackend {
    pub fn new(num_threads: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(rx),
            pool: ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap(),
        }
    }
}

#[async_trait]
impl ExecutorBackend for LocalExecutorBackend {
    #[instrument(skip(self))]
    async fn execute_task(&self, task: Task) -> ExecutorResult<()> {
        trace!("begin execute_task");
        let tx = self.tx.clone();
        self.pool.spawn(move || {
            let output = task.into_box().exec();
            tx.send(output).unwrap();
        });
        Ok(())
    }

    #[instrument(skip(self))]
    async fn task_output(&self) -> Option<TaskOutput> {
        self.rx.lock().await.recv().await
    }
}
