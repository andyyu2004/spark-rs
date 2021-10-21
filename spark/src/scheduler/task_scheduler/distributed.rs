use super::*;
use crate::config::DistributedUrl;
use crate::executor::{Executor, LocalExecutorBackend};
use async_bincode::{AsyncBincodeReader, AsyncBincodeWriter, AsyncDestination};
use dashmap::DashMap;
use futures::{SinkExt, TryStreamExt};
use std::lazy::SyncOnceCell;
use std::sync::Once;
use tokio::io::{DuplexStream, WriteHalf};
use tokio::sync::Mutex;

type BincodeTaskWriter = AsyncBincodeWriter<WriteHalf<DuplexStream>, Task, AsyncDestination>;

pub struct DistributedTaskSchedulerBackend {
    executor: Arc<Executor>,
    txs: DashMap<TaskId, TaskSender>,
    writer: SyncOnceCell<Mutex<BincodeTaskWriter>>,
}

impl DistributedTaskSchedulerBackend {
    pub fn new(url: DistributedUrl) -> Self {
        let backend = match url {
            DistributedUrl::Local { num_threads } =>
                Box::new(LocalExecutorBackend::new(num_threads)),
        };
        let executor = Executor::new(backend);
        Self { executor, writer: Default::default(), txs: Default::default() }
    }

    async fn start(self: Arc<Self>) -> SparkResult<()> {
        let (stream, executor_stream) = tokio::io::duplex(8192);
        let (reader, writer) = tokio::io::split(stream);
        let writer = AsyncBincodeWriter::from(writer).for_async();
        self.writer.set(Mutex::new(writer)).unwrap();
        let (executor_reader, executor_writer) = tokio::io::split(executor_stream);

        let executor = Arc::clone(&self.executor);
        let handle = tokio::spawn(executor.execute(executor_reader, executor_writer));
        let stream = AsyncBincodeReader::<_, TaskOutput>::from(reader);
        stream
            .try_for_each(|output @ (task_id, _)| {
                let this = Arc::clone(&self);
                async move {
                    let (_, tx) = this.txs.remove(&task_id).unwrap();
                    tx.send(output).or_else(|_| panic!())
                }
            })
            .await?;
        handle.await??;
        Ok(())
    }
}

#[async_trait]
impl TaskSchedulerBackend for DistributedTaskSchedulerBackend {
    async fn run_task(self: Arc<Self>, task: Task) -> SparkResult<TaskHandle> {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            // TODO how to handle the errors if start fails?
            let this = Arc::clone(&self);
            tokio::spawn(async move { this.start().await });
        });

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.txs.insert(task.id(), tx);

        // HACK: This yield_now is meant to give the task running `start` a chance to set the writer before we try to access it
        tokio::task::yield_now().await;
        self.writer
            .get()
            .expect("guess the hack above didn't work :/")
            .lock()
            .await
            .send(task)
            .await?;

        Ok(rx)
    }
}
