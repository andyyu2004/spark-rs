use super::*;
use crate::config::DistributedUrl;
use crate::executor::{Executor, LocalExecutorBackend};
use async_bincode::{AsyncBincodeReader, AsyncBincodeWriter, AsyncDestination};
use dashmap::DashMap;
use futures::{SinkExt, TryStreamExt};
use std::lazy::SyncOnceCell;
use std::sync::Once;
use tokio::io::{DuplexStream, WriteHalf};

pub const TASK_LIMIT: usize = 100;

type BincodeTaskWriter = AsyncBincodeWriter<WriteHalf<DuplexStream>, Task, AsyncDestination>;

pub struct DistributedTaskSchedulerBackend {
    executor: Arc<Executor>,
    txs: DashMap<TaskId, TaskSender>,
    task_dispatcher: SyncOnceCell<tokio::sync::mpsc::Sender<Task>>,
}

impl DistributedTaskSchedulerBackend {
    pub fn new(url: DistributedUrl) -> Self {
        let backend = match url {
            DistributedUrl::Local { num_threads } =>
                Box::new(LocalExecutorBackend::new(num_threads)),
        };
        let executor = Executor::new(backend);
        Self { executor, task_dispatcher: Default::default(), txs: Default::default() }
    }

    #[instrument(skip(self))]
    async fn start(self: Arc<Self>) -> SparkResult<()> {
        trace!("start distributed task_scheduler backend");
        let (stream, executor_stream) = tokio::io::duplex(8192);
        let (reader, writer) = tokio::io::split(stream);

        let mut writer = AsyncBincodeWriter::from(writer).for_async();
        let (task_dispatcher, mut task_receiver) = tokio::sync::mpsc::channel(TASK_LIMIT);
        // The reason for handling the sending of the writer over a separate task is because
        // `send` on a channel sender only requires `&self` while `send` on the `Sink` requires `&mut self`,
        // and so if we were to send on the Sink directly from `run_task` we would need a mutex.
        let dispatcher_handle = tokio::spawn(async move {
            while let Some(task) = task_receiver.recv().await {
                writer.send(task).await?;
            }
            Ok::<_, SparkError>(())
        });

        self.task_dispatcher.set(task_dispatcher).unwrap();
        let (executor_reader, executor_writer) = tokio::io::split(executor_stream);

        let executor = Arc::clone(&self.executor);
        let executor_handle = tokio::spawn(executor.execute(executor_reader, executor_writer));
        let stream = AsyncBincodeReader::<_, TaskOutput>::from(reader);
        stream
            .try_for_each(|output @ (task_id, _)| {
                trace!("recv completed task `{:?}`", task_id);
                let this = Arc::clone(&self);
                async move {
                    let (_, tx) = this.txs.remove(&task_id).unwrap();
                    tx.send(output).or_else(|_| panic!())
                }
            })
            .await?;

        dispatcher_handle.await??;
        executor_handle.await??;
        Ok(())
    }
}

#[async_trait]
impl TaskSchedulerBackend for DistributedTaskSchedulerBackend {
    #[instrument(skip(self))]
    async fn run_task(self: Arc<Self>, task: Task) -> SparkResult<TaskHandle> {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            // TODO how to handle the errors if start fails?
            let this = Arc::clone(&self);
            tokio::spawn(async move { this.start().await });
        });

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.txs.insert(task.id(), tx);

        loop {
            // Loop until the task spawned above has had a chance to run the initialization
            match self.task_dispatcher.get() {
                Some(dispatcher) => {
                    dispatcher.send(task).await.unwrap();
                    break;
                }
                None => tokio::task::yield_now().await,
            }
        }

        Ok(rx)
    }
}
