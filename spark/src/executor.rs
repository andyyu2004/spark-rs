mod backend;
mod local;

use crate::broadcast::BroadcastContext;
use crate::rpc::SparkRpcClient;
use crate::scheduler::{Task, TaskOutput};
use async_bincode::AsyncBincodeWriter;
use async_trait::async_trait;
use bincode::Options;
use futures::SinkExt;
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, BufReader};
use tokio::sync::Mutex;

pub use backend::*;
pub use local::LocalExecutorBackend;

pub type ExecutorResult<T> = anyhow::Result<T>;
pub type ExecutorError = anyhow::Error;

pub struct Executor {
    rpc_client: SparkRpcClient,
    broadcaster: BroadcastContext,
    backend: Arc<dyn ExecutorBackend>,
}

impl Executor {
    pub fn new(backend: Arc<dyn ExecutorBackend>, rpc_client: SparkRpcClient) -> Arc<Self> {
        Arc::new(Self { backend, rpc_client, broadcaster: BroadcastContext::new() })
    }

    #[instrument(skip(self, reader, writer))]
    pub async fn execute(
        self: Arc<Self>,
        reader: impl AsyncRead + Unpin,
        writer: impl AsyncWrite + Send + Unpin + 'static,
    ) -> ExecutorResult<()> {
        let handle = tokio::spawn(Arc::clone(&self).send_outputs(writer));

        let pool = ThreadPoolBuilder::new().build().unwrap();
        let mut reader = BufReader::new(reader);
        loop {
            if reader.fill_buf().await?.is_empty() {
                break;
            }
            let size = reader.read_u32().await?;
            let mut buf = vec![0; size as usize];
            reader.read_exact(&mut buf).await?;

            let backend = Arc::clone(&self.backend);
            let (tx, rx) = tokio::sync::oneshot::channel();

            tokio::spawn(async move {
                let task = rx.await?;
                backend.execute_task(task).await?;
                Ok::<_, ExecutorError>(())
            });

            pool.spawn(move || {
                trace!("start deserializing task");
                let task = bincode::options()
                    .with_limit(u32::max_value() as u64)
                    .allow_trailing_bytes()
                    .deserialize::<Task>(&buf[..])
                    .unwrap();
                trace!("finished deserializing task `{:?}`", task.id());
                trace!("start sending task to executor_backend");
                tx.send(task).unwrap();
                trace!("finish sending task to executor_backend");
            });
        }

        handle.await??;
        Ok(())
    }

    /// Send outputs from the executing tasks back via `self.writer`
    #[instrument(skip(self, writer))]
    pub async fn send_outputs(
        self: Arc<Self>,
        writer: impl AsyncWrite + Unpin,
    ) -> ExecutorResult<()> {
        let mut writer = AsyncBincodeWriter::from(writer).for_async();
        while let Some(output) = self.backend.task_output().await {
            trace!("recv task output from executor backend");
            writer.send(output).await?;
            trace!("task output sent");
        }
        Ok(())
    }
}
