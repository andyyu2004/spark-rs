mod backend;
mod local;

use crate::broadcast::BroadcastContext;
use crate::scheduler::{Task, TaskOutput};
use crate::{newtype_index, SparkEnv, SparkResult};
use async_bincode::AsyncBincodeWriter;
use async_trait::async_trait;
use bincode::Options;
use futures::SinkExt;
use rayon::ThreadPoolBuilder;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, BufReader};
use tokio::sync::Mutex;

pub use backend::*;
pub use local::LocalExecutorBackend;

pub type ExecutorResult<T> = anyhow::Result<T>;
pub type ExecutorError = anyhow::Error;

newtype_index!(ExecutorId);

impl ExecutorId {
    pub const DRIVER: Self = Self(0);
}

pub struct Executor {
    env: Arc<SparkEnv>,
    backend: Arc<dyn ExecutorBackend>,
}

impl Executor {
    pub async fn new(
        driver_addr: SocketAddr,
        backend: Arc<dyn ExecutorBackend>,
    ) -> SparkResult<Arc<Self>> {
        let env = SparkEnv::init_for_executor(driver_addr, BroadcastContext::new).await?;
        Ok(Arc::new(Self { backend, env }))
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
