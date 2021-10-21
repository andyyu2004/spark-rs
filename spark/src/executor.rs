mod backend;
mod local;

use crate::scheduler::{Task, TaskOutput};
use async_bincode::{AsyncBincodeReader, AsyncBincodeWriter};
use async_trait::async_trait;
use futures::{SinkExt, TryStreamExt};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;

pub use backend::*;
pub use local::LocalExecutorBackend;

pub type ExecutorResult<T> = anyhow::Result<T>;

pub struct Executor {
    backend: Box<dyn ExecutorBackend>,
}

impl Executor {
    pub fn new(backend: Box<dyn ExecutorBackend>) -> Arc<Self> {
        Arc::new(Self { backend })
    }

    #[instrument(skip(self, reader, writer))]
    pub async fn execute(
        self: Arc<Self>,
        reader: impl AsyncRead + Unpin,
        writer: impl AsyncWrite + Send + Unpin + 'static,
    ) -> ExecutorResult<()> {
        let handle = tokio::spawn(Arc::clone(&self).send_outputs(writer));
        let stream = AsyncBincodeReader::<_, Task>::from(reader);
        stream
            .map_err(|err| err.into())
            .try_for_each_concurrent(None, |task| self.backend.execute_task(task))
            .await?;

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
