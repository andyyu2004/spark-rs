mod backend;
mod local;

use crate::scheduler::{Task, TaskOutput};
use async_bincode::AsyncBincodeReader;
use async_trait::async_trait;
use futures::TryStreamExt;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub use backend::*;
pub use local::LocalExecutorBackend;

pub type ExecutorResult<T> = anyhow::Result<T>;

pub struct Executor {
    backend: Box<dyn ExecutorBackend>,
    writer: Pin<Box<dyn AsyncWrite + Send + Sync>>,
}

impl Executor {
    pub fn new(
        backend: Box<dyn ExecutorBackend>,
        writer: impl AsyncWrite + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self { backend, writer: Box::pin(writer) })
    }

    pub async fn execute(self: Arc<Self>, reader: impl AsyncRead + Unpin) -> ExecutorResult<()> {
        tokio::spawn(Arc::clone(&self).send_outputs());

        let stream = AsyncBincodeReader::<_, Task>::from(reader);
        stream
            .map_err(|err| err.into())
            .try_for_each_concurrent(None, |task| async { self.backend.execute_task(task).await })
            .await?;
        Ok(())
    }

    /// Send outputs from the executing tasks back via the `Writer`
    pub async fn send_outputs(self: Arc<Self>) {
    }
}
