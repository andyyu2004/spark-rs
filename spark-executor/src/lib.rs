mod local_executor;

use anyhow::anyhow;
use async_bincode::AsyncBincodeReader;
use async_trait::async_trait;
use futures::TryStreamExt;
use spark_core::scheduler::Task;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};

pub use local_executor::LocalExecutor;

pub type ExecutorResult<T> = anyhow::Result<T>;

pub struct Executor {
    backend: Box<dyn ExecutorBackend>,
    writer: Pin<Box<dyn AsyncWrite>>,
}

impl Executor {
    pub fn new(backend: Box<dyn ExecutorBackend>, writer: impl AsyncWrite + 'static) -> Self {
        Self { backend, writer: Box::pin(writer) }
    }

    pub async fn execute(self, reader: impl AsyncRead + Unpin) -> ExecutorResult<()> {
        let stream = AsyncBincodeReader::<_, Task>::from(reader);
        let results = stream
            .map_err(|err| anyhow!(err))
            .try_for_each_concurrent(None, |task| async { self.backend.execute_task(task).await })
            .await;
        Ok(())
    }
}

#[async_trait]
pub trait ExecutorBackend {
    async fn execute_task(&self, task: Task) -> ExecutorResult<()>;
}
