use super::*;
use crate::config::DistributedUrl;
use crate::executor::{Executor, LocalExecutorBackend};
use tokio::io::{AsyncWriteExt, DuplexStream, ReadHalf};
use tokio::sync::Mutex;

pub struct DistributedTaskSchedulerBackend {
    executor: Arc<Executor>,
    executor_reader: ReadHalf<DuplexStream>,
    stream: Mutex<DuplexStream>,
}

impl DistributedTaskSchedulerBackend {
    pub fn new(url: DistributedUrl) -> Self {
        let backend = match url {
            DistributedUrl::Local { num_threads } =>
                Box::new(LocalExecutorBackend::new(num_threads)),
        };
        let (stream, executor_stream) = tokio::io::duplex(8192);
        let (executor_reader, executor_writer) = tokio::io::split(executor_stream);
        let executor = Executor::new(backend, executor_writer);
        Self { stream: Mutex::new(stream), executor, executor_reader }
    }
}

#[async_trait]
impl TaskSchedulerBackend for DistributedTaskSchedulerBackend {
    async fn run_task(&self, task: Task) -> SparkResult<TaskHandle> {
        let mut buf = vec![];
        let size = bincode::serialized_size(&task).unwrap();
        buf.extend(size.to_be_bytes());
        bincode::serialize_into(&mut buf, &task).unwrap();
        self.stream.lock().await.write_all(&buf).await?;
        todo!()
    }
}
