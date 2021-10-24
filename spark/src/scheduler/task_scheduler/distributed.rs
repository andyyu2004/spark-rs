use super::*;
use async_bincode::AsyncBincodeReader;
use bincode::Options;
use dashmap::DashMap;
use futures::TryStreamExt;
use rayon::ThreadPoolBuilder;
use std::lazy::SyncOnceCell;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::Once;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};

pub const TASK_LIMIT: usize = 100;

pub struct DistributedTaskSchedulerBackend {
    driver_addr: SocketAddr,
    txs: DashMap<TaskId, TaskSender>,
    task_dispatcher: SyncOnceCell<tokio::sync::mpsc::Sender<Task>>,
}

struct ExecutorProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

impl DistributedTaskSchedulerBackend {
    pub fn new(driver_addr: SocketAddr) -> Self {
        Self { driver_addr, txs: Default::default(), task_dispatcher: Default::default() }
    }

    async fn dispatch_tasks(
        mut task_receiver: tokio::sync::mpsc::Receiver<Task>,
        mut writer: impl AsyncWrite + Send + Unpin + 'static,
    ) -> SparkResult<()> {
        // Use a thread pool to do the serialization as it's pretty slow and compute heavy and blocks the executor
        let pool = ThreadPoolBuilder::new().build().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            while let Some(serialized_task) = rx.recv().await {
                trace!("start writing serialized task to executor");
                writer.write_all(&serialized_task).await?;
                trace!("finish writing serialized task");
            }
            Ok::<_, SparkError>(())
        });

        while let Some(task) = task_receiver.recv().await {
            trace!("start serializing task `{:?}`", task.id());
            let tx = tx.clone();
            pool.spawn(move || {
                // Reserve 4 bytes for the size at the front so we don't have to do any shifting or copying
                let mut buffer = vec![0; 4];
                let c = bincode::options().with_limit(u32::max_value() as u64);
                c.serialize_into(&mut buffer, &task).expect("serialization into vec failed");
                // Don't want to use bincode to calculate the serialized size as that unncessary extra work
                let serialized_size = buffer.len() as u32 - 4;
                buffer[0..4].copy_from_slice(&serialized_size.to_be_bytes());
                trace!("finish serializing task `{:?}`", task.id());
                tx.send(buffer).expect("dispatch receiver hung up unexpectedly");
            })
        }
        Ok::<_, SparkError>(())
    }

    fn spawn_executor(&self) -> SparkResult<ExecutorProcess> {
        let mut child = tokio::process::Command::new("spark-submit")
            .arg("--driver-addr")
            .arg(&self.driver_addr.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        Ok(ExecutorProcess {
            stdin: child.stdin.take().unwrap(),
            stdout: child.stdout.take().unwrap(),
            stderr: child.stderr.take().unwrap(),
            child,
        })
    }

    #[instrument(skip(self))]
    async fn start(self: Arc<Self>) -> SparkResult<()> {
        trace!("start distributed task_scheduler backend");
        let (task_dispatcher, task_receiver) = tokio::sync::mpsc::channel(TASK_LIMIT);
        let executor = self.spawn_executor()?;

        // The reason for handling the sending of the writer over a separate task is because
        // `send` on a channel sender only requires `&self` while `send` on the `Sink` requires `&mut self`,
        // and so if we were to send on the Sink directly from `run_task` we would need a mutex.
        let dispatcher_handle = tokio::spawn(Self::dispatch_tasks(task_receiver, executor.stdin));
        self.task_dispatcher.set(task_dispatcher).unwrap();

        let stream = AsyncBincodeReader::<_, TaskOutput>::from(executor.stdout);
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
