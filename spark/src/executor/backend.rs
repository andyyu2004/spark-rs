use super::*;

#[async_trait]
pub trait ExecutorBackend: Send + Sync {
    async fn execute_task(&self, task: Task) -> ExecutorResult<()>;
    async fn task_output(&self) -> Option<TaskOutput>;
}

pub struct ExecutionOutput {
    task_output: TaskOutput,
}
