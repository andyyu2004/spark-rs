use spark_core::scheduler::TaskOutput;

use super::*;

#[async_trait]
pub trait ExecutorBackend: Send + Sync {
    async fn execute_task(&self, task: Task) -> ExecutorResult<()>;
}

pub struct ExecutionOutput {
    task_output: TaskOutput,
}
