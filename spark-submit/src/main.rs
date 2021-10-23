use clap::Parser;
use spark::config::{DriverUrl, SparkConfig, TaskSchedulerConfig};
use spark::executor::{Executor, LocalExecutorBackend};
use spark::SparkResult;
use std::sync::Arc;

#[derive(Parser)]
struct Opts {
    driver_url: DriverUrl,
    task_scheduler: TaskSchedulerConfig,
}

#[tokio::main]
async fn main() -> SparkResult<()> {
    let Opts { driver_url, task_scheduler } = Opts::parse();
    let config = SparkConfig { driver_url, task_scheduler };

    let backend = Arc::new(LocalExecutorBackend::new(num_cpus::get()));
    let executor = Executor::new(Arc::new(config), backend).await?;
    if let Err(err) = executor.execute(tokio::io::stdin(), tokio::io::stdout()).await {
        todo!("handle error `{}`", err)
    }
    Ok(())
}
