use clap::Parser;
use spark::config::MasterUrl;
use spark::executor::{Executor, LocalExecutorBackend};
use spark::SparkResult;
use std::sync::Arc;

#[macro_use]
extern crate tracing;

#[derive(Parser)]
struct Opts {
    master_url: MasterUrl,
}

#[tokio::main]
async fn main() -> SparkResult<()> {
    tracing_subscriber::fmt::init();
    let Opts { master_url } = Opts::parse();

    let driver_addr = todo!();
    info!("spark-submit");
    let backend = Arc::new(LocalExecutorBackend::new(num_cpus::get()));
    let executor = Executor::new(driver_addr, backend).await?;
    if let Err(err) = executor.execute(tokio::io::stdin(), tokio::io::stdout()).await {
        todo!("handle error `{}`", err)
    }
    Ok(())
}
