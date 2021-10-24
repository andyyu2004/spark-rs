use clap::Parser;
use spark::executor::{Executor, LocalExecutorBackend};
use spark::SparkResult;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Parser)]
struct Opts {
    #[clap(long)]
    driver_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> SparkResult<()> {
    let Opts { driver_addr } = Opts::parse();

    let backend = Arc::new(LocalExecutorBackend::new(num_cpus::get()));
    let executor = Executor::new(driver_addr, backend).await?;
    if let Err(err) = executor.execute(tokio::io::stdin(), tokio::io::stdout()).await {
        todo!("handle error `{}`", err)
    }
    Ok(())
}
