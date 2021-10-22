use anyhow::{Error, Result};
use clap::Parser;
use spark::executor::{Executor, LocalExecutorBackend};
use spark::SparkResult;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Parser)]
struct Opts {
    master_url: MasterUrl,
}

enum MasterUrl {
    Local { threads: usize },
}

impl FromStr for MasterUrl {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url =
            if s == "local" { MasterUrl::Local { threads: num_cpus::get() } } else { todo!() };
        Ok(url)
    }
}

#[tokio::main]
async fn main() -> SparkResult<()> {
    let opts = Opts::parse();
    let backend = match opts.master_url {
        MasterUrl::Local { threads: num_threads } =>
            Arc::new(LocalExecutorBackend::new(num_threads)),
    };

    let server_addr = SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 23423);
    let rpc_client = spark::rpc::create_client(server_addr).await?;
    let executor = Executor::new(backend, rpc_client);
    if let Err(err) = executor.execute(tokio::io::stdin(), tokio::io::stdout()).await {
        todo!("handle error `{}`", err)
    }
    Ok(())
}
