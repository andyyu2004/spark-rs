use anyhow::{Error, Result};
use spark::executor::{Executor, LocalExecutorBackend};
use std::str::FromStr;

use clap::Parser;

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
async fn main() {
    let opts = Opts::parse();
    let backend = match opts.master_url {
        MasterUrl::Local { threads: num_threads } =>
            Box::new(LocalExecutorBackend::new(num_threads)),
    };

    let executor = Executor::new(backend);
    if let Err(err) = executor.execute(tokio::io::stdin(), tokio::io::stdout()).await {
        todo!("handle error `{}`", err)
    }
}
