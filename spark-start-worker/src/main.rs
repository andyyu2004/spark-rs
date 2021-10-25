use clap::Parser;
use spark::SparkResult;
use std::net::SocketAddr;

#[derive(Parser)]
struct Opts {
    #[clap(long, default_value = "127.0.0.1:8077")]
    master_url: SocketAddr,
}

#[tokio::main]
async fn main() -> SparkResult<()> {
    let opts = Opts::parse();
    let client = spark::cluster::create_rpc_client(opts.master_url).await?;
    let worker_id = client.connect_worker(tarpc::context::current()).await?;
    dbg!(worker_id);
    Ok(())
}
