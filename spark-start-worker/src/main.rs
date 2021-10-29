use clap::Parser;
use spark::config::MasterUrl;
use spark::SparkResult;

#[derive(Parser)]
struct Opts {
    #[clap(long, default_value = "127.0.0.1:8077")]
    master_url: MasterUrl,
}

#[tokio::main]
async fn main() -> SparkResult<()> {
    let opts = Opts::parse();
    let addr = match &opts.master_url {
        MasterUrl::Local { addr, .. } => addr,
        MasterUrl::Cluster { url: _ } => todo!(),
    };
    let client = spark::cluster::create_rpc_client(addr).await?;
    let worker_id = client.connect_worker(tarpc::context::current()).await?;
    dbg!(worker_id);
    Ok(())
}
