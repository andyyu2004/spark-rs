use spark::cluster::{ClusterScheduler, StandaloneClusterScheduler, DEFAULT_MASTER_PORT};
use spark::SparkResult;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

#[tokio::main]
async fn main() -> SparkResult<()> {
    tracing_subscriber::fmt::init();
    let backend = StandaloneClusterScheduler::new().await?;
    let scheduler = ClusterScheduler::new(backend);
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, DEFAULT_MASTER_PORT));
    let (bind_addr, handle) = scheduler.bind_rpc(addr).await?;
    print!("spark://{}", bind_addr);
    std::io::stdout().flush()?;
    handle.await?;
    Ok(())
}
