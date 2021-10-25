mod standalone;

use super::*;
use std::sync::Arc;
use std::time::Duration;
use tarpc::context::Context;
use tokio::net::ToSocketAddrs;

pub use standalone::*;

pub const DEFAULT_MASTER_PORT: u16 = 8077;

newtype_index!(WorkerId);

pub async fn create_rpc_client(
    server_addr: impl ToSocketAddrs,
) -> SparkResult<Arc<ClusterSchedulerRpcClient>> {
    let mk_codec = tokio_serde::formats::Bincode::default;
    let connect = tarpc::serde_transport::tcp::connect(server_addr, mk_codec);
    let transport = tokio::time::timeout(Duration::from_secs(5), connect)
        .await
        .map_err(|_| eyre!("connection to master timed out"))??;
    let client = ClusterSchedulerRpcClient::new(tarpc::client::Config::default(), transport);
    Ok(Arc::new(client.spawn()))
}

pub struct ClusterScheduler {
    backend: Arc<dyn ClusterSchedulerBackend>,
}

impl ClusterScheduler {
    pub fn new(backend: Arc<dyn ClusterSchedulerBackend>) -> Self {
        Self { backend }
    }
}

pub trait ClusterSchedulerBackend: Send + Sync + 'static {
    fn next_worker_id(&self) -> WorkerId;
}

#[tarpc::service]
pub trait ClusterSchedulerRpc {
    async fn connect_worker() -> WorkerId;
}

#[tarpc::server]
impl<S: ClusterSchedulerBackend> ClusterSchedulerRpc for Arc<S> {
    #[instrument(skip(self, _cx))]
    async fn connect_worker(self, _cx: Context) -> WorkerId {
        let worker_id = self.next_worker_id();
        trace!(next_worker_id = ?worker_id);
        worker_id
    }
}
