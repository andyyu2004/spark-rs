mod kubernetes;
mod standalone;

use super::*;
use futures::StreamExt;
use indexed_vec::Idx;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tarpc::context::Context;
use tarpc::server::incoming::Incoming;
use tarpc::server::{BaseChannel, Channel};
use tokio::net::ToSocketAddrs;
use tokio::task::JoinHandle;

pub use kubernetes::*;
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
    worker_idx: AtomicUsize,
    backend: Arc<dyn ClusterSchedulerBackend>,
}

impl ClusterScheduler {
    pub fn new(backend: Arc<dyn ClusterSchedulerBackend>) -> Arc<Self> {
        Arc::new(Self { backend, worker_idx: Default::default() })
    }

    fn next_worker_id(&self) -> WorkerId {
        WorkerId::new(self.worker_idx.fetch_add(1, Ordering::SeqCst))
    }

    pub async fn bind_rpc(
        self: Arc<Self>,
        config_addr: SocketAddr,
    ) -> SparkResult<(SocketAddr, JoinHandle<()>)> {
        let mk_codec = tokio_serde::formats::Bincode::default;
        let mut bind_addr = config_addr;
        let mut listener = loop {
            if let Ok(listener) = tarpc::serde_transport::tcp::listen(&bind_addr, mk_codec).await {
                break listener;
            }

            let port = bind_addr.port();
            if port == u16::MAX {
                bail!("failed to bind to any port from `{}` onwards", config_addr);
            }
            bind_addr.set_port(1 + port);
        };

        info!("rpc server bound to `{}`", bind_addr);

        let handle = tokio::spawn(async move {
            listener.config_mut().max_frame_length(usize::MAX);
            listener
                // Ignore tcp accept errors
                .filter_map(async move |r| r.ok())
                .map(BaseChannel::with_defaults)
                .max_channels_per_key(2, |t| t.transport().peer_addr().unwrap().ip())
                .map(|channel| channel.execute(self.clone().serve()))
                .buffer_unordered(10)
                .for_each(|()| async {})
                .await;
            panic!("rpc service finished");
        });

        Ok((bind_addr, handle))
    }
}

pub trait ClusterSchedulerBackend: Send + Sync + 'static {}

#[tarpc::service]
pub trait ClusterSchedulerRpc {
    async fn connect_worker() -> WorkerId;
}

#[tarpc::server]
impl ClusterSchedulerRpc for Arc<ClusterScheduler> {
    #[instrument(skip(self, _cx))]
    async fn connect_worker(self, _cx: Context) -> WorkerId {
        let worker_id = self.next_worker_id();
        trace!(next_worker_id = ?worker_id);
        worker_id
    }
}
