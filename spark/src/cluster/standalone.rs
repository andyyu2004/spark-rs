use super::*;
use futures::StreamExt;
use indexed_vec::Idx;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tarpc::server::incoming::Incoming;
use tarpc::server::{BaseChannel, Channel};
use tokio::task::JoinHandle;

pub struct StandaloneClusterScheduler {
    worker_idx: AtomicUsize,
}

impl ClusterSchedulerBackend for StandaloneClusterScheduler {
    fn next_worker_id(&self) -> WorkerId {
        WorkerId::new(self.worker_idx.fetch_add(1, Ordering::SeqCst))
    }
}

impl StandaloneClusterScheduler {
    pub async fn new() -> SparkResult<Arc<Self>> {
        let scheduler = Self { worker_idx: Default::default() };
        Ok(Arc::new(scheduler))
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
