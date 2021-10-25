use super::*;
use crate::broadcast::BroadcastId;
use crate::executor::ExecutorId;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tarpc::context::Context;
use tarpc::server::incoming::Incoming;
use tarpc::server::{BaseChannel, Channel};
use thiserror::Error;
use tokio::net::ToSocketAddrs;
use tokio::task::JoinHandle;

pub type SparkRpcResult<T> = Result<T, SparkRpcError>;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum SparkRpcError {
    #[error("{0}")]
    Todo(String),
}

pub async fn create_client(server_addr: impl ToSocketAddrs) -> SparkResult<Arc<SparkRpcClient>> {
    let mk_codec = tokio_serde::formats::Bincode::default;
    let connect = tarpc::serde_transport::tcp::connect(server_addr, mk_codec);
    let transport = tokio::time::timeout(Duration::from_secs(5), connect)
        .await
        .map_err(|_| eyre!("connection to master timed out"))??;
    Ok(Arc::new(SparkRpcClient::new(tarpc::client::Config::default(), transport).spawn()))
}

#[tarpc::service]
pub trait SparkRpc {
    async fn alloc_executor_id() -> ExecutorId;
    async fn get_broadcasted_item(id: BroadcastId) -> SparkRpcResult<Vec<u8>>;
}

#[derive(Clone)]
pub struct SparkRpcServer {
    rcx: Arc<RpcContext>,
}

impl SparkRpcServer {
    pub fn new(rcx: Arc<RpcContext>) -> Self {
        Self { rcx }
    }

    fn env(&self) -> Arc<SparkEnv> {
        SparkEnv::get()
    }

    /// Starts the server attempting to bind to `config_addr`.
    /// If it is taken it will try consecutive ports (until u16::MAX for now)
    /// and returns the port that was bound.
    pub async fn bind(self, config_addr: &SocketAddr) -> SparkResult<(SocketAddr, JoinHandle<()>)> {
        let mk_codec = tokio_serde::formats::Bincode::default;
        let mut bind_addr = *config_addr;
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

#[tarpc::server]
impl SparkRpc for SparkRpcServer {
    async fn alloc_executor_id(self, _context: Context) -> ExecutorId {
        self.rcx.next_executor_id()
    }

    async fn get_broadcasted_item(
        self,
        _context: Context,
        id: BroadcastId,
    ) -> SparkRpcResult<Vec<u8>> {
        let bcx = self.env().broadcast_context();
        let bytes = bcx
            .get_broadcasted_bytes(id)
            .await
            .map_err(|err| SparkRpcError::Todo(err.to_string()))?;
        Ok(bytes.to_vec())
    }
}

#[cfg(test)]
mod tests;
