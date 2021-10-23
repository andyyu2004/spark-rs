use super::*;
use crate::broadcast::BroadcastId;
use crate::executor::ExecutorId;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
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
        .map_err(|_| anyhow!("connection to master timed out"))??;
    Ok(Arc::new(SparkRpcClient::new(tarpc::client::Config::default(), transport).spawn()))
}

#[tarpc::service]
pub trait SparkRpc {
    async fn alloc_executor_id() -> ExecutorId;
    async fn get_broadcasted_item(id: BroadcastId) -> SparkRpcResult<Vec<u8>>;
}

#[derive(Clone)]
pub struct SparkRpcServer {
    scx: Arc<SparkContext>,
}

impl SparkRpcServer {
    pub fn new(scx: Arc<SparkContext>) -> Self {
        Self { scx }
    }

    pub async fn start(self) -> SparkResult<JoinHandle<()>> {
        let mk_codec = tokio_serde::formats::Bincode::default;
        let mut listener =
            tarpc::serde_transport::tcp::listen(self.scx.env().driver_addr(), mk_codec).await?;
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
        Ok(handle)
    }
}

#[tarpc::server]
impl SparkRpc for SparkRpcServer {
    async fn alloc_executor_id(self, _context: Context) -> ExecutorId {
        self.scx.next_executor_id()
    }

    async fn get_broadcasted_item(
        self,
        _context: Context,
        id: BroadcastId,
    ) -> SparkRpcResult<Vec<u8>> {
        let bcx = self.scx.broadcast_context();
        let bytes = bcx
            .get_broadcasted_bytes(id)
            .await
            .map_err(|err| SparkRpcError::Todo(err.to_string()))?;
        Ok(bytes.to_vec())
    }
}

#[cfg(test)]
mod tests;
