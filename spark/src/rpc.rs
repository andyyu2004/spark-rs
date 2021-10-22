use super::*;
use crate::broadcast::BroadcastId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV4;
use std::sync::Arc;
use tarpc::context::Context;

pub type RpcResult<T> = Result<T, RpcError>;

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcError {
    Todo(String),
}

pub async fn create_client(server_addr: SocketAddrV4) -> SparkResult<SparkRpcClient> {
    let mk_codec = tokio_serde::formats::Bincode::default;
    let transport = tarpc::serde_transport::tcp::connect(server_addr, mk_codec).await?;
    Ok(SparkRpcClient::new(tarpc::client::Config::default(), transport).spawn())
}

#[tarpc::service]
pub trait SparkRpc {
    async fn get_broadcasted_item(id: BroadcastId) -> RpcResult<Vec<u8>>;
}

#[tarpc::server]
impl SparkRpc for Arc<SparkContext> {
    async fn get_broadcasted_item(self, _context: Context, id: BroadcastId) -> RpcResult<Vec<u8>> {
        let bcx = self.broadcast_context();
        let bytes =
            bcx.get_broadcasted_bytes(id).await.map_err(|err| RpcError::Todo(err.to_string()))?;
        Ok(bytes.to_vec())
    }
}
