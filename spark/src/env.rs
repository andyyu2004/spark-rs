use crate::broadcast::BroadcastContext;
use crate::config::SparkConfig;
use crate::executor::ExecutorId;
use crate::rpc::{self, SparkRpcClient};
use crate::SparkResult;
use std::sync::Arc;
use tokio::net::ToSocketAddrs;
use tokio::sync::OnceCell;

static SPARK_ENV: OnceCell<Arc<SparkEnv>> = OnceCell::const_new();

/// The common components of all spark nodes (worker and master)
/// Each component has a `<component>` method which return the component
/// given a reference `SparkEnv` as `&self`. There is also a `get_<component` variant
/// which accesses the environment through a global variable instead.
/// Prefer supplying a reference to the env if possible.
pub struct SparkEnv {
    config: Arc<SparkConfig>,
    executor_id: ExecutorId,
    broadcast_context: Arc<BroadcastContext>,
    rpc_client: OnceCell<Arc<SparkRpcClient>>,
}

impl SparkEnv {
    /// Get a reference to the `SparkEnv`.
    /// Prefer accessing this through the [`crate::SparkContext`], but tls can be used when necessary.
    pub fn get() -> Arc<Self> {
        Arc::clone(SPARK_ENV.get().expect("cannot `get` before `init`"))
    }

    pub fn is_driver(&self) -> bool {
        self.executor_id == ExecutorId::DRIVER
    }

    pub fn driver_addr(&self) -> impl ToSocketAddrs + Clone + '_ {
        &*self.config.driver_url
    }

    pub fn get_broadcast_context() -> Arc<BroadcastContext> {
        Self::get().broadcast_context()
    }

    pub fn broadcast_context(&self) -> Arc<BroadcastContext> {
        Arc::clone(&self.broadcast_context)
    }

    pub async fn get_rpc_client() -> SparkResult<Arc<SparkRpcClient>> {
        Self::get().rpc_client().await
    }

    pub async fn rpc_client(&self) -> SparkResult<Arc<SparkRpcClient>> {
        self.rpc_client
            .get_or_try_init(|| rpc::create_client(&*self.config.driver_url))
            .await
            .map(Arc::clone)
    }

    pub(crate) async fn init_for_driver(
        config: Arc<SparkConfig>,
        mk_bcx: impl FnOnce() -> BroadcastContext,
    ) -> SparkResult<Arc<Self>> {
        let init_env = || async move {
            Arc::new(SparkEnv {
                config,
                executor_id: ExecutorId::DRIVER,
                broadcast_context: Arc::new(mk_bcx()),
                rpc_client: Default::default(),
            })
        };
        Ok(Arc::clone(SPARK_ENV.get_or_init(init_env).await))
    }

    pub(crate) async fn init_for_executor(
        config: Arc<SparkConfig>,
        mk_bcx: impl FnOnce() -> BroadcastContext,
    ) -> SparkResult<Arc<Self>> {
        let init_env = || async move {
            let rpc_client = rpc::create_client(&*config.driver_url).await?;
            let executor_id = rpc_client.alloc_executor_id(tarpc::context::current()).await?;
            Ok(Arc::new(SparkEnv {
                config,
                executor_id,
                rpc_client: OnceCell::from(rpc_client),
                broadcast_context: Arc::new(mk_bcx()),
            }))
        };
        SPARK_ENV.get_or_try_init(init_env).await.map(Arc::clone)
    }
}
