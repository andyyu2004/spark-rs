use crate::broadcast::BroadcastContext;
use std::lazy::SyncOnceCell;
use std::sync::Arc;

thread_local! {
    static SPARK_ENV: SyncOnceCell<Arc<SparkEnv>> = SyncOnceCell::new();
}

/// The common components of all spark nodes (worker and master)
pub struct SparkEnv {
    broadcast_context: Arc<BroadcastContext>,
}

impl SparkEnv {
    /// Get a reference to the `SparkEnv`.
    /// Prefer accessing this through the [`crate::SparkContext`], but tls can be used when necessary.
    pub fn get() -> Arc<Self> {
        SPARK_ENV.with(|e| Arc::clone(e.get().expect("cannot `get` before `init`")))
    }

    pub fn get_broadcast_context() -> Arc<BroadcastContext> {
        Self::get().broadcast_context()
    }

    pub fn broadcast_context(&self) -> Arc<BroadcastContext> {
        Arc::clone(&self.broadcast_context)
    }

    pub(crate) fn init(broadcaster: BroadcastContext) -> Arc<Self> {
        let init_env = || Arc::new(SparkEnv { broadcast_context: Arc::new(broadcaster) });
        SPARK_ENV.with(|cell| Arc::clone(cell.get_or_init(init_env)))
    }
}
