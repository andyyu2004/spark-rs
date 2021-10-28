use super::*;
use std::sync::Arc;

pub struct StandaloneClusterScheduler {}

impl ClusterSchedulerBackend for StandaloneClusterScheduler {
}

impl StandaloneClusterScheduler {
    pub async fn new() -> SparkResult<Arc<Self>> {
        let scheduler = Self {};
        Ok(Arc::new(scheduler))
    }
}
