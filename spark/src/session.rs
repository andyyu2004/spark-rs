use crate::config::{MasterUrl, SparkConfig};
use crate::{SparkContext, SparkResult};
use std::sync::Arc;

const DEFAULT_PORT: u16 = 8077;

pub struct SparkSession {
    scx: Arc<SparkContext>,
}

impl SparkSession {
    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::default()
    }

    pub fn scx(&self) -> Arc<SparkContext> {
        Arc::clone(&self.scx)
    }
}

#[derive(Default)]
pub struct SparkSessionBuilder {
    config: SparkConfig,
}

impl SparkSessionBuilder {
    pub async fn create(self) -> SparkResult<SparkSession> {
        Ok(SparkSession { scx: SparkContext::new(self.config).await? })
    }

    pub fn master_url(mut self, master_url: MasterUrl) -> Self {
        self.config.master_url = master_url;
        self
    }
}
