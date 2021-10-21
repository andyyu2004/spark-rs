use crate::config::{MasterUrl, SparkConfig};
use crate::SparkContext;
use std::sync::Arc;

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
    pub fn create(self) -> SparkSession {
        SparkSession { scx: SparkContext::new(self.config) }
    }

    pub fn master_url(mut self, master_url: MasterUrl) -> Self {
        self.config.master_url = master_url;
        self
    }
}
