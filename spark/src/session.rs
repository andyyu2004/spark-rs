use crate::config::SparkConfig;
use crate::SparkContext;
use std::sync::Arc;

pub struct SparkSession {
    scx: Arc<SparkContext>,
}

impl SparkSession {
    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::default()
    }

    pub fn spark_context(&self) -> Arc<SparkContext> {
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
}
