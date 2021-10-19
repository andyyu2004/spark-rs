use std::sync::Arc;

use crate::config::SparkConfig;
use crate::SparkContext;

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
    master: Master,
}

pub enum Master {
    Local { cores: usize },
}

impl Default for Master {
    fn default() -> Self {
        Self::Local { cores: num_cpus::get() }
    }
}

impl SparkSessionBuilder {
    pub fn create(self) -> SparkSession {
        SparkSession { scx: SparkContext::new(SparkConfig::default()) }
    }
}
