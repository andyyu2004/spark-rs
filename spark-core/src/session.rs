use crate::config::SparkConfig;
use crate::SparkContext;

pub struct SparkSession {
    scx: SparkContext,
}

impl SparkSession {
    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::default()
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
