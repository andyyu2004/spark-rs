use crate::config::SparkConfig;
use crate::scheduler::LocalScheduler;
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
        // let scheduler = match self.master {
        //     Master::Local { cores } => LocalScheduler::new(cores),
        // };
        SparkSession { scx: SparkContext::new(SparkConfig::default()) }
    }
}
