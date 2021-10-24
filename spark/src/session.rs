use crate::config::{DriverUrl, SparkConfig, TaskSchedulerConfig};
use crate::{SparkContext, SparkResult};
use std::sync::Arc;

pub const DEFAULT_PORT: u16 = 8077;

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
    pub task_scheduler: TaskSchedulerConfig,
    pub driver_url: DriverUrl,
}

impl SparkSessionBuilder {
    pub async fn create(self) -> SparkResult<SparkSession> {
        let Self { task_scheduler, driver_url } = self;
        let config = SparkConfig { task_scheduler, driver_url };
        let scx = SparkContext::new(Arc::new(config)).await?;
        Ok(SparkSession { scx })
    }

    pub fn master_url(mut self, master_url: TaskSchedulerConfig) -> Self {
        self.task_scheduler = master_url;
        self
    }
}
