use crate::config::{SparkConfig, TaskSchedulerConfig};
use crate::{SparkContext, SparkResult};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

pub const DEFAULT_DRIVER_PORT: u16 = 8077;

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

pub struct SparkSessionBuilder {
    pub task_scheduler: TaskSchedulerConfig,
    pub driver_addr: SocketAddr,
}

impl Default for SparkSessionBuilder {
    fn default() -> Self {
        Self {
            task_scheduler: Default::default(),
            driver_addr: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                DEFAULT_DRIVER_PORT,
            )),
        }
    }
}

impl SparkSessionBuilder {
    pub async fn create(self) -> SparkResult<SparkSession> {
        let Self { task_scheduler, driver_addr } = self;
        let config = SparkConfig { task_scheduler, driver_addr };
        let scx = SparkContext::new(Arc::new(config)).await?;
        Ok(SparkSession { scx })
    }

    pub fn master_url(mut self, master_url: TaskSchedulerConfig) -> Self {
        self.task_scheduler = master_url;
        self
    }
}
