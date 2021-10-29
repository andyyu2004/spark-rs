use crate::cluster::DEFAULT_MASTER_PORT;
use crate::config::{MasterUrl, SparkConfig};
use crate::{SparkContext, SparkResult};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

pub const DEFAULT_DRIVER_PORT: u16 = 8078;

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

#[derive(Debug, Clone)]
pub struct SparkSessionBuilder {
    pub master_url: MasterUrl,
}

impl Default for SparkSessionBuilder {
    fn default() -> Self {
        Self { master_url: Default::default() }
    }
}

impl SparkSessionBuilder {
    pub async fn create(self) -> SparkResult<SparkSession> {
        let Self { master_url } = self;
        let config = SparkConfig { master_url };
        let scx = SparkContext::new(Arc::new(config)).await?;
        Ok(SparkSession { scx })
    }

    pub fn master_url(mut self, master_url: MasterUrl) -> Self {
        self.master_url = master_url;
        self
    }
}
