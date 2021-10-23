use crate::SparkError;
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs};
use std::ops::Deref;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize)]
pub struct SparkConfig {
    pub task_scheduler: TaskSchedulerConfig,
    pub driver_url: DriverUrl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverUrl(SocketAddr);

impl FromStr for DriverUrl {
    type Err = <SocketAddr as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SocketAddr::from_str(s).map(Self)
    }
}

impl ToSocketAddrs for DriverUrl {
    type Iter = <SocketAddr as ToSocketAddrs>::Iter;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        self.0.to_socket_addrs()
    }
}

impl Deref for DriverUrl {
    type Target = SocketAddr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for DriverUrl {
    fn default() -> Self {
        Self(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8077)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TaskSchedulerConfig {
    /// Run the tasks in the current process.
    /// This maybe more efficient than distributed local as we don't have to do as '
    /// much serialization and deserialization
    Local {
        num_threads: usize,
    },
    Distributed {
        url: DistributedUrl,
    },
}

impl Default for TaskSchedulerConfig {
    fn default() -> Self {
        Self::Local { num_threads: num_cpus::get() }
    }
}

impl FromStr for TaskSchedulerConfig {
    type Err = SparkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = if s == "local" {
            TaskSchedulerConfig::Local { num_threads: num_cpus::get() }
        } else {
            TaskSchedulerConfig::Distributed {
                url: DistributedUrl::Local { num_threads: num_cpus::get() },
            }
        };
        Ok(url)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DistributedUrl {
    /// Run the tasks on a new process on the current machine
    /// This differs from [`MasterUrl::Local`]
    /// Not sure what the benefit of this over [`MasterUrl::Local`] is,
    /// it's currently used for testing the distributed framework.
    Local { num_threads: usize },
}
