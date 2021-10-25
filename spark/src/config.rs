use crate::SparkError;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize)]
pub struct SparkConfig {
    pub task_scheduler: TaskSchedulerConfig,
    pub driver_addr: SocketAddr,
    pub master_addr: SocketAddr,
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
