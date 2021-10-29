use crate::cluster::DEFAULT_MASTER_PORT;
use crate::SparkError;
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;
use url::Url;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SparkConfig {
    pub master_url: MasterUrl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MasterUrl {
    /// Run the tasks in the current process.
    /// This maybe more efficient than distributed local as we don't have to do serialization and deserialization
    Local {
        addr: SocketAddr,
        num_threads: usize,
    },
    Cluster {
        url: ClusterUrl,
    },
}

impl Default for MasterUrl {
    fn default() -> Self {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, DEFAULT_MASTER_PORT));
        Self::Local { addr, num_threads: num_cpus::get() }
    }
}

impl FromStr for MasterUrl {
    type Err = SparkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(s)?;
        let cfg = match url.scheme() {
            "" => MasterUrl::default(),
            scheme => {
                let url = match scheme {
                    "k8s" => ClusterUrl::Kube { url },
                    _ => bail!("unknown master url scheme: {}", scheme),
                };
                MasterUrl::Cluster { url }
            }
        };
        Ok(cfg)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterUrl {
    /// Run the tasks on a new process on the current machine.
    /// This differs from [`MasterUrl::Local`] which runs in the current process.
    /// Not sure what the benefit of this over [`MasterUrl::Local`] is,
    /// it's currently used for testing the distributed framework.
    Standalone {
        num_threads: usize,
    },
    Kube {
        url: Url,
    },
}
