#[derive(Debug, Default)]
pub struct SparkConfig {
    pub master_url: MasterUrl,
}

#[derive(Debug)]
pub enum MasterUrl {
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

impl Default for MasterUrl {
    fn default() -> Self {
        Self::Local { num_threads: num_cpus::get() }
    }
}

#[derive(Debug)]
pub enum DistributedUrl {
    /// Run the tasks on a new process on the current machine
    /// This differs from [`MasterUrl::Local`]
    /// Not sure what the benefit of this over [`MasterUrl::Local`] is,
    /// it's currently used for testing the distributed framework.
    Local { num_threads: usize },
}
