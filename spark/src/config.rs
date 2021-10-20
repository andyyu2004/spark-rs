#[derive(Debug, Default)]
pub struct SparkConfig {
    pub master_url: MasterUrl,
}

#[derive(Debug)]
pub enum MasterUrl {
    /// Run the tasks in the current process
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
    /// This differs from `MasterUrl::Local`
    Local,
}
