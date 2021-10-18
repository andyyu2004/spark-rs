pub struct TaskScheduler {
    backend: Box<dyn TaskSchedulerBackend>,
}

impl TaskScheduler {
    pub fn new(backend: Box<dyn TaskSchedulerBackend>) -> Self {
        Self { backend }
    }

    pub fn default_parallelism(&self) -> usize {
        num_cpus::get()
    }
}

pub trait TaskSchedulerBackend: Send + Sync {}

pub struct LocalSchedulerBackend {}

impl LocalSchedulerBackend {
    pub fn new() -> Self {
        Self {}
    }
}

impl TaskSchedulerBackend for LocalSchedulerBackend {
}
