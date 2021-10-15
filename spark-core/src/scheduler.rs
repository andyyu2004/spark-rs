use crate::rdd::Rdd;
use crate::*;

pub struct DagScheduler {}

pub struct TaskScheduler {}

impl TaskScheduler {
    pub fn default_parallelism(&self) -> usize {
        num_cpus::get()
    }
}

pub enum SchedulerBackend {
    Local(LocalScheduler),
    Distributed(DistributedScheduler),
}

impl SchedulerBackend {
}

pub struct LocalScheduler {
    cores: usize,
}

impl LocalScheduler {
    pub fn new(cores: usize) -> Self {
        Self { cores }
    }

    pub fn run<T>(&self, rdd: impl Rdd<Output = T>) -> SparkResult<Vec<T>> {
        todo!()
    }
}

pub struct DistributedScheduler;
impl DagScheduler {
    pub(crate) fn run<T, U>(
        &self,
        rdd: impl Rdd<Output = T>,
        partitions: Partitions,
        f: impl Fn(crate::TaskContext, SparkIterator<T>) -> U,
        handler: impl Fn(PartitionIndex, U),
    ) -> Result<(), SparkError> {
        todo!()
    }
}
