use super::*;
use crate::config::DistributedUrl;

pub struct DistributedTaskSchedulerBackend;

impl DistributedTaskSchedulerBackend {
    pub fn new(url: DistributedUrl) -> Self {
        match url {
            DistributedUrl::Local => todo!(),
        }
    }
}

impl TaskSchedulerBackend for DistributedTaskSchedulerBackend {
    fn run_task(&self, task: Task) -> TaskHandle {
        let serialized_task = bincode::serialize(&task);
        todo!()
    }
}
