use super::*;

pub struct DistributedTaskSchedulerBackend;

impl TaskSchedulerBackend for DistributedTaskSchedulerBackend {
    fn run_task(&self, task: Task) -> TaskHandle {
        let serialized_task = bincode::serialize(&task);
        todo!()
    }
}
