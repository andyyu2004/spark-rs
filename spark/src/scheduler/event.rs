use super::*;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub(crate) type SchedulerEventSender = UnboundedSender<SchedulerEvent>;
pub(crate) type SchedulerEventReceiver = UnboundedReceiver<SchedulerEvent>;

#[derive(Debug)]
pub enum SchedulerEvent {
    JobSubmitted(JobSubmittedEvent),
}

#[derive(Debug)]
pub struct JobSubmittedEvent {
    pub(super) rdd: RddRef,
    pub(crate) partitions: Partitions,
    pub(super) job_id: JobId,
}
