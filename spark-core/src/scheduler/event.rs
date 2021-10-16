use super::*;
use crate::rdd::TypedRddRef;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub(crate) type SchedulerEventSender<T, U> = UnboundedSender<SchedulerEvent<T, U>>;
pub(crate) type SchedulerEventReceiver<T, U> = UnboundedReceiver<SchedulerEvent<T, U>>;

pub enum SchedulerEvent<T, U> {
    JobSubmitted(JobSubmittedEvent<T, U>),
}

pub struct JobSubmittedEvent<T, U> {
    pub(super) rdd: TypedRddRef<T>,
    pub(crate) partitions: std::ops::Range<usize>,
    pub(crate) mapper: PartitionMapperRef<T, U>,
    pub(super) job_id: JobId,
}
