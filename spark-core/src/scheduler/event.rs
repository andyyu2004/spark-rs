use super::*;
use crate::rdd::RddRef;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub(crate) type SchedulerEventSender<T> = UnboundedSender<SchedulerEvent<T>>;
pub(crate) type SchedulerEventReceiver<T> = UnboundedReceiver<SchedulerEvent<T>>;

pub enum SchedulerEvent<T> {
    JobSubmitted { job_id: JobId, rdd: RddRef<T> },
}
