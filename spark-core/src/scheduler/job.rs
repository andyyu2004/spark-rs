use super::*;
use std::sync::Arc;

newtype_index!(JobId);

pub struct JobHandle<T> {
    scheduler: Arc<DagScheduler>,
    pd: std::marker::PhantomData<T>,
}

impl<T> JobHandle<T> {
    pub fn new(scheduler: Arc<DagScheduler>, job_id: JobId, handler: impl HandlerFn<T>) -> Self {
        Self { scheduler, pd: std::marker::PhantomData }
    }
}
