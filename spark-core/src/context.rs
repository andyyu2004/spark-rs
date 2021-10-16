use crate::config::SparkConfig;
use crate::rdd::*;
use crate::scheduler::{DagScheduler, JobOutput, PartitionMapper, TaskScheduler};
use crate::*;
use std::cell::UnsafeCell;
use std::lazy::SyncOnceCell;
use std::ops::Deref;
use std::sync::Arc;

pub struct SparkContext {
    dag_scheduler_cell: SyncOnceCell<Arc<DagScheduler>>,
    task_scheduler_cell: SyncOnceCell<TaskScheduler>,
}

static_assertions::assert_impl_all!(Arc<SparkContext>: Send, Sync);

impl SparkContext {
    pub fn new(config: SparkConfig) -> Self {
        Self { dag_scheduler_cell: Default::default(), task_scheduler_cell: Default::default() }
    }

    pub fn dag_scheduler(&self) -> Arc<DagScheduler> {
        Arc::clone(self.dag_scheduler_cell.get_or_init(|| todo!()))
    }

    pub fn task_scheduler(&self) -> &TaskScheduler {
        self.task_scheduler_cell.get_or_init(|| todo!())
    }

    #[inline(always)]
    pub fn make_rdd<T: Datum>(self: Arc<Self>, data: &[T]) -> impl TypedRdd<Output = T> {
        self.parallelize(data)
    }

    pub fn parallelize<T: Datum>(self: Arc<Self>, data: &[T]) -> impl TypedRdd<Output = T> {
        let num_slices = self.task_scheduler().default_parallelism();
        self.parallelize_with_slices(data, num_slices)
    }

    pub fn parallelize_with_slices<T: Datum>(
        self: Arc<Self>,
        data: &[T],
        num_slices: usize,
    ) -> impl TypedRdd<Output = T> {
        ParallelCollection::new(self, data, num_slices)
    }

    pub async fn run_rdd<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        f: impl PartitionMapper<T, U>,
    ) -> SparkResult<JobOutput<U>>
    where
        T: Datum,
        U: Send + 'static,
    {
        self.dag_scheduler().run(rdd, partitions, f).await
    }

    pub async fn collect_rdd<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        f: impl PartitionMapper<T, U>,
    ) -> SparkResult<Vec<U>>
    where
        T: Datum,
        U: Send + 'static,
    {
        let n = rdd.partitions().len();
        let partition_results = self.run_rdd(rdd, 0..n, f).await;
        todo!()
    }

    pub fn interruptible_iterator<T: Datum>(
        self: Arc<Self>,
        iter: impl Iterator<Item = T> + 'static,
    ) -> Box<dyn Iterator<Item = T>> {
        pub struct InterruptibleIterator<I> {
            iter: I,
        }
        impl<I: Iterator> Iterator for InterruptibleIterator<I> {
            type Item = I::Item;

            fn next(&mut self) -> Option<Self::Item> {
                // TODO kill this if interrupted (does this need a taskcontext or a sparkcontext?)
                self.iter.next()
            }
        }
        Box::new(InterruptibleIterator { iter })
    }
}

/// A wrapped `UnsafeCell` that is unsafely `Sync` if `V: Send`
/// This can be used if there are additional invariants that make this safe
struct SyncUnsafeCell<V> {
    cell: UnsafeCell<V>,
}

impl<V> SyncUnsafeCell<V> {
    pub unsafe fn new(value: V) -> Self {
        Self { cell: UnsafeCell::new(value) }
    }

    pub fn into_inner(self) -> V {
        self.cell.into_inner()
    }
}

impl<V> Deref for SyncUnsafeCell<V> {
    type Target = UnsafeCell<V>;

    fn deref(&self) -> &Self::Target {
        &self.cell
    }
}

unsafe impl<V: Send> Sync for SyncUnsafeCell<V> {
}

impl SparkContext {
}

pub struct TaskContext {}
