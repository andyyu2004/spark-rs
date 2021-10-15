use crate::config::SparkConfig;
use crate::rdd::*;
use crate::scheduler::{DagScheduler, HandlerFn, PartitionMapper, TaskScheduler};
use crate::*;
use indexed_vec::Idx;
use std::cell::UnsafeCell;
use std::lazy::SyncOnceCell;
use std::mem::MaybeUninit;
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
    pub fn make_rdd<T: Datum>(self: Arc<Self>, data: &[T]) -> impl Rdd<Output = T> {
        self.parallelize(data)
    }

    pub fn parallelize<T: Datum>(self: Arc<Self>, data: &[T]) -> impl Rdd<Output = T> {
        let num_slices = self.task_scheduler().default_parallelism();
        self.parallelize_with_slices(data, num_slices)
    }

    pub fn parallelize_with_slices<T: Datum>(
        self: Arc<Self>,
        data: &[T],
        num_slices: usize,
    ) -> impl Rdd<Output = T> {
        ParallelCollection::new(self, data, num_slices)
    }

    pub async fn run_rdd<T: Datum, U>(
        self: Arc<Self>,
        rdd: Arc<impl Rdd<Output = T>>,
        partitions: Partitions,
        f: impl PartitionMapper<T, U>,
        handler: impl HandlerFn<U>,
    ) -> SparkResult<()> {
        self.dag_scheduler().run(rdd, partitions, f, handler).await
    }

    pub async fn collect_rdd<T: Datum, U>(
        self: Arc<Self>,
        rdd: Arc<impl Rdd<Output = T>>,
        f: impl PartitionMapper<T, U>,
    ) -> SparkResult<Vec<U>> {
        let n = rdd.partitions().len();

        // SAFETY: Each index of `out` should be written to exactly once (once per partition)
        let out = unsafe { SyncUnsafeCell::new(Box::<[U]>::new_uninit_slice(n)) };
        let handler =
            |i: PartitionIndex, data| unsafe { &mut *out.get() }[i.index()] = MaybeUninit::new(data);

        self.run_rdd(rdd, 0..n, f, handler).await?;
        // SAFETY: We initialized every element of `out` above
        Ok(unsafe { out.into_inner().assume_init() }.into_vec())
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
