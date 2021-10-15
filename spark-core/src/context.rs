use crate::rdd::*;
use crate::scheduler::{DagScheduler, TaskScheduler};
use crate::*;
use indexed_vec::Idx;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;

pub struct SparkContext {
    dag_scheduler: DagScheduler,
    task_scheduler: TaskScheduler,
}

impl SparkContext {
    pub fn new() -> Self {
        Self { dag_scheduler: DagScheduler {}, task_scheduler: TaskScheduler {} }
    }

    #[inline(always)]
    pub fn make_rdd<T: Datum>(self: Arc<Self>, data: &[T]) -> impl Rdd<Output = T> {
        self.parallelize(data)
    }

    pub fn parallelize<T: Datum>(self: Arc<Self>, data: &[T]) -> impl Rdd<Output = T> {
        let num_slices = self.task_scheduler.default_parallelism();
        self.parallelize_with_slices(data, num_slices)
    }

    pub fn parallelize_with_slices<T: Datum>(
        self: Arc<Self>,
        data: &[T],
        num_slices: usize,
    ) -> impl Rdd<Output = T> {
        ParallelCollection::new(self, data, num_slices)
    }

    pub fn run<T, U>(
        self: Arc<Self>,
        rdd: impl Rdd<Output = T>,
        partitions: Partitions,
        f: impl Fn(TaskContext, SparkIterator<T>) -> U,
        handler: impl Fn(PartitionIndex, U),
    ) -> SparkResult<()> {
        self.dag_scheduler.run(rdd, partitions, f, handler)
    }

    pub fn run_rdd<T, U>(
        self: Arc<Self>,
        rdd: impl Rdd<Output = T>,
        f: impl Fn(TaskContext, SparkIterator<T>) -> U,
    ) -> SparkResult<Vec<U>> {
        let n = rdd.partitions().len();
        let out = UnsafeCell::new(Box::<[U]>::new_uninit_slice(n));
        let handler =
            // SAFETY: Each index of `out` should be written to exactly once (once per partition)
            |i: PartitionIndex, data| unsafe { &mut *out.get() }[i.index()] = MaybeUninit::new(data);
        self.run(rdd, 0..n, f, handler)?;
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

impl SparkContext {
}

pub struct TaskContext {}
