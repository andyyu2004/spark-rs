use crate::broadcast::{Broadcast, BroadcastContext};
use crate::config::{SparkConfig, TaskSchedulerConfig};
use crate::data::{CloneDatum, Datum};
use crate::env::SparkEnv;
use crate::executor::ExecutorId;
use crate::rdd::*;
use crate::rpc::SparkRpcServer;
use crate::scheduler::*;
use crate::*;
use indexed_vec::Idx;
use serde::{Deserialize, Serialize};
use std::lazy::SyncOnceCell;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct SparkContext {
    env: Arc<SparkEnv>,
    dag_scheduler_cell: SyncOnceCell<Arc<DagScheduler>>,
    rdd_idx: AtomicUsize,
    task_scheduler: Arc<TaskScheduler>,
    /// The address the driver is actually bound to.
    /// This may differ from the configured port due to it being unavailable.
    bind_addr: SocketAddr,
}

static_assertions::assert_impl_all!(Arc<SparkContext>: Send, Sync);

pub struct RpcContext {
    executor_idx: AtomicUsize,
}

impl RpcContext {
    pub fn next_executor_id(&self) -> ExecutorId {
        ExecutorId::new(self.executor_idx.fetch_add(1, Ordering::SeqCst))
    }
}

impl SparkContext {
    pub async fn new(config: Arc<SparkConfig>) -> SparkResult<Arc<Self>> {
        let rcx = Arc::new(RpcContext {
            /// Start from 1 as 0 is reserved for the driver
            executor_idx: AtomicUsize::new(1),
        });

        let (bind_addr, _) = SparkRpcServer::new(rcx).bind(&config.driver_addr).await?;

        let env = SparkEnv::init_for_driver(bind_addr, BroadcastContext::new).await;

        let task_scheduler_backend: Arc<dyn TaskSchedulerBackend> = match &config.task_scheduler {
            TaskSchedulerConfig::Local { num_threads } =>
                Arc::new(LocalTaskSchedulerBackend::new(*num_threads)),
            TaskSchedulerConfig::Distributed { url: _ } =>
                Arc::new(DistributedTaskSchedulerBackend::new(bind_addr)),
        };

        let task_scheduler = Arc::new(TaskScheduler::new(task_scheduler_backend));

        let scx = Arc::new(Self {
            env,
            task_scheduler,
            bind_addr,
            dag_scheduler_cell: Default::default(),
            rdd_idx: Default::default(),
        });

        Ok(scx)
    }

    pub fn env(&self) -> Arc<SparkEnv> {
        Arc::clone(&self.env)
    }

    pub fn bind_addr(&self) -> &SocketAddr {
        &self.bind_addr
    }

    pub fn dag_scheduler(&self) -> Arc<DagScheduler> {
        let task_scheduler = self.task_scheduler();
        let dag_scheduler =
            self.dag_scheduler_cell.get_or_init(|| Arc::new(DagScheduler::new(task_scheduler)));
        Arc::clone(dag_scheduler)
    }

    pub fn task_scheduler(&self) -> Arc<TaskScheduler> {
        Arc::clone(&self.task_scheduler)
    }

    pub fn next_rdd_id(&self) -> RddId {
        RddId::new(self.rdd_idx.fetch_add(1, Ordering::SeqCst))
    }

    #[inline(always)]
    pub fn make_rdd<T: CloneDatum>(self: Arc<Self>, data: &[T]) -> Arc<impl TypedRdd<Element = T>> {
        self.parallelize(data)
    }

    pub fn parallelize_iter<T: CloneDatum>(
        self: Arc<Self>,
        data: impl IntoIterator<Item = T>,
    ) -> Arc<ParallelCollection<T>> {
        let data = data.into_iter().collect::<Vec<_>>();
        self.parallelize(&data)
    }

    pub fn parallelize<T: CloneDatum>(self: Arc<Self>, data: &[T]) -> Arc<ParallelCollection<T>> {
        let num_slices = self.task_scheduler().default_parallelism();
        self.parallelize_with_slices(data, num_slices)
    }

    pub fn parallelize_with_slices<T: CloneDatum>(
        self: Arc<Self>,
        data: &[T],
        num_slices: usize,
    ) -> Arc<ParallelCollection<T>> {
        Arc::new(ParallelCollection::new(self, data, num_slices))
    }

    pub async fn run_rdd<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        partitions: Partitions,
        f: impl PartitionMapper<T, U>,
    ) -> SparkResult<JobOutput<U>>
    where
        T: CloneDatum,
        U: Datum,
    {
        self.dag_scheduler().run(rdd, partitions, f).await
    }

    pub async fn collect_rdd<T, U>(
        self: Arc<Self>,
        rdd: TypedRddRef<T>,
        f: impl PartitionMapper<T, U>,
    ) -> SparkResult<Vec<U>>
    where
        T: CloneDatum,
        U: Datum,
    {
        let n = rdd.partitions().await?.len();
        self.run_rdd(rdd, (0..n).map(PartitionIdx::new).collect(), f).await
    }

    pub fn broadcast<T: Datum>(self: &Arc<Self>, datum: T) -> Broadcast<T> {
        Broadcast::new(self, datum)
    }

    pub fn interruptible_iterator<T: CloneDatum>(
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

impl Deref for SparkContext {
    type Target = Arc<SparkEnv>;

    fn deref(&self) -> &Self::Target {
        &self.env
    }
}

#[derive(Serialize, Deserialize)]
pub struct TaskContext {}
