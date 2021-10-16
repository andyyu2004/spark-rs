mod map;
mod parallel_collection;

pub use parallel_collection::ParallelCollection;

use self::map::Map;
use crate::*;
use std::sync::Arc;

pub type RddRef = Arc<dyn Rdd>;
pub type TypedRddRef<T> = Arc<dyn TypedRdd<Output = T>>;

pub trait Rdd: Send + Sync + 'static {
    fn spark(&self) -> Arc<SparkContext>;

    fn dependencies(&self) -> Dependencies;

    fn partitions(&self) -> Partitions;
}

pub trait TypedRdd: Rdd {
    type Output: Datum;

    fn compute(
        self: Arc<Self>,
        ctxt: TaskContext,
        split: PartitionIndex,
    ) -> Box<dyn Iterator<Item = Self::Output>>;

    fn map<F>(self: Arc<Self>, f: F) -> Map<Self, F>
    where
        Self: Sized,
    {
        Map::new(self, f)
    }
}

#[async_trait]
trait RddAsyncExt: TypedRdd + Sized {
    async fn collect(self: Arc<Self>) -> SparkResult<Vec<Self::Output>> {
        let spark = self.spark();
        let partition_iterators = spark.collect_rdd(self, |_cx, iter| iter).await?;
        Ok(partition_iterators.into_iter().flatten().collect())
    }
}

impl<R: TypedRdd> RddAsyncExt for R {
}

fn _rdd_is_dyn_safe<T>(_rdd: Box<dyn TypedRdd<Output = T>>) {
}
