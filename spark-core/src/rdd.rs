mod map;
mod parallel_collection;

pub use parallel_collection::ParallelCollection;

use self::map::Map;
use crate::*;
use std::sync::Arc;

pub type RddRef<T> = Arc<dyn Rdd<Output = T>>;

pub trait Rdd: Send + Sync + 'static {
    type Output: Datum;

    fn spark(&self) -> Arc<SparkContext>;

    fn dependencies(&self) -> &[Dependency<Self::Output>];

    fn partitions(&self) -> Partitions;

    fn compute(
        self: Arc<Self>,
        ctxt: TaskContext,
        split: PartitionIndex,
    ) -> Box<dyn Iterator<Item = Self::Output>>;
}

#[async_trait]
trait RddExt: Rdd + Sized {
    fn map<F>(self: Arc<Self>, f: F) -> Map<Self, F> {
        Map::new(self, f)
    }

    async fn collect(self: Arc<Self>) -> SparkResult<Vec<Self::Output>> {
        let spark = self.spark();
        let partition_iterators = spark.collect_rdd(self, |_cx, iter| iter).await?;
        Ok(partition_iterators.into_iter().flatten().collect())
    }
}

impl<R: Rdd> RddExt for R {
}

fn _rdd_is_dyn_safe<T>(_rdd: Box<dyn Rdd<Output = T>>) {
}
