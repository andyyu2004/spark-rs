mod map;
mod parallel_collection;

pub use parallel_collection::ParallelCollection;

use self::map::Map;
use crate::*;
use std::sync::Arc;

#[async_trait]
pub trait Rdd: Send + 'static {
    type Output: Datum;

    fn spark(&self) -> Arc<SparkContext>;

    fn dependencies(&self) -> &[Dependency];

    fn partitions(&self) -> Partitions;

    fn compute(
        self,
        ctxt: TaskContext,
        split: PartitionIndex,
    ) -> Box<dyn Iterator<Item = Self::Output>>;

    fn map<F>(self, f: F) -> Map<Self, F>
    where
        Self: Sized,
    {
        Map::new(self, f)
    }

    async fn collect(self) -> SparkResult<Vec<Self::Output>>
    where
        Self: Sized,
    {
        let spark = self.spark();
        let partition_iterators = spark.collect_rdd(self, |_cx, iter| iter).await?;
        Ok(partition_iterators.into_iter().flatten().collect())
    }
}
