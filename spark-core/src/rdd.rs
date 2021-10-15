mod map;
mod parallel_collection;

pub use parallel_collection::ParallelCollection;

use self::map::Map;
use crate::*;
use std::sync::Arc;

pub trait Rdd: Sized {
    type Output;

    fn spark(&self) -> Arc<SparkContext>;

    fn dependencies(&self) -> &[Dependency];

    fn partitions(&self) -> Partitions;

    fn compute(
        self,
        ctxt: TaskContext,
        split: PartitionIndex,
    ) -> Box<dyn Iterator<Item = Self::Output>>;

    fn map<F>(self, f: F) -> Map<Self, F> {
        Map::new(self, f)
    }

    fn collect(self) -> SparkResult<Vec<Self::Output>> {
        Ok(self.spark().run_rdd(self, |_cx, iter| iter)?.into_iter().flatten().collect())
    }
}
