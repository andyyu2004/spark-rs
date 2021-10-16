use indexed_vec::Idx;

use super::*;
use std::sync::Arc;

pub struct ParallelCollection<T> {
    scx: Arc<SparkContext>,
    partitions: Vec<ParallelCollectionPartition<T>>,
    partition_indices: Vec<PartitionIndex>,
}

pub struct ParallelCollectionPartition<T> {
    data: Arc<Vec<T>>,
}

impl<T: Datum> ParallelCollection<T> {
    pub fn new(scx: Arc<SparkContext>, data: &[T], num_slices: usize) -> Self {
        assert!(num_slices > 0, "require at least one slice");
        let partitions = data
            .chunks(data.len() / num_slices)
            .map(|chunk| ParallelCollectionPartition { data: Arc::new(chunk.to_vec()) })
            .collect::<Vec<_>>();
        let partition_indices = (0..partitions.len()).map(PartitionIndex::new).collect();
        Self { scx, partitions, partition_indices }
    }
}

impl<T: Datum> Rdd for ParallelCollection<T> {
    type Output = T;

    fn spark(&self) -> Arc<SparkContext> {
        Arc::clone(&self.scx)
    }

    fn dependencies(&self) -> &[Dependency<T>] {
        &[]
    }

    fn partitions(&self) -> Partitions {
        0..self.partitions.len()
    }

    fn compute(
        self: Arc<Self>,
        _cx: TaskContext,
        idx: PartitionIndex,
    ) -> Box<dyn Iterator<Item = Self::Output>> {
        let partition = &self.partitions[idx.index()];
        let data = Arc::clone(&partition.data);
        let iter = (0..data.len()).map(move |i| data[i].clone());
        Arc::clone(&self.scx).interruptible_iterator(iter)
    }
}
