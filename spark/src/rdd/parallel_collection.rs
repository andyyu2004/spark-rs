use super::*;
use crate::broadcast::Broadcast;
use crate::data::CloneDatum;
use indexed_vec::Idx;
use serde_derive::{Deserialize, Serialize};
use std::sync::{Arc, Weak};

#[derive(Serialize, Deserialize)]
pub struct ParallelCollection<T> {
    #[serde(skip_serializing, skip_deserializing)]
    scx: Weak<SparkContext>,
    rdd_id: RddId,
    partitions: Broadcast<Vec<ParallelCollectionPartition<T>>>,
    partition_indices: Vec<PartitionIdx>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ParallelCollectionPartition<T> {
    data: Arc<Vec<T>>,
}

impl<T: CloneDatum> ParallelCollection<T> {
    pub fn new(scx: Arc<SparkContext>, data: &[T], num_slices: usize) -> Self {
        assert!(num_slices > 0, "require at least one slice");
        let partitions = data
            .chunks(std::cmp::max(data.len() / num_slices, 1))
            .map(|chunk| ParallelCollectionPartition { data: Arc::new(chunk.to_vec()) })
            .collect::<Vec<_>>();
        let partition_indices = (0..partitions.len()).map(PartitionIdx::new).collect();
        let partitions = scx.broadcast(partitions);
        let rdd_id = scx.next_rdd_id();
        Self { rdd_id, scx: Arc::downgrade(&scx), partitions, partition_indices }
    }

    fn partitions_blocking(&self) -> SparkResult<&[ParallelCollectionPartition<T>]> {
        futures::executor::block_on(async { Ok(&self.partitions.get().await?[..]) })
    }
}

impl<T> std::fmt::Debug for ParallelCollection<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelCollection").field("rdd_id", &self.rdd_id).finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: CloneDatum> Rdd for ParallelCollection<T> {
    fn id(&self) -> RddId {
        self.rdd_id
    }

    fn scx(&self) -> Arc<SparkContext> {
        self.scx.upgrade().expect("cannot access context remotely")
    }

    fn dependencies(&self) -> Dependencies {
        Default::default()
    }

    async fn partitions(&self) -> SparkResult<Partitions> {
        Ok((0..self.partitions.get().await?.len()).map(PartitionIdx::new).collect())
    }
}

impl<T: CloneDatum> TypedRdd for ParallelCollection<T> {
    type Element = T;

    fn as_untyped(self: Arc<Self>) -> RddRef {
        RddRef::from_inner(self)
    }

    fn compute(
        self: Arc<Self>,
        _cx: &mut TaskContext,
        idx: PartitionIdx,
    ) -> SparkResult<SparkIteratorRef<Self::Element>> {
        let partition = &self.partitions_blocking()?[idx.index()];
        let data = Arc::clone(&partition.data);
        Ok(Box::new((0..data.len()).map(move |i| data[i].clone())))
    }
}
