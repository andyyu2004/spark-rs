use crate::data::CloneDatum;

use super::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Map<R, F> {
    id: RddId,
    rdd: Arc<R>,
    f: F,
}

impl<R: Rdd, F> Map<R, F> {
    pub fn new(rdd: Arc<R>, f: F) -> Self {
        let rdd_id = rdd.scx().next_rdd_id();
        Self { id: rdd_id, rdd, f }
    }
}

impl<R: Rdd, F> std::fmt::Debug for Map<R, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Map").field("id", &self.id).field("rdd", &self.rdd).finish_non_exhaustive()
    }
}

#[async_trait]
impl<R: Rdd + Serialize + DeserializeOwned, F: Datum> Rdd for Map<R, F> {
    fn id(&self) -> RddId {
        self.id
    }

    fn scx(&self) -> Arc<SparkContext> {
        self.rdd.scx()
    }

    fn dependencies(&self) -> Dependencies {
        todo!()
    }

    async fn partitions(&self) -> SparkResult<Partitions> {
        todo!()
    }
}

impl<R, F, T> TypedRdd for Map<R, F>
where
    R: TypedRdd + Serialize + DeserializeOwned,
    T: CloneDatum,
    F: Fn(R::Element) -> T + CloneDatum,
{
    type Element = T;

    fn as_untyped(self: Arc<Self>) -> RddRef {
        RddRef::from_inner(self)
    }

    fn compute(
        self: Arc<Self>,
        cx: &mut TaskContext,
        split: PartitionIdx,
    ) -> SparkResult<SparkIteratorRef<Self::Element>> {
        Ok(Box::new(Arc::clone(&self.rdd).compute(cx, split)?.map(self.f.clone())))
    }
}
