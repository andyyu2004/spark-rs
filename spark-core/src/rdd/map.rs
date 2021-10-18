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

    fn partitions(&self) -> Partitions {
        todo!()
    }
}

impl<R, F, T> TypedRdd for Map<R, F>
where
    R: TypedRdd + Serialize + DeserializeOwned,
    T: Datum,
    F: Fn(R::Output) -> T + Datum,
{
    type Output = T;

    fn as_untyped(self: Arc<Self>) -> RddRef {
        RddRef::from_inner(self)
    }

    fn compute(
        self: Arc<Self>,
        ctxt: TaskContext,
        split: PartitionIdx,
    ) -> Box<dyn Iterator<Item = Self::Output>> {
        Box::new(Arc::clone(&self.rdd).compute(ctxt, split).map(self.f.clone()))
    }
}
