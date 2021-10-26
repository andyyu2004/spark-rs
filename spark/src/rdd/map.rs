use super::*;
use crate::data::CloneDatum;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MapRdd<T: 'static, F> {
    id: RddId,
    rdd: TypedRddRef<T>,
    f: F,
}

impl<T, F> MapRdd<T, F> {
    pub fn new(rdd: TypedRddRef<T>, f: F) -> Self {
        let id = rdd.scx().next_rdd_id();
        Self { id, rdd, f }
    }
}

impl<T, F> std::fmt::Debug for MapRdd<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapRdd")
            .field("id", &self.id)
            .field("rdd", &self.rdd)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: CloneDatum, F: Datum> Rdd for MapRdd<T, F> {
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

impl<T, U, F> TypedRdd for MapRdd<T, F>
where
    T: CloneDatum,
    U: CloneDatum,
    F: Fn(T) -> U + Datum,
{
    type Element = U;

    fn as_untyped(self: Arc<Self>) -> RddRef {
        RddRef::from_inner(self)
    }

    fn as_typed_ref(self: Arc<Self>) -> TypedRddRef<Self::Element> {
        TypedRddRef::from_inner(self as Arc<dyn TypedRdd<Element = Self::Element>>)
    }

    fn compute(
        self: Arc<Self>,
        cx: &mut TaskContext,
        partition: PartitionIdx,
    ) -> SparkResult<SparkIteratorRef<Self::Element>> {
        Ok(Box::new(Arc::clone(&self.rdd).compute(cx, partition)?.map(move |x| (&self.f)(x))))
    }
}

#[derive(Serialize, Deserialize)]
pub struct ErasedMapRdd<T: 'static> {
    id: RddId,
    rdd: TypedRddRef<T>,
    #[serde(with = "serde_traitobject")]
    f: ErasedMapper<T>,
}

impl<T> ErasedMapRdd<T> {
    pub fn new(rdd: TypedRddRef<T>, f: ErasedMapper<T>) -> Self {
        let id = rdd.scx().next_rdd_id();
        Self { id, rdd, f }
    }
}

impl<T> std::fmt::Debug for ErasedMapRdd<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasedMapRdd")
            .field("id", &self.id)
            .field("rdd", &self.rdd)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: CloneDatum> Rdd for ErasedMapRdd<T> {
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

impl<T: CloneDatum> TypedRdd for ErasedMapRdd<T> {
    type Element = T;

    fn as_untyped(self: Arc<Self>) -> RddRef {
        RddRef::from_inner(self)
    }

    fn as_typed_ref(self: Arc<Self>) -> TypedRddRef<Self::Element> {
        TypedRddRef::from_inner(self as Arc<dyn TypedRdd<Element = Self::Element>>)
    }

    fn compute(
        self: Arc<Self>,
        cx: &mut TaskContext,
        partition: PartitionIdx,
    ) -> SparkResult<SparkIteratorRef<Self::Element>> {
        Ok(Box::new(Arc::clone(&self.rdd).compute(cx, partition)?.map(move |x| (&self.f)(x))))
    }
}
