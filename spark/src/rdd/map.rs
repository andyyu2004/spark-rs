use super::*;
use crate::data::CloneDatum;
use crate::Dependency;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MapRdd<T: 'static, F> {
    id: RddId,
    rdd: TypedRddRef<T>,
    deps: Dependencies,
    f: F,
}

impl<T: CloneDatum, F> MapRdd<T, F> {
    pub fn new(rdd: TypedRddRef<T>, f: F) -> Self {
        let id = rdd.scx().next_rdd_id();
        let deps = Arc::new(vec![Dependency::new_one_to_one(Arc::clone(&rdd).as_untyped())]);
        Self { id, rdd, deps, f }
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
        Arc::clone(&self.deps)
    }

    async fn partitions(&self) -> SparkResult<Partitions> {
        self.first_parent().rdd().partitions().await
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
    deps: Dependencies,
    f: ErasedMapper<T>,
}

impl<T: CloneDatum> ErasedMapRdd<T> {
    pub fn new(rdd: TypedRddRef<T>, f: ErasedMapper<T>) -> Self {
        let id = rdd.scx().next_rdd_id();
        let deps = Arc::new(vec![Dependency::new_one_to_one(Arc::clone(&rdd).as_untyped())]);
        Self { id, rdd, deps, f }
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
        Arc::clone(&self.deps)
    }

    async fn partitions(&self) -> SparkResult<Partitions> {
        self.first_parent().rdd().partitions().await
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
