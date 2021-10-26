use super::*;

#[derive(Serialize, Deserialize)]
pub struct FilterRdd<T: 'static, P> {
    id: RddId,
    rdd: TypedRddRef<T>,
    deps: Dependencies,
    p: P,
    #[serde(skip)]
    base: RddBase,
}

impl<T: CloneDatum, P> FilterRdd<T, P> {
    pub fn new(rdd: TypedRddRef<T>, f: P) -> Self {
        let id = rdd.scx().next_rdd_id();
        let deps = Arc::new(vec![Dependency::new_one_to_one(Arc::clone(&rdd).as_untyped())]);
        Self { id, rdd, deps, p: f, base: Default::default() }
    }
}

impl<T, F> std::fmt::Debug for FilterRdd<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterRdd")
            .field("id", &self.id)
            .field("rdd", &self.rdd)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: CloneDatum, F: Datum> Rdd for FilterRdd<T, F> {
    fn id(&self) -> RddId {
        self.id
    }

    fn base(&self) -> &RddBase {
        &self.base
    }

    fn scx(&self) -> Arc<SparkContext> {
        self.rdd.scx()
    }

    fn compute_dependencies(&self) -> Dependencies {
        Arc::clone(&self.deps)
    }

    async fn compute_partitions(&self) -> SparkResult<Partitions> {
        self.first_parent().rdd().partitions().await
    }
}

static_assertions::assert_impl_all!(ErasedPredicate<i32>: Fn(&i32) -> bool, Datum);

impl<T, F> TypedRdd for FilterRdd<T, F>
where
    T: CloneDatum,
    F: Fn(&T) -> bool + Datum,
{
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
        let filtered = Arc::clone(&self.rdd).compute(cx, partition)?.filter(move |x| (&self.p)(x));
        Ok(Box::new(filtered))
    }
}
