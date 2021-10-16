use super::*;

pub struct Map<R, F> {
    rdd: Arc<R>,
    f: F,
}

impl<R, F> Map<R, F> {
    pub fn new(rdd: Arc<R>, f: F) -> Self {
        Self { rdd, f }
    }
}

impl<R, F, T> Rdd for Map<R, F>
where
    R: Rdd + Sync,
    R::Output: 'static,
    T: Datum,
    F: Fn(R::Output) -> T + Send + Sync + Clone + 'static,
{
    type Output = T;

    fn spark(&self) -> Arc<SparkContext> {
        self.rdd.spark()
    }

    fn dependencies(&self) -> &[Dependency<T>] {
        todo!()
    }

    fn partitions(&self) -> Partitions {
        todo!()
    }

    fn compute(
        self: Arc<Self>,
        ctxt: TaskContext,
        split: PartitionIndex,
    ) -> Box<dyn Iterator<Item = Self::Output>> {
        Box::new(Arc::clone(&self.rdd).compute(ctxt, split).map(self.f.clone()))
    }
}
