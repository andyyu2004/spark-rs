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

impl<R: TypedRdd, F: Datum> Rdd for Map<R, F> {
    fn spark(&self) -> Arc<SparkContext> {
        self.rdd.spark()
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
    R: TypedRdd,
    T: Datum,
    F: Fn(R::Output) -> T + Datum,
{
    type Output = T;

    fn compute(
        self: Arc<Self>,
        ctxt: TaskContext,
        split: PartitionIndex,
    ) -> Box<dyn Iterator<Item = Self::Output>> {
        Box::new(Arc::clone(&self.rdd).compute(ctxt, split).map(self.f.clone()))
    }
}
