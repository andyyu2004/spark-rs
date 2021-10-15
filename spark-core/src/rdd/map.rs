use super::*;

pub struct Map<R, F> {
    rdd: R,
    f: F,
}

impl<R, F> Map<R, F> {
    pub fn new(rdd: R, f: F) -> Self {
        Self { rdd, f }
    }
}

impl<R, F, T> Rdd for Map<R, F>
where
    R: Rdd,
    R::Output: 'static,
    F: Fn(R::Output) -> T + 'static,
{
    type Output = T;

    fn spark(&self) -> Arc<SparkContext> {
        self.rdd.spark()
    }

    fn dependencies(&self) -> &[Dependency] {
        todo!()
    }

    fn partitions(&self) -> Partitions {
        todo!()
    }

    fn compute(
        self,
        ctxt: TaskContext,
        split: PartitionIndex,
    ) -> Box<dyn Iterator<Item = Self::Output>> {
        Box::new(self.rdd.compute(ctxt, split).map(self.f))
    }
}
