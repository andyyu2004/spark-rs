mod map;
mod parallel_collection;

pub use parallel_collection::ParallelCollection;

use self::map::Map;
use crate::*;
use indexed_vec::newtype_index;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

newtype_index!(RddId);

#[derive(Clone)]
pub struct RddRef(Arc<dyn Rdd>);

impl Deref for RddRef {
    type Target = dyn Rdd;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl RddRef {
    pub fn new(rdd: &Arc<dyn Rdd>) -> Self {
        Self(Arc::clone(rdd))
    }

    pub fn from_inner(rdd: Arc<dyn Rdd>) -> Self {
        Self(rdd)
    }
}

impl Hash for RddRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl Eq for RddRef {
}

impl PartialEq for RddRef {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

pub type TypedRddRef<T> = Arc<dyn TypedRdd<Output = T>>;

impl PartialEq for dyn Rdd {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Hash for dyn Rdd {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }
}

pub trait Rdd: Send + Sync + 'static {
    fn id(&self) -> RddId;

    fn spark(&self) -> Arc<SparkContext>;

    fn dependencies(&self) -> Dependencies;

    fn partitions(&self) -> Partitions;

    fn immediate_shuffle_dependencies(&self) -> HashSet<ShuffleDependency> {
        let deps = self.dependencies();
        let mut shuffle_deps = HashSet::new();
        for dep in deps.iter() {
            match dep {
                Dependency::Narrow(narrow_dep) =>
                    shuffle_deps.extend(narrow_dep.rdd().immediate_shuffle_dependencies()),
                Dependency::Shuffle(shuffle_dep) => {
                    shuffle_deps.insert(shuffle_dep.clone());
                }
            }
        }
        shuffle_deps
    }
}

pub trait TypedRdd: Rdd {
    type Output: Datum;

    /// Convert the `Arc<TypedRdd>` to a `RddRef`
    /// Can be implemented as the following.
    /// ```
    /// fn as_untyped(self: Arc<Self>) -> RddRef {
    ///     RddRef::from_inner(self)
    /// }
    /// ```
    /// But this doesn't work as a default implementation unfortunately.
    fn as_untyped(self: Arc<Self>) -> RddRef;

    fn compute(
        self: Arc<Self>,
        ctxt: TaskContext,
        split: PartitionIndex,
    ) -> Box<dyn Iterator<Item = Self::Output>>;

    fn map<F>(self: Arc<Self>, f: F) -> Map<Self, F>
    where
        Self: Sized,
    {
        Map::new(self, f)
    }
}

#[async_trait]
trait RddAsyncExt: TypedRdd + Sized {
    async fn collect(self: Arc<Self>) -> SparkResult<Vec<Self::Output>> {
        let spark = self.spark();
        let partition_iterators = spark.collect_rdd(self, |_cx, iter| iter).await?;
        Ok(partition_iterators.into_iter().flatten().collect())
    }
}

impl<R: TypedRdd> RddAsyncExt for R {
}

fn _rdd_is_dyn_safe<T>(_rdd: Box<dyn TypedRdd<Output = T>>) {
}
