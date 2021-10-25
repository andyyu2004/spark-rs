mod map;
mod parallel_collection;

pub use map::MapRdd;
pub use parallel_collection::ParallelCollection;

use crate::serialize::SerdeArc;
use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

newtype_index!(RddId);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RddRef(#[serde(with = "serde_traitobject")] Arc<dyn Rdd>);

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
        let lhs = &*self as *const _ as *const ();
        let rhs = &*other as *const _ as *const ();
        lhs == rhs
    }
}

static_assertions::assert_impl_all!(TypedRddRef<i32>: serde_traitobject::Serialize, serde_traitobject::Deserialize);

pub type TypedRddRef<T> = SerdeArc<dyn TypedRdd<Element = T>>;

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

#[async_trait]
pub trait Rdd:
    Send
    + Sync
    + serde_traitobject::Serialize
    + serde_traitobject::Deserialize
    + std::fmt::Debug
    + 'static
{
    fn id(&self) -> RddId;

    fn scx(&self) -> Arc<SparkContext>;

    fn dependencies(&self) -> Dependencies;

    async fn partitions(&self) -> SparkResult<Partitions>;

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
    type Element: CloneDatum;

    /// Convert the [`Arc<TypedRdd>`] to a [`RddRef`]
    /// Can be implemented as the following.
    /// ```
    /// fn as_untyped(self: Arc<Self>) -> RddRef {
    ///     RddRef::from_inner(self)
    /// }
    /// ```
    /// But this doesn't work as a default implementation unfortunately.
    fn as_untyped(self: Arc<Self>) -> RddRef;

    /// Convert concrete [`TypedRdd`] type to a [`TypedRddRef`]
    /// Can be implemented as follows
    /// ```
    /// TypedRddRef::from_inner(self as Arc<dyn TypedRdd<Element = Self::Element>>)
    /// ```
    fn as_typed_ref(self: Arc<Self>) -> TypedRddRef<Self::Element>;

    fn compute(
        self: Arc<Self>,
        cx: &mut TaskContext,
        partition: PartitionIdx,
    ) -> SparkResult<SparkIteratorRef<Self::Element>>;

    fn map<F>(self: Arc<Self>, f: F) -> Arc<MapRdd<Self::Element, F>>
    where
        Self: Sized,
    {
        Arc::new(MapRdd::new(self.as_typed_ref(), f))
    }
}

#[async_trait]
pub trait TypedRddExt: TypedRdd {
    async fn collect(self: Arc<Self>) -> SparkResult<Vec<Self::Element>> {
        let scx = self.scx();
        let f = Fn!(|_cx: &mut TaskContext, iter: SparkIteratorRef<Self::Element>| iter
            .collect::<Vec<Self::Element>>());
        let partition_iterators = scx.collect_rdd(self.as_typed_ref(), f).await?;
        Ok(partition_iterators.into_iter().flatten().collect())
    }
}

impl<R: TypedRdd + ?Sized> TypedRddExt for R {
}

fn _rdd_is_dyn_safe<T>(_rdd: Box<dyn TypedRdd<Element = T>>) {
}
