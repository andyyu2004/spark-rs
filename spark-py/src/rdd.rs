use super::*;
use serde::{Deserialize, Serialize};
use serde_closure::Fn;
use spark::rdd::{MapRdd, ParallelCollection, Rdd, RddId, RddRef};
use spark::*;

#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct PyParallelCollectionRdd {
    pub(super) inner: Arc<ParallelCollection<SerdePyObject>>,
}

#[pymethods]
impl PyParallelCollectionRdd {
    pub fn map(&self, py: Python<'_>, pyfn: &PyFunction) -> PyMapRdd {
        let fn_obj = SerdePyObject(pyfn.to_object(py));
        let f = Fn!(move |obj: SerdePyObject| Python::with_gil(|py| {
            let pyfn = fn_obj.0.cast_as::<PyFunction>(py).unwrap();
            let any = pyfn.call1((obj.0,)).unwrap();
            SerdePyObject(any.to_object(py))
        }));
        let map: Arc<MapRdd<SerdePyObject, _>> = Arc::clone(&self.inner).map(f);
        PyMapRdd { inner: map.as_typed_ref() }
    }

    pub fn collect<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let rdd = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let output: Vec<SerdePyObject> = crate::unwrap!(rdd.collect().await);
            Python::with_gil(|py| Ok(PyList::new(py, output).to_object(py)))
        })
    }
}

pub trait PyFn:
    Fn(SerdePyObject) -> SerdePyObject
    + std::fmt::Debug
    + serde_traitobject::Serialize
    + serde_traitobject::Deserialize
    + Send
    + Sync
    + 'static
{
}

impl<F> PyFn for F where
    F: Fn(SerdePyObject) -> SerdePyObject
        + std::fmt::Debug
        + serde_traitobject::Serialize
        + serde_traitobject::Deserialize
        + Send
        + Sync
        + 'static
{
}

// TODO Need to think of a way to make the non object safe methods on `TypedRdd` work.
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct PyMapRdd2 {
    pub(super) inner: TypedRddRef<SerdePyObject>,
    #[serde(with = "serde_traitobject")]
    pub(super) f: Arc<dyn PyFn>,
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct PyMapRdd {
    pub(super) inner: TypedRddRef<SerdePyObject>,
}

// TODO can hopefully wrap all this reimplementation into a macro when the common patterns are clearer
#[async_trait]
impl Rdd for PyParallelCollectionRdd {
    fn id(&self) -> RddId {
        self.inner.id()
    }

    fn scx(&self) -> Arc<SparkContext> {
        self.inner.scx()
    }

    fn dependencies(&self) -> Dependencies {
        self.inner.dependencies()
    }

    async fn partitions(&self) -> SparkResult<Partitions> {
        self.inner.partitions().await
    }
}

impl TypedRdd for PyParallelCollectionRdd {
    type Element = SerdePyObject;

    fn as_untyped(self: Arc<Self>) -> RddRef {
        Arc::clone(&self.inner).as_untyped()
    }

    fn as_typed_ref(self: Arc<Self>) -> TypedRddRef<Self::Element> {
        Arc::clone(&self.inner).as_typed_ref()
    }

    fn compute(
        self: Arc<Self>,
        cx: &mut TaskContext,
        partition: PartitionIdx,
    ) -> SparkResult<SparkIteratorRef<Self::Element>> {
        Arc::clone(&self.inner).compute(cx, partition)
    }
}

#[async_trait]
impl Rdd for PyMapRdd {
    fn id(&self) -> RddId {
        self.inner.id()
    }

    fn scx(&self) -> Arc<SparkContext> {
        self.inner.scx()
    }

    fn dependencies(&self) -> Dependencies {
        self.inner.dependencies()
    }

    async fn partitions(&self) -> SparkResult<Partitions> {
        self.inner.partitions().await
    }
}

impl TypedRdd for PyMapRdd {
    type Element = SerdePyObject;

    fn as_untyped(self: Arc<Self>) -> RddRef {
        Arc::clone(&self.inner).as_untyped()
    }

    fn as_typed_ref(self: Arc<Self>) -> TypedRddRef<Self::Element> {
        Arc::clone(&self.inner).as_typed_ref()
    }

    fn compute(
        self: Arc<Self>,
        cx: &mut TaskContext,
        partition: PartitionIdx,
    ) -> SparkResult<SparkIteratorRef<Self::Element>> {
        Arc::clone(&self.inner).compute(cx, partition)
    }
}
