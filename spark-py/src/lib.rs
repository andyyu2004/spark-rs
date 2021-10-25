#![feature(trait_alias)]

#[macro_use]
extern crate async_trait;

mod rdd;
mod serialize;

use pyo3::prelude::*;
use pyo3::types::{PyFunction, PyList};
use serialize::SerdePyObject;
use spark::rdd::{TypedRdd, TypedRddExt, TypedRddRef};
use spark::{SparkContext, SparkSession};
use std::sync::Arc;

use self::rdd::PyParallelCollectionRdd;

// Error handling is a bit broken with eyre/anyhow
// Eyre's pyo3 feature is out of date or something
// Pyo3 is planning to have a anyhow and eyre feature next version
// so we can wait for that, features = ["pyo3"] , features = ["pyo3"]

/// Use this macro for an easy find and replace with ? when the time comes
/// (To distinguish from when we actually want to unwrap)
#[macro_export]
macro_rules! unwrap {
    ($expr:expr) => {
        $expr.unwrap()
    };
}

#[pyclass(name = "SparkSession")]
pub struct PySparkSession {
    #[pyo3(get)]
    spark_context: PySparkContext,
}

#[pymethods]
impl PySparkSession {
    #[staticmethod]
    pub fn build(py: Python<'_>) -> PyResult<&PyAny> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let scx = unwrap!(SparkSession::builder().create().await).scx();
            let session = PySparkSession { spark_context: PySparkContext { scx } };
            Python::with_gil(|py| Ok(PyCell::new(py, session)?.to_object(py)))
        })
    }
}

#[pyclass(name = "SparkContext")]
#[derive(Clone)]
pub struct PySparkContext {
    scx: Arc<SparkContext>,
}

impl PySparkContext {
    #[inline]
    fn scx(&self) -> Arc<SparkContext> {
        Arc::clone(&self.scx)
    }
}

#[pymethods]
impl PySparkContext {
    pub fn higher_order_experiment<'py>(
        &self,
        py: Python<'py>,
        _f: &PyFunction,
    ) -> PyResult<&'py PyAny> {
        // let x = serde_closure::Fn!(|| f.call0());
        let rdd = self.scx().parallelize(&[3]);

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let output = unwrap!(rdd.collect().await);
            Python::with_gil(|py| Ok(PyList::new(py, output).to_object(py)))
        })
    }

    pub fn parallelize(&self, data: Vec<PyObject>) -> PyResult<PyParallelCollectionRdd> {
        let serializable_py_objects =
            data.into_iter().map(SerdePyObject).collect::<Vec<SerdePyObject>>();
        let inner = self.scx().parallelize_iter(serializable_py_objects);
        Ok(PyParallelCollectionRdd { inner })
    }

    pub fn collect_rdd<'py>(&self, py: Python<'py>, py_rdd: &PyRdd) -> PyResult<&'py PyAny> {
        let rdd = py_rdd.inner.clone().into_inner();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let output: Vec<SerdePyObject> = unwrap!(rdd.collect().await);
            Python::with_gil(|py| Ok(PyList::new(py, output).to_object(py)))
        })
    }
}

#[pyclass(name = "Rdd")]
#[derive(Clone)]
pub struct PyRdd {
    scx: PySparkContext,
    inner: TypedRddRef<SerdePyObject>,
}

#[pymethods]
impl PyRdd {
    pub fn collect<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        self.scx.collect_rdd(py, self)
    }
}

#[pymodule]
#[pyo3(name = "pyspark")]
fn spark(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySparkSession>()?;
    Ok(())
}
