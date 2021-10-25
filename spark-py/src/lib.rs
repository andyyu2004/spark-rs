mod serialize;

use pyo3::prelude::*;
use pyo3::types::{PyFunction, PyList};
use serialize::SerdePyObject;
use spark::rdd::{TypedRddExt, TypedRddRef};
use spark::{SparkContext, SparkSession};
use std::sync::Arc;

// Error handling is a bit broken with eyre/anyhow
// Eyre's pyo3 feature is out of date or something
// Pyo3 is planning to have a anyhow and eyre feature next version
// so we can wait for that, features = ["pyo3"] , features = ["pyo3"]

/// Use this macro for an easy find and replace with ? when the time comes
/// (To distinguish from when we actually want to unwrap)
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
        f: &PyFunction,
    ) -> PyResult<&'py PyAny> {
        let x = serde_closure::Fn!(|| f.call0());
        let rdd = self.scx().parallelize(&[3]);

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let output = unwrap!(rdd.collect().await);
            Python::with_gil(|py| Ok(PyList::new(py, output).to_object(py)))
        })
    }

    pub fn parallelize(&self, py: Python<'_>, data: Vec<PyObject>) -> PyResult<PyRdd> {
        // Vector over the serialized representation of each element
        let serializable_py_objects =
            data.into_iter().map(SerdePyObject).collect::<Vec<SerdePyObject>>();
        let rdd = self.scx().parallelize_iter(serializable_py_objects);
        todo!();
        // Ok(PyRdd { scx: self.scx(), inner: rdd })
    }

    pub fn collect_rdd<'py>(&self, py: Python<'py>, rdd: PyRdd) -> PyResult<&'py PyList> {
        // rdd.inner.collect();
        todo!()
    }
}

#[pyclass(name = "Rdd")]
#[derive(Clone)]
pub struct PyRdd {
    scx: Arc<SparkContext>,
    inner: TypedRddRef<Vec<u8>>,
}

#[pymodule]
#[pyo3(name = "pyspark")]
fn spark(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySparkSession>()?;
    Ok(())
}