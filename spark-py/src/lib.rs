mod serialize;

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFunction, PyList};
use serde_closure::Fn;
use serialize::SerdePyObject;
use spark::config::SparkConfig;
use spark::rdd::{ErasedRdd, ErasedRddRef, TypedRddExt};
use spark::serialize::{SerdeArc, SerdeBox};
use spark::{SparkContext, SparkSession, SparkSessionBuilder};
use std::sync::Arc;

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
    pub fn builder() -> PySparkSessionBuilder {
        PySparkSessionBuilder::default()
    }
}

#[pyclass(name = "SparkSessionBuilder")]
#[derive(Default)]
pub struct PySparkSessionBuilder {
    builder: SparkSessionBuilder,
}

#[pymethods]
impl PySparkSessionBuilder {
    pub fn set_master(&mut self, master_url: &str) -> PyResult<()> {
        // self.builder = self.builder.master_url(unwrap!(master_url.parse()));
        todo!();
        Ok(())
    }

    pub fn build<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let builder = self.builder.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let spark = unwrap!(builder.create().await);
            let scx = spark.scx();
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
    pub fn parallelize(&self, data: Vec<PyObject>) -> PyResult<PyRdd> {
        let serializable_py_objects =
            data.into_iter().map(SerdePyObject).collect::<Vec<SerdePyObject>>();
        let inner = self.scx().parallelize_iter(serializable_py_objects).as_erased_ref();
        Ok(PyRdd { scx: self.clone(), inner })
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
    inner: ErasedRddRef<SerdePyObject>,
}

impl PyRdd {
    fn scx(&self) -> PySparkContext {
        self.scx.clone()
    }
}

#[pymethods]
impl PyRdd {
    pub fn collect<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        self.scx.collect_rdd(py, self)
    }

    pub fn map<'py>(&self, py: Python<'py>, pyfn: &PyFunction) -> PyResult<Self> {
        let fn_obj = SerdePyObject(pyfn.to_object(py));
        let f = Fn!(move |obj: SerdePyObject| Python::with_gil(|py| {
            let pyfn = fn_obj.0.cast_as::<PyFunction>(py).unwrap();
            let any = unwrap!(pyfn.call1((obj.0,)));
            SerdePyObject(any.to_object(py))
        }));
        let inner = SerdeArc::clone(&self.inner).into_inner().erased_map(SerdeBox::new(f));
        Ok(Self { scx: self.scx(), inner: inner.as_erased_ref() })
    }

    pub fn filter<'py>(&self, py: Python<'py>, pyfn: &PyFunction) -> PyResult<Self> {
        let fn_obj = SerdePyObject(pyfn.to_object(py));
        let f = Fn!(move |obj: &SerdePyObject| Python::with_gil(|py| {
            let pyfn = fn_obj.0.cast_as::<PyFunction>(py).unwrap();
            let any = unwrap!(pyfn.call1((&obj.0,)));
            unwrap!(any.cast_as::<PyBool>()).is_true()
        }));
        let inner = SerdeArc::clone(&self.inner).into_inner().erased_filter(SerdeBox::new(f));
        Ok(Self { scx: self.scx(), inner: inner.as_erased_ref() })
    }
}

#[pymodule]
#[pyo3(name = "pyspark")]
fn spark(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySparkSession>()?;
    m.add_class::<PyRdd>()?;
    Ok(())
}
