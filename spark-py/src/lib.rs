use pyo3::prelude::*;
use pyo3::types::PySequence;
use spark::{SparkContext, SparkSession};
use std::sync::Arc;

// Error handling is a bit broken with eyre/anyhow
// Eyre's pyo3 feature is out of date or something
// Pyo3 is planning to have a anyhow and eyre feature next version
// so we can wait for that, features = ["pyo3"] , features = ["pyo3"]

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
            let scx = SparkSession::builder().create().await.unwrap().scx();
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

#[pymethods]
impl PySparkContext {
    pub fn parallelize(&self, data: &PySequence) -> PyResult<()> {
        let data: Vec<i32> = pythonize::depythonize(&data).unwrap();
        dbg!(&data);
        Arc::clone(&self.scx).parallelize(&data);
        Ok(())
    }
}

#[pymodule]
#[pyo3(name = "pyspark")]
fn spark(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySparkSession>()?;
    Ok(())
}
