use super::*;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct SerdePyObject(pub(super) PyObject);

impl Serialize for SerdePyObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Python::with_gil(|py| {
            let pybytes = pyo3::marshal::dumps(py, &self.0, pyo3::marshal::VERSION).unwrap();
            serializer.serialize_bytes(pybytes.as_bytes())
        })
    }
}

struct PyVisitor;

impl<'de> Visitor<'de> for PyVisitor {
    type Value = SerdePyObject;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "bytes that can be loaded using pyo3::marshal::loads")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Python::with_gil(|py| {
            let obj = pyo3::marshal::loads(py, v).unwrap();
            Ok(SerdePyObject(obj.to_object(py)))
        })
    }
}

impl<'de> Deserialize<'de> for SerdePyObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(PyVisitor)
    }
}
