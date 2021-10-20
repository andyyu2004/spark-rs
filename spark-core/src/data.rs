use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

pub trait Datum = Send
    + Debug
    + Sync
    + Serialize
    + DeserializeOwned
    + serde_traitobject::Serialize
    + serde_traitobject::Deserialize
    + 'static;

pub trait CloneDatum = Clone + Datum;
