use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait Datum = Clone
    + Send
    + Sync
    + Serialize
    + DeserializeOwned
    + serde_traitobject::Serialize
    + serde_traitobject::Deserialize
    + 'static;
