pub trait SerdeFn<Args>:
    Fn<Args> + Send + Sync + Clone + serde::ser::Serialize + serde::de::DeserializeOwned + 'static
{
}

impl<Args, T> SerdeFn<Args> for T where
    T: Fn<Args>
        + Send
        + Sync
        + Clone
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + 'static
{
}

#[macro_export]
macro_rules! newtype_index {
    ($name:ident) => {
        indexed_vec::newtype_index!($name);

        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use indexed_vec::Idx;
                self.index().serialize(serializer)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                usize::deserialize(deserializer).map(indexed_vec::Idx::new)
            }
        }
    };
}
