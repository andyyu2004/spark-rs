#![feature(
    new_uninit,
    once_cell,
    type_name_of_val,
    trait_alias,
    arbitrary_self_types,
    unboxed_closures,
    exact_size_is_empty
)]

mod context;
mod data;
mod dependency;
mod error;
mod iter;
mod partition;
mod session;

pub mod config;
pub mod executor;
pub mod rdd;
pub mod scheduler;
pub mod serialize;

pub use context::*;
pub use data::*;
pub use dependency::*;
pub use error::{SparkError, SparkResult};
pub use iter::*;
pub use partition::*;
pub use session::*;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate serde_closure;

#[macro_use]
extern crate tracing;
