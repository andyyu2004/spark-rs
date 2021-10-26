#![feature(
    arbitrary_self_types,
    async_closure,
    exact_size_is_empty,
    new_uninit,
    once_cell,
    result_cloned,
    trait_alias,
    try_blocks,
    type_name_of_val,
    unboxed_closures
)]

mod broadcast;
mod context;
mod data;
mod dependency;
mod env;
mod error;
mod iter;
mod partition;
mod session;

pub mod cluster;
pub mod config;
pub mod executor;
pub mod rdd;
pub mod rpc;
pub mod scheduler;
pub mod serialize;

pub use context::*;
pub use data::*;
pub use dependency::*;
pub use env::SparkEnv;
pub use error::{SparkError, SparkResult};
pub use iter::*;
pub use partition::*;
pub use session::*;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate eyre;

#[macro_use]
extern crate serde_closure;

#[macro_use]
extern crate tracing;
