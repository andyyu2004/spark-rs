#![feature(new_uninit, once_cell, trait_alias)]

pub mod rdd;

mod config;
mod context;
mod data;
mod dependency;
mod error;
mod iter;
mod partition;
mod scheduler;
mod session;

pub use context::*;
pub use data::Datum;
pub use dependency::*;
pub use error::{SparkError, SparkResult};
pub use iter::*;
pub use partition::*;
pub use session::*;

#[macro_use]
extern crate async_trait;
