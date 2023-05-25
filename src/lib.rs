//! Parallelo Structured Parallel Processing is a simple parallel processing
//! library written in Rust.
//!
//#![warn(missing_docs)]
#![feature(unsized_fn_params)]
#![feature(box_into_inner)]
#![feature(once_cell)]
#![feature(let_chains)]

pub mod collections;
pub mod core;
pub mod mpsc;
pub mod pipeline;
mod task;
pub mod thread_pool;

pub mod prelude {
    //! This module contains the most used types and traits.
    pub use crate::core::orchestrator::get_global_orchestrator;
    pub use crate::core::orchestrator::Orchestrator;
    pub use crate::pipeline::node::{In, InNode, InOut, InOutNode, Node, Out, OutNode};
    pub use crate::pipeline::Pipeline;
    pub use crate::thread_pool::ThreadPool;
    pub use crate::{parallel, propagate};
}
