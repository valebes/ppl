//! Parallelo Structured Parallel Processing is a simple parallel processing
//! library written in Rust.
//!
//#![warn(missing_docs)]
#![feature(unsized_fn_params)]
#![feature(box_into_inner)]

pub mod channel;
//pub mod map;
pub mod core;
pub mod node;
pub mod pspp;
mod task;
pub mod thread_pool;
