//! Parallelo Structured Parallel Processing is a simple parallel processing
//! library written in Rust.
//!
//#![warn(missing_docs)]
#![feature(unsized_fn_params)]
#![feature(box_into_inner)]

//pub mod channel;
pub mod channel_ff;
pub mod map;
pub mod node;
pub mod pspp;
mod task;
mod thread;
pub mod thread_pool;
