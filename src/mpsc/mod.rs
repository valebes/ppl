//! Multi-producer, Single-consumer channels.
//!
//! This module contains the traits and implementations for multi-producer, single-consumer channels.
//!
//! The traits are:
//! - [`channel::Receiver<T>`]: defines the receiver side of a channel.
//! - [`channel::Sender<T>`]: defines the sender side of a channel.
//!
//! The structs are:
//! - [`channel::ReceiverChannel<T>`]: defines the receiver side of a channel.
//! - [`channel::SenderChannel<T>`]: defines the sender side of a channel.
//!
//! The channel implementations available are:
//! - **crossbeam**: uses the crossbeam channel.
//! - **flume**: uses the flume channel.
//! - **kanal**: uses the kanal channel.
//! - **ff**: uses a channel based on fastflow spsc queues.
//!
//! The channel implementation is selected at compile time by the feature flag.

/// Module containing Traits and Structs to support channel operations.
pub mod channel;
#[cfg(feature = "crossbeam")]
mod channel_cb;
#[cfg(feature = "ff")]
#[cfg(not(tarpaulin_include))]
mod channel_ff;
#[cfg(feature = "flume")]
#[cfg(not(tarpaulin_include))]
mod channel_flume;
#[cfg(feature = "kanal")]
#[cfg(not(tarpaulin_include))]
mod channel_kanal;
/// Channel errors
pub mod err;
