//! Multi-producer, single-consumer channels.
//! 
//! This module contains the traits and implementations for multi-producer, single-consumer channels.
//! 
//! The traits are:
//! - `Receiver<T>`: defines the receiver side of a channel.
//! - `Sender<T>`: defines the sender side of a channel.
//! 
//! The structs are:
//! - `InputChannel<T>`: defines the receiver side of a channel.
//! - `OutputChannel<T>`: defines the sender side of a channel.
//! 
//! The channel implementations available are:
//! - *crossbeam*: uses the crossbeam channel.
//! - *flume*: uses the flume channel.
//! - *kanal*: uses the kanal channel.
//! - *ff*: uses a channel based on fastflow spsc queues.
//! 
//! The channel implementation is selected at compile time by the feature flag.

pub mod channel;
#[cfg(feature = "crossbeam")]
mod channel_cb;
#[cfg(feature = "ff")]
mod channel_ff;
#[cfg(feature = "flume")]
mod channel_flume;
#[cfg(feature = "kanal")]
mod channel_kanal;
pub mod err;
