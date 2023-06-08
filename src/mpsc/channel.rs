use crate::core::configuration::WaitPolicy;
use super::err::{ReceiverError, SenderError};

#[cfg(feature = "ff")]
use super::channel_ff as backend;

#[cfg(feature = "crossbeam")]
use super::channel_cb as backend;

#[cfg(feature = "kanal")]
use super::channel_kanal as backend;

#[cfg(feature = "flume")]
use super::channel_flume as backend;

/// Trait defining a channel receiver.
pub trait Receiver<T> {
    /// Receive a message from the channel.
    fn receive(&self) -> Result<Option<T>, ReceiverError>;
    /// Check if the channel is empty.
    fn is_empty(&self) -> bool;
}

/// Trait defining a channel sender.
pub trait Sender<T> {
    /// Send a message to the channel.
    fn send(&self, msg: T) -> Result<(), SenderError>;
}

/// Struct defining the receiver side of a channel.
/// This struct is implemented by the channel backend.
/// The channel backend is selected at compile time by the feature flag.
pub struct InputChannel<T> {
    rx: Box<dyn Receiver<T> + Sync + Send>,
    blocking: bool,
}
impl<T> InputChannel<T>
where
    T: Send,
{
    /// Receive a message from the channel.
    pub fn receive(&self) -> Result<Option<T>, ReceiverError> {
        self.rx.receive()
    }

    /// Receive all messages from the channel, if any.
    /// This method does not block.
    pub fn try_receive_all(&self) -> Vec<T> {
        let mut res = Vec::new();
        // if we're in blocking mode and the queue is empty, then we return immediately to avoid blocking
        while !self.is_empty() {
            match self.receive() {
                Ok(Some(msg)) => res.push(msg),
                Ok(None) => break, // The channel is empty, so we break
                Err(_e) => break,  // The channel is disconnected, so we break
            }
        }

        res
    }

    /// Check if the channel is in blocking mode.
    pub fn is_blocking(&self) -> bool {
        self.blocking
    }
    /// Check if the channel is empty.
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

/// Struct defining the sender side of a channel.
/// This struct is implemented by the channel backend.
/// The channel backend is selected at compile time by the feature flag.
pub struct OutputChannel<T> {
    tx: Box<dyn Sender<T> + Sync + Send>,
}
impl<T> OutputChannel<T>
where
    T: Send,
{
    /// Send a message to the channel.
    pub fn send(&self, msg: T) -> Result<(), SenderError> {
        self.tx.send(msg)
    }
}

/// Channel factory.
/// This struct is used to create a channel.
/// The channel backend is selected at compile time by the feature flag.
pub struct Channel;

impl Channel {
    /// Create a new channel.
    pub fn channel<T: Send + 'static>(wait_policy: WaitPolicy) -> (InputChannel<T>, OutputChannel<T>) {
        let blocking = match wait_policy {
            WaitPolicy::Active => false,
            WaitPolicy::Passive => true,
        };

        let (rx, tx) = backend::Channel::channel(blocking);
        (InputChannel { rx, blocking }, OutputChannel { tx })
    }
}
