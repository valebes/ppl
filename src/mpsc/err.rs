use std::{error, fmt};

/// An error returned from the [`receive`] method.
///
/// A message could not be received because the channel is empty and disconnected.
///
/// [`receive`]: super::InputChannel::receive
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct ReceiverError;

/// An error returned from the [`send`] method.
///
/// A message could not be sent because the channel is disconnected or is full.
///
/// [`send`]: super::OutputChannel::send
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SenderError;

impl fmt::Debug for ReceiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt("Can't receive the msg.", f)
    }
}
impl fmt::Display for ReceiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt("Can't receive the msg.", f)
    }
}

impl error::Error for ReceiverError {}

impl fmt::Debug for SenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt("Can't send the msg.", f)
    }
}

impl fmt::Display for SenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt("Can't send the msg.", f)
    }
}

impl error::Error for SenderError {}
