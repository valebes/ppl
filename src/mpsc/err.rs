use std::{error, fmt};

/// A message could not be received because the channel is empty and disconnected.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct ReceiverError;

/// A message could not be sent because the channel is disconnected or is full.
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
