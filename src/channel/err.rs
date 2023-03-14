use std::{error::Error, fmt};

#[derive(Debug)]
pub struct ChannelError {
    details: String,
}

impl ChannelError {
    pub fn new(msg: &str) -> ChannelError {
        ChannelError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for ChannelError {
    fn description(&self) -> &str {
        &self.details
    }
}