use crossbeam_channel::{Receiver, RecvError, Sender, TryRecvError};
use std::{error::Error, fmt};

#[derive(Debug)]
pub struct ChannelError {
    details: String,
}

impl ChannelError {
    fn new(msg: &str) -> ChannelError {
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

pub struct InputChannel<T> {
    rx: Receiver<T>,
    blocking: bool,
}
impl<T: Send> InputChannel<T> {
    fn non_block_receive(&self) -> Result<T, TryRecvError> {
        self.rx.try_recv()
    }
    fn block_receive(&self) -> Result<T, RecvError> {
        self.rx.recv()
    }

    pub fn receive(&self) -> Result<Option<T>, ChannelError> {
        if self.blocking {
            let err = self.block_receive();
            match err {
                Ok(msg) => Ok(Some(msg)),
                Err(e) => Err(ChannelError::new(&e.to_string())),
            }
        } else {
            let err = self.non_block_receive();
            match err {
                Ok(msg) => Ok(Some(msg)),
                Err(e) => match e {
                    TryRecvError::Empty => Ok(None),
                    TryRecvError::Disconnected => Err(ChannelError::new(&e.to_string())),
                },
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

pub struct OutputChannel<T> {
    tx: Sender<T>,
}

impl<T: Send> OutputChannel<T> {
    pub fn send(&self, msg: T) -> Result<(), ChannelError> {
        let err = self.tx.send(msg);
        match err {
            Ok(()) => Ok(()),
            Err(e) => Err(ChannelError::new(&e.to_string())),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tx.is_empty()
    }
}

impl<T> Clone for OutputChannel<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

pub struct Channel {}

impl Channel {
    pub fn channel<T: Send>(blocking: bool) -> (InputChannel<T>, OutputChannel<T>) {
        let (tx, rx) = crossbeam_channel::unbounded();
        (
            InputChannel {
                rx,
                blocking,
            },
            OutputChannel { tx },
        )
    }
}
