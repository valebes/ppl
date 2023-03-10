use ff_buffer::{self, FFReceiver, FFSender};
use std::{error::Error, fmt, sync::Mutex};

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
    rx: FFReceiver<T>,
    blocking: bool,
}
impl<T: Send> InputChannel<T> {
    fn non_block_receive(&self) -> Option<Box<T>> {
        self.rx.try_pop()
    }
    fn block_receive(&self) -> Box<T> {
        self.rx.pop()
    }

    pub fn receive(&self) -> Result<Option<T>, ChannelError> {
        if self.blocking {
            Ok(Some(Box::into_inner(self.block_receive())))
        } else {
            match self.non_block_receive() {
                Some(boxed) => Ok(Some(Box::into_inner(boxed))),
                None => Ok(None),
            }
        }
    }
}

pub struct OutputChannel<T> {
    tx: Mutex<FFSender<T>>,
}

impl<T: Send> OutputChannel<T> {
    pub fn send(&self, msg: T) -> Result<(), ChannelError> {
        let mtx = self.tx.lock();
        match mtx {
            Ok(ch) => {
                let err = ch.push(Box::new(msg));
                match err {
                    Some(_) => Err(ChannelError::new(&"Can't send the msg.".to_string())),
                    None => Ok(()),
                }
            },
            Err(_) => panic!("Cannot lock mutex on this channel"),
        }
    }
}

pub struct Channel {}

impl Channel {
    pub fn new<T: Send>(blocking: bool) -> (InputChannel<T>, OutputChannel<T>) {
        let (tx, rx) = ff_buffer::build::<T>();
        (
            InputChannel {
                rx: rx,
                blocking: blocking,
            },
            OutputChannel { tx: Mutex::new(tx) },
        )
    }
}
