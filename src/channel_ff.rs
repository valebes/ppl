use std::{
    error::Error,
    fmt,
    sync::{
        Mutex,
    },
};

use ff_buffer::{self, FFReceiver, FFSender};

// Working in progress.

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

struct InputChannel<T> {
    rx: Mutex<FFReceiver<T>>,
}

impl<T: Send> InputChannel<T> {
    fn receive(&self) -> Option<T> {
        let ch = self.rx.lock().unwrap();
        match ch.try_pop() {
            Some(res) => Some(Box::into_inner(res)),
            None => None,
        }
    }
    fn block_receive(&self) -> T {
        let ch = self.rx.lock().unwrap();
        Box::into_inner(ch.pop())
    }
}

struct OutputChannel<T> {
    tx: Mutex<FFSender<T>>,
}

impl<T: Send> OutputChannel<T> {
    fn send(&self, msg: T) -> Option<&str>{
        let ch = self.tx.lock().unwrap();
        let res = ch.push(Box::new(msg));
        match res {
            Some(_) => Some("Can't send the msg."),
            None => None,
        }
    }
}

pub struct Channel<T> {
    rx: InputChannel<T>,
    tx: OutputChannel<T>,
    blocking: bool,
}

impl<T: Send> Channel<T> {
    pub fn new(blocking: bool) -> Channel<T> {
        let (tx, rx) = ff_buffer::build::<T>();
        Channel {
            rx: InputChannel { rx: Mutex::new(rx) },
            tx: OutputChannel { tx: Mutex::new(tx) },
            blocking: blocking,
        }
    }

    pub fn send(&self, msg: T) -> Result<(), ChannelError> {
        let err = self.tx.send(msg);
        match err {
            None => Ok(()),
            Some(e) => Err(ChannelError::new(&e.to_string())),
        }
    }

    pub fn receive(&self) -> Result<T, ChannelError> {
        if self.blocking {
            Ok(self.rx.block_receive())
        } else {
            let err = self.rx.receive();
            match err {
                Some(msg) => Ok(msg),
                None => Err(ChannelError::new("The channel is empty.")),
            }
        }
    }
}
