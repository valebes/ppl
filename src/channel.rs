use std::{
    error::Error,
    fmt,
    sync::{
        mpsc::{channel, Receiver, RecvError, SendError, Sender, TryRecvError},
        Mutex,
    },
};

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
    rx: Mutex<Receiver<T>>,
}

impl<T: Send> InputChannel<T> {
    fn receive(&self) -> Result<T, TryRecvError> {
        let ch = self.rx.lock().unwrap();
        ch.try_recv()
    }
    fn block_receive(&self) -> Result<T, RecvError> {
        let ch = self.rx.lock().unwrap();
        ch.recv()
    }
}

struct OutputChannel<T> {
    tx: Mutex<Sender<T>>,
}

impl<T: Send> OutputChannel<T> {
    fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let ch = self.tx.lock().unwrap();
        ch.send(msg)
    }
}

pub struct Channel<T> {
    rx: InputChannel<T>,
    tx: OutputChannel<T>,
    blocking: bool,
}

impl<T: Send> Channel<T> {
    pub fn new(blocking: bool) -> Channel<T> {
        let (tx, rx) = channel();
        Channel {
            rx: InputChannel { rx: Mutex::new(rx) },
            tx: OutputChannel { tx: Mutex::new(tx) },
            blocking: blocking,
        }
    }

    pub fn send(&self, msg: T) -> Result<(), ChannelError> {
        let err = self.tx.send(msg);
        match err {
            Ok(()) => Ok(()),
            Err(e) => Err(ChannelError::new(&e.to_string())),
        }
    }

    pub fn receive(&self) -> Result<T, ChannelError> {
        if self.blocking {
            let err = self.rx.block_receive();
            match err {
                Ok(msg) => Ok(msg),
                Err(e) => Err(ChannelError::new(&e.to_string())),
            }
        } else {
            let err = self.rx.receive();
            match err {
                Ok(msg) => Ok(msg),
                Err(e) => Err(ChannelError::new(&e.to_string())),
            }
        }
    }
}
