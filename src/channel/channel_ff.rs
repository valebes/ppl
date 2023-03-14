use ff_buffer::{self, FFReceiver, FFSender};
use std::{sync::Mutex};

use super::err::ChannelError;


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

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
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
                    Some(_) => Err(ChannelError::new("Can't send the msg.")),
                    None => Ok(()),
                }
            }
            Err(_) => panic!("Cannot lock mutex on this channel"),
        }
    }
}

pub struct Channel {}

impl Channel {
    pub fn channel<T: Send>(blocking: bool) -> (InputChannel<T>, OutputChannel<T>) {
        let (tx, rx) = ff_buffer::build::<T>();
        (
            InputChannel {
                rx,
                blocking,
            },
            OutputChannel { tx: Mutex::new(tx) },
        )
    }
}
