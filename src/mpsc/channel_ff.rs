use ff_buffer::{self, FFReceiver, FFSender};
use std::sync::Mutex;

use super::{
    channel::{Receiver, Sender},
    err::{ReceiverError, SenderError},
};

pub struct FFInputChannel<T> {
    rx: FFReceiver<T>,
}
impl<T> Receiver<T> for FFInputChannel<T>
where
    T: Send,
{
    fn receive(&self) -> Result<Option<T>, ReceiverError> {
        match self.rx.try_pop() {
            Some(boxed) => Ok(Some(Box::into_inner(boxed))),
            None => {
                if self.rx.is_disconnected() && self.rx.is_empty() {
                    Err(ReceiverError)
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

pub struct FFBlockingInputChannel<T> {
    rx: FFReceiver<T>,
}
impl<T> Receiver<T> for FFBlockingInputChannel<T>
where
    T: Send,
{
    fn receive(&self) -> Result<Option<T>, ReceiverError> {
        match self.rx.pop() {
            Some(boxed) => Ok(Some(Box::into_inner(boxed))),
            None => Err(ReceiverError),
        }
    }

    fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

pub struct FFOutputChannel<T> {
    tx: Mutex<FFSender<T>>,
}

impl<T> Sender<T> for FFOutputChannel<T>
where
    T: Send,
{
    fn send(&self, msg: T) -> Result<(), SenderError> {
        let mtx = self.tx.lock();
        match mtx {
            Ok(ch) => {
                let err = ch.push(Box::new(msg));
                match err {
                    Some(_) => Err(SenderError),
                    None => Ok(()),
                }
            }
            Err(_) => panic!("Cannot lock mutex on this channel"),
        }
    }
}

/// Channel is a factory for creating channels.
///  It is a wrapper around the fastflow channel implementation.
pub struct Channel;

impl Channel {
    pub fn channel<T>(
        blocking: bool,
    ) -> (
        Box<dyn Receiver<T> + Sync + Send>,
        Box<dyn Sender<T> + Sync + Send>,
    )
    where
        T: Send + 'static,
    {
        let (tx, rx) = ff_buffer::build::<T>();
        if blocking {
            (
                Box::new(FFBlockingInputChannel { rx }),
                Box::new(FFOutputChannel { tx: Mutex::new(tx) }),
            )
        } else {
            (
                Box::new(FFInputChannel { rx }),
                Box::new(FFOutputChannel { tx: Mutex::new(tx) }),
            )
        }
    }
}
