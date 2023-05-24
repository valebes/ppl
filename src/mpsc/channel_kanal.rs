use kanal::{Receiver, Sender};

use super::{
    channel,
    err::{ReceiverError, SenderError},
};

pub struct KanalInputChannel<T> {
    rx: Receiver<T>,
}
impl<T> channel::Receiver<T> for KanalInputChannel<T>
where
    T: Send,
{
    fn receive(&self) -> Result<Option<T>, ReceiverError> {
        let err = self.rx.try_recv();
        match err {
            Ok(msg) => Ok(msg),
            Err(e) => match e {
                kanal::ReceiveError::Closed => Err(ReceiverError),
                kanal::ReceiveError::SendClosed => Err(ReceiverError),
            },
        }
    }

    fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

pub struct KanalBlockingInputChannel<T> {
    rx: Receiver<T>,
}
impl<T> channel::Receiver<T> for KanalBlockingInputChannel<T>
where
    T: Send,
{
    fn receive(&self) -> Result<Option<T>, ReceiverError> {
        let err = self.rx.recv();
        match err {
            Ok(msg) => Ok(Some(msg)),
            Err(_e) => Err(ReceiverError),
        }
    }

    fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

pub struct KanalOutputChannel<T> {
    tx: Sender<T>,
}

impl<T> channel::Sender<T> for KanalOutputChannel<T>
where
    T: Send,
{
    fn send(&self, msg: T) -> Result<(), SenderError> {
        let err = self.tx.send(msg);
        match err {
            Ok(()) => Ok(()),
            Err(_e) => Err(SenderError),
        }
    }
}
impl<T> Clone for KanalOutputChannel<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

/// Channel is a factory for creating channels
/// It is a wrapper around kanal channel.
pub struct Channel;

impl Channel {
    pub fn channel<T>(
        blocking: bool,
    ) -> (
        Box<dyn channel::Receiver<T> + Sync + Send>,
        Box<dyn channel::Sender<T> + Sync + Send>,
    )
    where
        T: Send + 'static,
    {
        let (tx, rx) = kanal::unbounded();
        if blocking {
            (
                Box::new(KanalBlockingInputChannel { rx }),
                Box::new(KanalOutputChannel { tx }),
            )
        } else {
            (
                Box::new(KanalInputChannel { rx }),
                Box::new(KanalOutputChannel { tx }),
            )
        }
    }
}
