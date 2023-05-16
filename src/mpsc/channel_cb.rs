use super::{
    channel,
    err::{ReceiverError, SenderError},
};
use crossbeam_channel::{Receiver, Sender, TryRecvError};

pub struct CBInputChannel<T> {
    rx: Receiver<T>,
}
impl<T: Send> channel::Receiver<T> for CBInputChannel<T> {
    fn receive(&self) -> Result<Option<T>, ReceiverError> {
        let err = self.rx.try_recv();
        match err {
            Ok(msg) => Ok(Some(msg)),
            Err(e) => match e {
                TryRecvError::Empty => Ok(None),
                TryRecvError::Disconnected => Err(ReceiverError),
            },
        }
    }

    fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

pub struct CBBlockingInputChannel<T> {
    rx: Receiver<T>,
}
impl<T: Send> channel::Receiver<T> for CBBlockingInputChannel<T> {
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

pub struct CBOutputChannel<T> {
    tx: Sender<T>,
}

impl<T: Send> channel::Sender<T> for CBOutputChannel<T> {
    fn send(&self, msg: T) -> Result<(), SenderError> {
        let err = self.tx.send(msg);
        match err {
            Ok(()) => Ok(()),
            Err(_e) => Err(SenderError),
        }
    }
}
impl<T> Clone for CBOutputChannel<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

/// Channel is a factory for creating channels.
/// It is a wrapper around crossbeam_channel.
pub struct Channel;

impl Channel {
    /// Create a new channel using crossbeam_channel.
    pub fn channel<T: Send + 'static>(
        blocking: bool,
    ) -> (
        Box<dyn channel::Receiver<T> + Sync + Send>,
        Box<dyn channel::Sender<T> + Sync + Send>,
    ) {
        let (tx, rx) = crossbeam_channel::unbounded();
        if blocking {
            (
                Box::new(CBBlockingInputChannel { rx }),
                Box::new(CBOutputChannel { tx }),
            )
        } else {
            (
                Box::new(CBInputChannel { rx }),
                Box::new(CBOutputChannel { tx }),
            )
        }
    }
}
