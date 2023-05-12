use kanal::{Receiver, Sender};

use super::{channel, err::ChannelError};

pub struct KanalInputChannel<T> {
    rx: Receiver<T>,
}
impl<T: Send> channel::Receiver<T> for KanalInputChannel<T> {
    fn receive(&self) -> Result<Option<T>, ChannelError> {
        let err = self.rx.try_recv();
        match err {
            Ok(msg) => Ok(msg),
            Err(e) => match e {
                kanal::ReceiveError::Closed => Err(ChannelError::new(&e.to_string())),
                kanal::ReceiveError::SendClosed => Err(ChannelError::new(&e.to_string())),
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
impl<T: Send> channel::Receiver<T> for KanalBlockingInputChannel<T> {
    fn receive(&self) -> Result<Option<T>, ChannelError> {
        let err = self.rx.recv();
        match err {
            Ok(msg) => Ok(Some(msg)),
            Err(e) => Err(ChannelError::new(&e.to_string())),
        }
    }

    fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

pub struct KanalOutputChannel<T> {
    tx: Sender<T>,
}

impl<T: Send> channel::Sender<T> for KanalOutputChannel<T> {
    fn send(&self, msg: T) -> Result<(), ChannelError> {
        let err = self.tx.send(msg);
        match err {
            Ok(()) => Ok(()),
            Err(e) => Err(ChannelError::new(&e.to_string())),
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
    pub fn channel<T: Send + 'static>(
        blocking: bool,
    ) -> (
        Box<dyn channel::Receiver<T> + Sync + Send>,
        Box<dyn channel::Sender<T> + Sync + Send>,
    ) {
        let (tx, rx) = kanal::unbounded();
        if blocking {
            (
                Box::new(KanalBlockingInputChannel { rx }),
                Box::new(KanalOutputChannel { tx: tx }),
            )
        } else {
            (
                Box::new(KanalInputChannel { rx }),
                Box::new(KanalOutputChannel { tx: tx }),
            )
        }
    }
}
