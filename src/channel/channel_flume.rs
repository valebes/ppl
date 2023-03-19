use flume::{Receiver, Sender, TryRecvError};

use super::{channel, err::ChannelError};

pub struct FlumeInputChannel<T> {
    rx: Receiver<T>,
}
impl<T: Send> channel::Receiver<T> for FlumeInputChannel<T> {
    fn receive(&self) -> Result<Option<T>, ChannelError> {
        let err = self.rx.try_recv();
        match err {
            Ok(msg) => Ok(Some(msg)),
            Err(e) => match e {
                TryRecvError::Empty => Ok(None),
                TryRecvError::Disconnected => Err(ChannelError::new(&e.to_string())),
            },
        }
    }

    fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

pub struct FlumeBlockingInputChannel<T> {
    rx: Receiver<T>,
}
impl<T: Send> channel::Receiver<T> for FlumeBlockingInputChannel<T> {
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

pub struct FlumeOutputChannel<T> {
    tx: Sender<T>,
}

impl<T: Send> channel::Sender<T> for FlumeOutputChannel<T> {
    fn send(&self, msg: T) -> Result<(), ChannelError> {
        let err = self.tx.send(msg);
        match err {
            Ok(()) => Ok(()),
            Err(e) => Err(ChannelError::new(&e.to_string())),
        }
    }
}
impl<T> Clone for FlumeOutputChannel<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

pub struct Channel;

impl Channel {
    pub fn channel<T: Send + 'static>(
        blocking: bool,
    ) -> (
        Box<dyn channel::Receiver<T> + Sync + Send>,
        Box<dyn channel::Sender<T> + Sync + Send>,
    ) {
        let (tx, rx) = flume::unbounded();
        if blocking {
            (
                Box::new(FlumeBlockingInputChannel { rx }),
                Box::new(FlumeOutputChannel { tx: tx }),
            )
        } else {
            (
                Box::new(FlumeInputChannel { rx }),
                Box::new(FlumeOutputChannel { tx: tx }),
            )
        }
    }
}
