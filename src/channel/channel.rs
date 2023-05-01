use super::err::ChannelError;

#[cfg(feature = "ff")]
use super::channel_ff as backend;

#[cfg(feature = "crossbeam")]
use super::channel_cb as backend;

#[cfg(feature = "kanal")]
use super::channel_kanal as backend;

#[cfg(feature = "flume")]
use super::channel_flume as backend;

pub trait Receiver<T> {
    fn receive(&self) -> Result<Option<T>, ChannelError>;
    fn is_empty(&self) -> bool;
}

pub trait Sender<T> {
    fn send(&self, msg: T) -> Result<(), ChannelError>;
}

pub struct InputChannel<T> {
    rx: Box<dyn Receiver<T> + Sync + Send>,
}
impl<T: Send> InputChannel<T> {
    pub fn receive(&self) -> Result<Option<T>, ChannelError> {
        self.rx.receive()
    }

    pub fn receive_all(&self) -> Result<Vec<T>, ChannelError> {
        let mut res = Vec::new();
        while let Some(msg) = self.receive()? {
            res.push(msg);
        }
        Ok(res)
    }

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

pub struct OutputChannel<T> {
    tx: Box<dyn Sender<T> + Sync + Send>,
}
impl<T: Send> OutputChannel<T> {
    pub fn send(&self, msg: T) -> Result<(), ChannelError> {
        self.tx.send(msg)
    }
}

pub struct Channel;

impl Channel {
    pub fn channel<T: Send + 'static>(blocking: bool) -> (InputChannel<T>, OutputChannel<T>) {
        let (rx, tx) = backend::Channel::channel(blocking);
        (InputChannel { rx }, OutputChannel { tx })
    }
}
