use crate::{task::Task, channel::ChannelError};

pub trait Node<TIn: Send, TCollected = ()> {
    fn send(&self, input: Task<TIn>) -> Result<(), ChannelError>;
    fn collect(self) -> Option<TCollected>;
}