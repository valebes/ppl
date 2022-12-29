use crate::{channel::ChannelError, task::Task};

pub trait Node<TIn: Send, TCollected> {
    fn send(&self, input: Task<TIn>) -> Result<(), ChannelError>;
    fn collect(self) -> Option<TCollected>;
}
