use crate::{task::Message, channel::err::ChannelError};

pub trait Node<TIn: Send, TCollected> {
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), ChannelError>;
    fn collect(self) -> Option<TCollected>;
    fn get_num_of_replicas(&self) -> usize;
}
