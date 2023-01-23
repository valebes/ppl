use crate::{channel::ChannelError, task::Task};

pub trait Node<TIn: Send, TCollected> {
    fn send(&self, input: Task<TIn>, rec_id: usize) -> Result<(), ChannelError>;
    fn collect(self) -> Option<TCollected>;
    fn get_num_of_replicas(&self) -> usize;
}
