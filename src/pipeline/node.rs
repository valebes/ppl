use crate::{mpsc::err::SenderError, task::Message};

pub trait Node<TIn, TCollected>
where
    TIn: Send,
{
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), SenderError>;
    fn collect(self) -> Option<TCollected>;
    fn get_num_of_replicas(&self) -> usize;
}
