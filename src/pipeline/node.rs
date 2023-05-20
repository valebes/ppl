use crate::{mpsc::err::SenderError, task::Message};

pub trait Node<TIn: Send, TCollected> {
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), SenderError>;
    fn collect(self) -> Option<TCollected>;
    fn get_num_of_replicas(&self) -> usize;
}
