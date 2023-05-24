mod in_node;
mod inout_node;
mod out_node;

pub use in_node::{In, InNode};
pub use inout_node::{InOut, InOutNode};
pub use out_node::{Out, OutNode};

use crate::{mpsc::err::SenderError, task::Message};

pub trait Node<TIn, TCollected>
where
    TIn: Send,
{
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), SenderError>;
    fn collect(self) -> Option<TCollected>;
    fn get_num_of_replicas(&self) -> usize;
}
