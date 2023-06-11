mod in_node;
mod inout_node;
mod out_node;

pub use in_node::{In, InNode};
pub use inout_node::{InOut, InOutNode};
pub use out_node::{Out, OutNode};

use crate::{mpsc::err::SenderError, task::Message};

/// Trait that defines the common behaviors of a node.
pub trait Node<TIn, TCollected>
where
    TIn: Send,
{
    /// Send a message to the node.
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), SenderError>;
    /// Collect the final result stored in the last stage of the pipeline where is located this node.
    fn collect(self) -> Option<TCollected>;
    /// Fetch the number of replicas of this node.
    fn get_num_of_replicas(&self) -> usize;
}
