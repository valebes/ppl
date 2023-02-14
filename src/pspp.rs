use crate::node::{node::Node, out_node::*};

pub struct Parallel<TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    first_block: OutNode<TOut, TCollected, TNext>,
}
impl<TOut: Send + 'static, TCollected, TNext: Node<TOut, TCollected> + Send + Sync + 'static>
    Parallel<TOut, TCollected, TNext>
{
    pub fn new(first_block: OutNode<TOut, TCollected, TNext>) -> Parallel<TOut, TCollected, TNext> {
        Parallel { first_block }
    }
    pub fn start(&mut self) {
        let err = self.first_block.start();
        if err.is_err() {
            panic!("Error: Cannot start thread!");
        }
    }

    pub fn wait_and_collect(self) -> Option<TCollected> {
        Node::<TOut, TCollected>::collect(self.first_block)
    }
}

#[macro_export]
macro_rules! propagate {
    ($id:expr, $s1:expr) => {
        {
            let mut block = InNode::new($id, Box::new($s1), false, false).unwrap();
            block
        }
    };

    ($id:expr, $s1:expr $(, $tail:expr)*) => {
        {
            let mut block = InOutNode::new($id, Box::new($s1),
                propagate!($id + (1 * $s1.number_of_replicas()), $($tail),*),
                false, false).unwrap();
            block
        }
    };
}

#[macro_export]
macro_rules! parallel {
    ($s1:expr $(, $tail:expr)*) => {
        {
            let mut block = OutNode::new(0, Box::new($s1),
                propagate!(1, $($tail),*), false).unwrap();

            let mut pipeline = Parallel::new(block);
            //pipeline.start();
            pipeline
        }
    };
}
