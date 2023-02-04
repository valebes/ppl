use crate::node::{node::Node, out_node::*};

pub struct Pipeline<TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    first_block: OutNode<TOut, TCollected, TNext>,
}
impl<TOut: Send + 'static, TCollected, TNext: Node<TOut, TCollected> + Send + Sync + 'static>
    Pipeline<TOut, TCollected, TNext>
{
    pub fn new(first_block: OutNode<TOut, TCollected, TNext>) -> Pipeline<TOut, TCollected, TNext> {
        Pipeline { first_block }
    }
    pub fn start(&mut self) {
        let err = self.first_block.start();
        if err.is_err() {
            panic!("Error: Cannot start thread!");
        }
    }

    pub fn collect(self) -> Option<TCollected> {
        Node::<TOut, TCollected>::collect(self.first_block)
    }
}

#[macro_export]
macro_rules! pipeline_propagate {
    ($id:expr, $s1:expr) => {
        {
            let mut block = InNode::new($id, $s1, false).unwrap();
            block
        }
    };

    ($id:expr, $s1:expr $(, $tail:expr)*) => {
        {
            let mut block = InOutNode::new($id, $s1,
                pipeline_propagate!($id + (1 * $s1.number_of_replicas()), $($tail),*),
                false).unwrap();
            block
        }
    };
}

#[macro_export]
macro_rules! pipeline {
    ($s1:expr $(, $tail:expr)*) => {
        {
            let mut block = OutNode::new(0, $s1,
                pipeline_propagate!(1, $($tail),*)).unwrap();

            let mut pipeline = Pipeline::new(block);
            pipeline.start();
            pipeline
        }
    };
}
