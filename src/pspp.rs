use crate::node::{node::Node, out_node::*};

pub struct Parallel<TOut: Send + 'static, TCollected, TNext: Node<TOut, TCollected> + Send + Sync + 'static > {
    first_block: Option<OutNode<TOut, TCollected, TNext>>,
}
impl<TOut: Send + 'static, TCollected, TNext: Node<TOut, TCollected> + Send + Sync + 'static>
    Parallel<TOut, TCollected, TNext>
{
    pub fn new(first_block: OutNode<TOut, TCollected, TNext>) -> Parallel<TOut, TCollected, TNext> {
        Parallel { first_block: Some(first_block) }
    }
    pub fn start(&mut self) {
        match &mut self.first_block {
            Some(block) =>{
                let err = block.start();
                if err.is_err() {
                    panic!("Error: Cannot start thread!");
                }
            },
            None =>  panic!("Error: Cannot start the pipeline!"),
        }

    }

    pub fn wait_and_collect(&mut self) -> Option<TCollected> {
        match &mut self.first_block {
            Some(_block) => { 
                let block = std::mem::replace(&mut self.first_block, None);
                if block.is_some() {
                    Node::<TOut, TCollected>::collect(block.unwrap())
                } else {
                    None
                }             
            },
            None => None,
        }
    }
}

impl<TOut: Send + 'static, TCollected, TNext: Node<TOut, TCollected> + Send + Sync + 'static> Drop for
    Parallel<TOut, TCollected, TNext> {
        fn drop(&mut self) {
            match &mut self.first_block {
                Some(_block) => { 
                    let block = std::mem::replace(&mut self.first_block, None);
                    if block.is_some() {
                        let _err= block.unwrap().terminate(); 
                    }               
                },
                None => (),
            }
            println!("pipeline dropped")
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
            pipeline
        }
    };
}
