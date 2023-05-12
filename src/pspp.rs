pub use crate::pipeline::{node::Node, out_node::*};

pub struct Parallel<
    TOut: Send + 'static,
    TCollected,
    TNext: Node<TOut, TCollected> + Send + Sync + 'static,
> {
    first_block: Option<OutNode<TOut, TCollected, TNext>>,
}
impl<TOut: Send + 'static, TCollected, TNext: Node<TOut, TCollected> + Send + Sync + 'static>
    Parallel<TOut, TCollected, TNext>
{
    /// Creates a new parallel pipeline.
    pub fn new(first_block: OutNode<TOut, TCollected, TNext>) -> Parallel<TOut, TCollected, TNext> {
        Parallel {
            first_block: Some(first_block),
        }
    }
    /// Starts the pipeline.
    pub fn start(&mut self) {
        match &mut self.first_block {
            Some(block) => {
                block.start();
            }
            None => panic!("Error: Cannot start the pipeline!"),
        }
    }
    /// Waits for the pipeline to terminate and collects the results.
    pub fn wait_and_collect(&mut self) -> Option<TCollected> {
        match &mut self.first_block {
            Some(_block) => {
                let block = self.first_block.take();
                match block {
                    Some(block) => Node::<TOut, TCollected>::collect(block),
                    None => None,
                }
            }
            None => None,
        }
    }
}

impl<TOut: Send + 'static, TCollected, TNext: Node<TOut, TCollected> + Send + Sync + 'static> Drop
    for Parallel<TOut, TCollected, TNext>
{
    fn drop(&mut self) {
        match &mut self.first_block {
            Some(_block) => {
                let block = self.first_block.take();
                if let Some(block) = block {
                    block.terminate();
                }
            }
            None => (),
        }
    }
}

#[macro_export]
macro_rules! propagate {
    ($id:expr, $s1:expr) => {
        {
            let mut block = InNode::new($id, Box::new($s1), get_global_orchestrator());
            block
        }
    };

    ($id:expr, $s1:expr $(, $tail:expr)*) => {
        {
            let node = ($s1);
            let replicas = node.number_of_replicas();
            let mut block = InOutNode::new($id, Box::new(node),
                propagate!($id + (1 * replicas), $($tail),*),
                get_global_orchestrator());
            block
        }
    };
}

/// Creates a new parallel pipeline.
/// The macro takes as input a list of stages.
/// The stages are executed in parallel.
/// The output of the pipeline is the output of the last stage.
/// The macro returns a `Parallel` struct that can be used to start and wait for the pipeline.
#[macro_export]
macro_rules! parallel {
    ($s1:expr $(, $tail:expr)*) => {
        {
            let orchestrator = get_global_orchestrator();
            let mut block = OutNode::new(0, Box::new($s1),
                propagate!(1, $($tail),*), orchestrator);

            let mut pipeline = Parallel::new(block);
            pipeline
        }
    };

}
