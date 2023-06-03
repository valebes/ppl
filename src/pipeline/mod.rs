//! Parallel pipeline
//!
//! The pipeline module contains the [`Pipeline`] struct and the `parallel!` macro.
//! The `Parallel` struct represents a pipeline of nodes. The `parallel!` macro
//! is used to create that pipeline.
//! There are also some traits and structs that are used to create the pipeline.
//! For example, the [`Node`] trait is used to define a node of the pipeline.
//!
pub mod node;

use node::{Node, OutNode};
/// The [`Pipeline`] struct represents a pipeline of nodes.
///
/// The [`Pipeline`] struct is generic over the type of the output of the pipeline
/// and the type of the collected result, is created using the `parallel!` macro.
pub struct Pipeline<TOut, TCollected, TNext>
where
    TOut: Send + 'static,
    TNext: Node<TOut, TCollected> + Send + Sync + 'static,
{
    first_block: Option<OutNode<TOut, TCollected, TNext>>,
}
impl<TOut, TCollected, TNext> Pipeline<TOut, TCollected, TNext>
where
    TOut: Send + 'static,
    TNext: Node<TOut, TCollected> + Send + Sync + 'static,
{
    /// Creates a new [`Pipeline`] struct.
    ///
    /// # Arguments
    ///
    /// * `source` - The first stage of the pipeline.
    pub fn new(source: OutNode<TOut, TCollected, TNext>) -> Pipeline<TOut, TCollected, TNext> {
        Pipeline {
            first_block: Some(source),
        }
    }
    /// Starts the pipeline.
    ///
    /// This method starts the pipeline by starting the first stage.
    ///
    /// # Panics
    ///
    /// This method panics if the pipeline is empty.
    pub fn start(&mut self) {
        match &mut self.first_block {
            Some(block) => {
                block.start();
            }
            None => panic!("Error: Cannot start the pipeline!"),
        }
    }
    /// Waits for the pipeline to finish and collects the result.
    ///
    /// This method waits for the pipeline to finish and collects the result.
    ///
    /// # Panics
    ///
    /// This method panics if the pipeline is empty.
    pub fn wait_and_collect(mut self) -> Option<TCollected> {
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

impl<TOut, TCollected, TNext> Drop for Pipeline<TOut, TCollected, TNext>
where
    TOut: Send + 'static,
    TNext: Node<TOut, TCollected> + Send + Sync + 'static,
{
    /// Drop the pipeline.
    ///     
    /// This method terminates the pipeline by terminating the first stage.
    /// After this the termination is propagated to the next stages and so on.
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

/// Macro that allows to create a parallel pipeline.
///
/// The macro takes as input a list of stages. Each stage is a node of the pipeline.
///
/// The first stage is the source of the pipeline. It can be any struct that implements the
/// [`node::Out`] trait. The last stage is the sink of the pipeline. It can be any struct that
/// implements the [`node::In`] trait. The stages in the middle are the processing stages of the
/// pipeline. They can be any struct that implements the [`node::InOut`] trait.
///
/// The stages in the middle can be replicated. This means that the same stage can be used
/// multiple times in the pipeline. This is useful when the processing stage is a heavy
/// computation and the input data is large. In this case, the input data can be split
/// into multiple chunks and each chunk can be processed by a different replica of the
/// processing stage. The number of replicas of a stage is defined by the
/// [`node::InOut::number_of_replicas`] method of the [`node::InOut`] trait.
///
///
/// The macro returns a [`Pipeline`] struct that can be used to start and wait for the pipeline.
///
/// # Example
///
/// ```
/// use ppl::prelude::*;
/// use ppl::templates::misc::*;
///
/// let mut pipeline = pipeline![
///      SourceIter::build(0..10),
///      Sequential::build(|el: usize| -> usize { el * 2 }),
///      SinkVec::build()
/// ];
/// pipeline.start();
/// let res = pipeline.wait_and_collect().unwrap();
///
/// assert_eq!(res, vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18]);
/// ```
#[macro_export]
macro_rules! pipeline {
    ($s1:expr $(, $tail:expr)*) => {
        {
            let orchestrator = get_global_orchestrator();
            let mut block = OutNode::new(0, Box::new($s1),
                propagate!(1, $($tail),*), orchestrator);

            let mut pipeline = Pipeline::new(block);
            pipeline
        }
    };

}
