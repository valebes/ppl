use std::marker::PhantomData;
use std::sync::Arc;

use log::{trace, warn};

use crate::channel::ChannelError;
use crate::task::Task;
use crate::thread::{Thread, ThreadError};

use super::node::Node;

/*
Public API
*/
pub trait Out<TOut: 'static + Send> {
    fn run(&mut self) -> Option<TOut>;
}

pub struct OutNode<TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    thread: Thread,
    next_node: Arc<TNext>,
    phantom: PhantomData<(TOut, TCollected)>,
}

impl<
        TIn: Send,
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Send + Sync + 'static,
    > Node<TIn, TCollected> for OutNode<TOut, TCollected, TNext>
{
    fn send(&self, _input: Task<TIn>, rec_id: usize) -> Result<(), ChannelError> {
        Ok(())
    }

    fn collect(mut self) -> Option<TCollected> {
        let err = self.wait();
        if err.is_err() {
            panic!("Error: Cannot wait thread.");
        }
        match Arc::try_unwrap(self.next_node) {
            Ok(nn) => nn.collect(),
            Err(_) => panic!("Error: Cannot collect results"),
        }
    }

    fn get_num_of_replicas(&self) -> usize {
        1
    }
}

impl<TOut: Send + 'static, TCollected, TNext: Node<TOut, TCollected> + Send + Sync + 'static>
    OutNode<TOut, TCollected, TNext>
{
    pub fn new(
        id: usize,
        handler: Box<dyn Out<TOut> + Send + Sync>,
        next_node: TNext,
    ) -> Result<OutNode<TOut, TCollected, TNext>, ()> {
        trace!("Created a new OutNode!");

        let next_node = Arc::new(next_node);

        let nn = Arc::clone(&next_node);

        let thread = Thread::new(
            id,
            move || {
                Self::rts(handler, &nn);
            },
            false,
        );

        let node = OutNode {
            thread: thread,
            next_node: next_node,
            phantom: PhantomData,
        };

        Ok(node)
    }

    fn rts(mut node: Box<dyn Out<TOut>>, nn: &TNext) {
        let mut order = 0;
        let mut counter = 0;
        loop {
            if counter >= nn.get_num_of_replicas() {
                counter = 0;
            }
            let res = node.run();
            match res {
                Some(output) => {
                    let err = nn.send(Task::NewTask(output, order), counter);
                    if err.is_err() {
                        warn!("Error: {}", err.unwrap_err())
                    }
                    order = order + 1;
                }
                None => {
                    let err = nn.send(Task::Terminate(order), counter);
                    if err.is_err() {
                        warn!("Error: {}", err.unwrap_err())
                    }
                    break;
                }
            }
            counter = counter + 1;
        }
    }

    pub fn start(&mut self) -> std::result::Result<(), ThreadError> {
        self.thread.start()
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        self.thread.wait()
    }
}
