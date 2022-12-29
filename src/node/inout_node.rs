use std::{marker::PhantomData, sync::Arc};

use log::{trace, warn};

use crate::{
    channel::{Channel, ChannelError},
    task::Task,
    thread::{Thread, ThreadError},
};

use super::node::Node;

/*
Public API
*/
pub trait InOut<TIn, TOut> {
    fn run(&mut self, input: TIn) -> Option<TOut>;
}

pub struct InOutNode<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    thread: Thread,
    channel: Arc<Channel<Task<TIn>>>,
    next_node: Arc<TNext>,
    phantom: PhantomData<(TOut, TCollected)>,
}

impl<
        TIn: Send + 'static,
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Send + Sync + 'static,
    > Node<TIn, TCollected> for InOutNode<TIn, TOut, TCollected, TNext>
{
    fn send(&self, input: Task<TIn>) -> Result<(), ChannelError> {
        self.channel.send(input)
    }

    fn collect(mut self) -> Option<TCollected> {
        let err = self.wait();
        if err.is_err() {
            panic!("Error: Cannot wait thread.");
        }
        match Arc::try_unwrap(self.next_node) {
            Ok(nn) => nn.collect(),
            Err(_) => panic!("Error: Cannot collect results inout."),
        }
    }
}

impl<
        TIn: Send + 'static,
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Sync + Send + 'static,
    > InOutNode<TIn, TOut, TCollected, TNext>
{
    pub fn new(
        id: usize,
        handler: Box<dyn InOut<TIn, TOut> + Send + Sync>,
        next_node: TNext,
        blocking: bool,
    ) -> Result<InOutNode<TIn, TOut, TCollected, TNext>, ThreadError> {
        trace!("Created a new InOutNode!");

        let channel_in = Arc::new(Channel::new(blocking));
        let next_node = Arc::new(next_node);

        let ch_in = Arc::clone(&channel_in);
        let nn = Arc::clone(&next_node);

        let thread = Thread::new(
            id,
            move || {
                Self::rts(handler, &ch_in, &nn);
            },
            false,
        );

        let mut node = InOutNode {
            channel: channel_in,
            thread: thread,
            next_node: next_node,
            phantom: PhantomData,
        };
        let err = node.thread.start();
        if err.is_err() {
            return Err(err.unwrap_err());
        }
        Ok(node)
    }

    fn rts(
        mut node: Box<dyn InOut<TIn, TOut>>,
        channel_in: &Channel<Task<TIn>>,
        next_node: &TNext,
    ) {
        loop {
            let input = channel_in.receive();
            match input {
                Ok(Task::NewTask(arg)) => {
                    let output = node.run(arg);
                    if output.is_some() {
                        let err = next_node.send(Task::NewTask(output.unwrap()));
                        if err.is_err() {
                            warn!("Error: {}", err.unwrap_err())
                        }
                    } else {
                        let err = next_node.send(Task::Dropped);
                        if err.is_err() {
                            warn!("Error: {}", err.unwrap_err())
                        }
                    }
                }
                Ok(Task::Dropped) => {
                    let err = next_node.send(Task::Dropped);
                    if err.is_err() {
                        warn!("Error: {}", err.unwrap_err())
                    }
                }
                Ok(Task::Terminate) => {
                    let err = next_node.send(Task::Terminate);
                    if err.is_err() {
                        warn!("Error: {}", err.unwrap_err())
                    }
                    break;
                }
                Err(e) => {
                    warn!("Error: {}", e);
                }
            }
        }
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        self.thread.wait()
    }
}
