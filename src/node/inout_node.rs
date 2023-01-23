use std::{marker::PhantomData, sync::Arc};

use log::{trace, warn};
use dyn_clone::DynClone;

use crate::{
    channel::{Channel, ChannelError},
    task::Task,
    thread::{Thread, ThreadError},
};

use super::node::Node;

/*
Public API
*/
pub trait InOut<TIn, TOut>: DynClone {
    fn run(&mut self, input: TIn) -> Option<TOut>;
    fn number_of_replicas(&self) -> usize {
        1
    }
}

pub struct InOutNode<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    threads: Vec<Thread>,
    channels: Vec<Arc<Channel<Task<TIn>>>>,
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
    fn send(&self, input: Task<TIn>, rec_id: usize) -> Result<(), ChannelError> {
        match input {
            Task::NewTask(e) =>{
                let mut rec_id = rec_id;
                if rec_id > self.threads.len() {
                    rec_id = rec_id % self.threads.len();
                }
                let res = self.channels[rec_id].send(Task::NewTask(e));
                res
            },
            Task::Dropped => Ok(()),
            Task::Terminate => {
                for ch in &self.channels {
                    let err = ch.send(Task::Terminate);
                    if err.is_err() {
                        return err;
                    }     
                }
                
                Ok(())
            },
        }
 
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

    fn get_num_of_replicas(&self) -> usize {
        self.threads.len()
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

        let mut threads = Vec::new();
        let mut channels = Vec::new();
        let next_node = Arc::new(next_node);

        let replicas = handler.number_of_replicas();
        for i in 0..replicas {
            channels.push(Arc::new(Channel::new(blocking)));
            let ch_in = Arc::clone(&channels[i]);
            let nn = Arc::clone(&next_node);
            let copy = dyn_clone::clone_box(&*handler);
            let mut thread = Thread::new(
                id,
                move || {
                    Self::rts(i, copy, &ch_in, &nn);
                },
                false,
            );
            let err = thread.start();
            if err.is_err() {
                return Err(err.unwrap_err());
            }            
            threads.push(thread);
        }

 

        let mut node = InOutNode {
            channels: channels,
            threads: threads,
            next_node: next_node,
            phantom: PhantomData,
        };
    
        Ok(node)
    }

    fn rts(
        id: usize,
        mut node: Box<dyn InOut<TIn, TOut>>,
        channel_in: &Channel<Task<TIn>>,
        next_node: &TNext,
    ) {
        loop {
            let input = channel_in.receive();
            trace!("Node {}", id);
            match input {
                Ok(Task::NewTask(arg)) => {
                    let output = node.run(arg);
                    if output.is_some() {
                        let err = next_node.send(Task::NewTask(output.unwrap()),id);
                        if err.is_err() {
                            warn!("Error: {}", err.unwrap_err())
                        }
                    } else {
                        let err = next_node.send(Task::Dropped,id);
                        if err.is_err() {
                            warn!("Error: {}", err.unwrap_err())
                        }
                    }
                }
                Ok(Task::Dropped) => {
                    let err = next_node.send(Task::Dropped,id);
                    if err.is_err() {
                        warn!("Error: {}", err.unwrap_err())
                    }
                }
                Ok(Task::Terminate) => {
                    break;
                }
                Err(e) => {
                    warn!("Error: {}", e);
                }
            }
        }
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        for th in &mut self.threads {
            let err = th.wait();
            if err.is_err() {
                return Err(err.unwrap_err());
            }
        }
        let err = self.next_node.send(Task::Terminate, 0);
        if err.is_err() {
            todo!();
        }     
        Ok(())
    }
}
