use std::{marker::PhantomData, sync::Arc};

use log::{trace, warn};

use crate::{
    channel::{Channel, ChannelError},
    task::Task,
    thread::{Thread, ThreadError},
};

use super::{node::Node, inout_node::InOut};

pub struct InOutMoNode<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    thread: Thread,
    channel: Arc<Channel<Task<TIn>>>,
    next_nodes: Arc<Vec<Arc<TNext>>>,
    phantom: PhantomData<(TOut, TCollected)>,
}

impl<
        TIn: Send + 'static,
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Send + Sync + 'static,
    > Node<TIn, TCollected> for InOutMoNode<TIn, TOut, TCollected, TNext>
{
    fn send(&self, input: Task<TIn>, rec_id: usize) -> Result<(), ChannelError> {
        self.channel.send(input)
    }

    fn collect(mut self) -> Option<TCollected> {
        let err = self.wait();
        if err.is_err() {
            panic!("Error: Cannot wait thread.");
        }
        match Arc::try_unwrap(self.next_nodes) {
            Ok(mut nn) => {
                let last_replica = nn.pop().unwrap();
                for e  in nn {
                    match Arc::try_unwrap(e) {
                        Ok(tmp) => { drop(tmp) },
                        Err(_) => panic!("Error: Cannot collect results inout."),
                    }
                }
                match Arc::try_unwrap(last_replica) {
                    Ok(tmp) => tmp.collect(),
                    Err(_) => panic!("Error: Cannot collect results inout."),
                }
            },
            Err(_) => panic!("Error: Cannot collect results inout."),
        }
    }

    fn get_num_of_replicas(&self) -> usize {
        1
    }
}

impl<
        TIn: Send + 'static,
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Sync + Send + 'static,
    > InOutMoNode<TIn, TOut, TCollected, TNext>
{
    pub fn new(
        id: usize,
        handler: Box<dyn InOut<TIn, TOut> + Send + Sync>,
        next_nodes: Vec<TNext>,
        blocking: bool,
    ) -> Result<InOutMoNode<TIn, TOut, TCollected, TNext>, ThreadError> {
        trace!("Created a new InOutMoNode!");

        let channel_in = Arc::new(Channel::new(blocking));

        let ch_in = Arc::clone(&channel_in);


        let mut next_nodes_new = Vec::new();
        for node in next_nodes {
            next_nodes_new.push(Arc::new(node));
        }

        let next_nodes_new = Arc::new(next_nodes_new);

        let nn = Arc::clone(&next_nodes_new);

        let thread = Thread::new(
            id,
            move || {
                Self::rts(handler, &ch_in, &nn);
            },
            false,
        );

        let mut node = InOutMoNode {
            channel: channel_in,
            thread: thread,
            next_nodes: next_nodes_new,
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
        next_nodes: &Arc<Vec<Arc<TNext>>>,
    ) {
        let mut stop = false;
        loop {
            for it in next_nodes.iter() {
                let msg = channel_in.receive();
                match msg {
                    Ok(task) => {
                        match task {
                            Task::NewTask(arg) => {
                                let res = node.run(arg);
                                if res.is_some() {
                                    let err = it.send(Task::NewTask(res.unwrap()),0); // todo: move task and not clone it
                                    if err.is_err() {
                                        warn!("Error: {}", err.unwrap_err())
                                    }
                                } else if res.is_none() {
                                    let err = it.send(Task::Dropped,0); // todo: move task and not clone it
                                    if err.is_err() {
                                        warn!("Error: {}", err.unwrap_err())
                                    }
                                }
                            }
                            Task::Terminate => {
                                stop = true;
                                break;
                            }
                            Task::Dropped => {
                                //TODO: Manage dropped
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Error: {}", e);
                    }
                }
            }
            if stop {
                for it in next_nodes.iter() {
                    let err = it.send(Task::Terminate,0);
                    if err.is_err() {
                        warn!("Error: {}", err.unwrap_err())
                    }
                }
                break;
            }
        }
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        self.thread.wait()
    }
}
