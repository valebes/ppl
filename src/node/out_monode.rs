use std::{marker::PhantomData, sync::Arc};

use log::{trace, warn};

use crate::{
    channel::{ChannelError},
    task::Task,
    thread::{Thread, ThreadError},
};

use super::{node::Node, out_node::Out};

pub struct OutMoNode<TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    thread: Thread,
    next_nodes: Arc<Vec<Arc<TNext>>>,
    phantom: PhantomData<(TOut, TCollected)>,
}

impl<
        TIn: Send + 'static,
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Send + Sync + 'static,
    > Node<TIn, TCollected> for OutMoNode<TOut, TCollected, TNext>
{
    fn send(&self, _input: Task<TIn>) -> Result<(), ChannelError> {
        Ok(())
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
}

impl<
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Sync + Send + 'static,
    > OutMoNode<TOut, TCollected, TNext>
{
    pub fn new(
        id: usize,
        handler: Box<dyn Out<TOut> + Send + Sync>,
        next_nodes: Vec<TNext>,
    ) -> Result<OutMoNode<TOut, TCollected, TNext>, ()> {
        trace!("Created a new InOutMoNode!");

        let mut next_nodes_new = Vec::new();
        for node in next_nodes {
            next_nodes_new.push(Arc::new(node));
        }

        let next_nodes_new = Arc::new(next_nodes_new);

        let nn = Arc::clone(&next_nodes_new);

        let thread = Thread::new(
            id,
            move || {
                Self::rts(handler, &nn);
            },
            false,
        );

        let node = OutMoNode {
            thread: thread,
            next_nodes: next_nodes_new,
            phantom: PhantomData,
        };
  
        Ok(node)
    }

    fn rts(
        mut node: Box<dyn Out<TOut>>,
        next_nodes: &Arc<Vec<Arc<TNext>>>,
    ) {
        let mut stop = false;
        loop {
            for it in next_nodes.iter() {
                let res = node.run();
                match res {
                    Some(output) => {
                        let err = it.send(Task::NewTask(output));
                        if err.is_err() {
                            warn!("Error: {}", err.unwrap_err())
                        }
                    }
                    None => {
                        stop = true;
                        break;
                    }
                }
            }
            if stop {
                for it in next_nodes.iter() {
                    let err = it.send(Task::Terminate);
                    if err.is_err() {
                        warn!("Error: {}", err.unwrap_err())
                    }
                }
                break;
            }
        }
        
    }

    pub fn start(&mut self) -> std::result::Result<(), ThreadError> {
        self.thread.start()
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        self.thread.wait()
    }
}
