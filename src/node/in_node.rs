use std::sync::{Arc, Mutex};

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
pub trait In<TIn: 'static + Send, TOut> {
    fn run(&mut self, input: TIn);
    fn finalize(&mut self) -> Option<TOut>;
}

pub struct InNode<TIn: Send, TCollected> {
    thread: Thread,
    channel: Arc<Channel<Task<TIn>>>,
    result: Arc<Mutex<Option<TCollected>>>,
}

impl<TIn: Send + 'static, TCollected: Send + 'static> Node<TIn, TCollected>
    for InNode<TIn, TCollected>
{
    fn send(&self, input: Task<TIn>) -> Result<(), ChannelError> {
        self.channel.send(input)
    }

    fn collect(mut self) -> Option<TCollected> {
        let err = self.wait();
        if err.is_err() {
            panic!("Error: Cannot wait thread.");
        }
        let tmp = self.result.lock();
        if tmp.is_err() {
            panic!("Error: Cannot collect results in.");
        }

        let mut res = tmp.unwrap();
        if res.is_none() {
            return None;
        } else {
            return res.take();
        }
    }
}

impl<TIn: Send + 'static, TCollected: Send + 'static> InNode<TIn, TCollected> {
    pub fn new(
        id: usize,
        handler: Box<dyn In<TIn, TCollected> + Send + Sync>,
        blocking: bool,
    ) -> Result<InNode<TIn, TCollected>, ThreadError> {
        trace!("Created a new InNode!");

        let channel = Arc::new(Channel::new(blocking));
        let result = Arc::new(Mutex::new(None));

        let ch = Arc::clone(&channel);
        let bucket = Arc::clone(&result);

        let thread = Thread::new(
            id,
            move || {
                let res = InNode::rts(handler, &ch);
                if res.is_some() {
                    let err = bucket.lock();
                    if err.is_ok() {
                        let mut lock_bucket = err.unwrap();
                        *lock_bucket = res;
                    } else if err.is_err() {
                        panic!("Error: Cannot collect results.")
                    }
                }
            },
            false,
        );

        let mut node = InNode {
            thread: thread,
            channel: channel,
            result,
        };
        let err = node.thread.start();
        if err.is_err() {
            return Err(err.unwrap_err());
        }
        Ok(node)
    }

    fn rts(
        mut node: Box<dyn In<TIn, TCollected>>,
        channel: &Channel<Task<TIn>>,
    ) -> Option<TCollected> {
        loop {
            let input = channel.receive();
            match input {
                Ok(Task::NewTask(arg)) => {
                    node.run(arg);
                }
                Ok(Task::Dropped) => {
                    //TODO: Manage dropped
                }
                Ok(Task::Terminate) => {
                    break;
                }
                Err(e) => {
                    warn!("Error: {}", e);
                }
            }
        }
        node.finalize()
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        self.thread.wait()
    }
}
