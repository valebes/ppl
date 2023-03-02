use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use log::{trace, warn};

use crate::channel::ChannelError;
use crate::task::{Message, Task};
use crate::thread::{Thread, ThreadError};

use super::node::Node;

/// Trait defining a node that have an output.
///
/// # Examples:
///
/// A node emitting a vector containing numbers from 0 to 99 for `streamlen` times:
///
/// ```no_run
///struct Source {
///     streamlen: usize,
///     counter: usize,
///}
///impl Out<Vec<i32>> for Source {
///     fn run(&mut self) -> Option<Vec<i32>> {
///         if self.counter < self.streamlen {
///             self.counter = self.counter + 1;
///             Some((0..99).collect())
///         } else {
///             None
///         }
///     }
/// }

/// ```
pub trait Out<TOut: 'static + Send> {
    /// This method is called by the rts until a None is returned.
    /// When None is returned, the node will terminate.
    fn run(&mut self) -> Option<TOut>;
}

pub struct OutNode<TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    thread: Thread,
    next_node: Arc<TNext>,
    stop: Arc<Mutex<bool>>,
    phantom: PhantomData<(TOut, TCollected)>,
}

impl<
        TIn: Send,
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Send + Sync + 'static,
    > Node<TIn, TCollected> for OutNode<TOut, TCollected, TNext>
{
    fn send(&self, _input: Message<TIn>, _rec_id: usize) -> Result<(), ChannelError> {
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
        pinning: bool,
    ) -> Result<OutNode<TOut, TCollected, TNext>, ()> {
        trace!("Created a new Source! Id: {}", id);
        let stop = Arc::new(Mutex::new(false));
        let stop_copy = Arc::clone(&stop);

        let next_node = Arc::new(next_node);

        let nn = Arc::clone(&next_node);

        let thread = Thread::new(
            id,
            move || {
                Self::rts(handler, &nn, &stop_copy);
            },
            pinning,
        );

        let node = OutNode {
            thread: thread,
            next_node: next_node,
            stop: stop,
            phantom: PhantomData,
        };

        Ok(node)
    }

    fn rts(mut node: Box<dyn Out<TOut>>, nn: &TNext, stop: &Mutex<bool>) {
        let mut order = 0;
        let mut counter = 0;
        loop {
            let stop_mtx = stop.lock();
            match stop_mtx {
                Ok(mtx) => {
                    if *mtx {
                        let err = nn.send(Message::new(Task::Terminate, order), counter);
                        if err.is_err() {
                            warn!("Error: {}", err.unwrap_err())
                        }
                        // to do cleanup
                        break;
                    }
                }
                Err(_) => panic!("Error: Cannot lock mutex."),
            }

            if counter >= nn.get_num_of_replicas() {
                counter = 0;
            }
            let res = node.run();
            match res {
                Some(output) => {
                    let err = nn.send(Message::new(Task::NewTask(output), order), counter);
                    if err.is_err() {
                        warn!("Error: {}", err.unwrap_err())
                    }
                    order = order + 1;
                }
                None => {
                    let err = nn.send(Message::new(Task::Terminate, order), counter);
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

    pub fn terminate(mut self) -> std::result::Result<(), ThreadError> {
        self.send_stop();
        let err = self.wait();
        if err.is_err() {
            return err;
        }

        match Arc::try_unwrap(self.next_node) {
            Ok(nn) => {
                nn.collect();
            }
            Err(_) => panic!("Error: Cannot collect results"),
        }

        Ok(())
    }
    fn send_stop(&self) {
        let mtx = self.stop.lock();
        match mtx {
            Ok(mut stop) => *stop = true,
            Err(_) => panic!("Error: Cannot lock mutex."),
        }
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        self.thread.wait()
    }
}
