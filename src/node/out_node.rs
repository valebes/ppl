use std::marker::PhantomData;
use std::sync::{Arc, Condvar, Mutex};

use log::trace;

use crate::channel::err::ChannelError;
use crate::core::orchestrator::{JobInfo, Orchestrator};
use crate::task::{Message, Task};

use super::node::Node;

/// Trait defining a node that output data.
///
/// # Examples:
///
/// A node emitting a vector containing numbers from 0 to 99 for `streamlen` times:
/// ```
/// use pspp::node::{out_node::{Out, OutNode}};
/// struct Source {
///      streamlen: usize,
///      counter: usize,
/// }
/// impl Out<Vec<i32>> for Source {
///      fn run(&mut self) -> Option<Vec<i32>> {
///          if self.counter < self.streamlen {
///              self.counter = self.counter + 1;
///              Some((0..99).collect())
///          } else {
///              None
///          }
///     }
///  }
/// ```
pub trait Out<TOut: 'static + Send> {
    /// This method is called by the rts until a None is returned.
    /// When None is returned, the node will terminate.
    fn run(&mut self) -> Option<TOut>;
}

pub struct OutNode<TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    next_node: Arc<TNext>,
    stop: Arc<Mutex<bool>>,
    job_info: JobInfo,
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
        self.wait();

        match Arc::try_unwrap(self.next_node) {
            Ok(nn) => nn.collect(),
            Err(_) => panic!("Error: Cannot collect results"),
        }
    }

    fn get_free_node(&self) -> Option<usize> {
        None
    }

    fn get_num_of_replicas(&self) -> usize {
        1
    }
}

impl<TOut: Send + 'static, TCollected, TNext: Node<TOut, TCollected> + Send + Sync + 'static>
    OutNode<TOut, TCollected, TNext>
{
    /// Create a new output Node.
    /// The `handler` is the  struct that implement the trait `Out` and defines
    /// the behavior of the node we're creating.
    /// `next_node` contains the stage that follows the node.
    /// If `pinning` is `true` the node will be pinned to the thread in position `id`.
    ///
    pub fn new(
        id: usize,
        handler: Box<dyn Out<TOut> + Send + Sync>,
        next_node: TNext,
        orchestrator: Arc<Orchestrator>,
    ) -> OutNode<TOut, TCollected, TNext> {
        trace!("Created a new Source! Id: {}", id);
        let stop = Arc::new(Mutex::new(true));
        let stop_copy = Arc::clone(&stop);

        let next_node = Arc::new(next_node);

        let nn = Arc::clone(&next_node);

        let scheduling = orchestrator.get_configuration().get_scheduling();

        let res = orchestrator.push(move || {
            Self::rts(handler, &nn, scheduling, &stop_copy);
        });

        OutNode {
            next_node,
            stop,
            job_info: res,
            phantom: PhantomData,
        }
    }

    fn rts(mut node: Box<dyn Out<TOut>>, nn: &TNext, scheduling: bool, stop: &Mutex<bool>) {
        let mut order = 0;
        let mut counter = 0;

        let mut stop_mtx = stop.lock().unwrap();
        let cvar = Condvar::new();

        // Wait until the node is started
        while *stop_mtx {
            stop_mtx = cvar.wait(stop_mtx).unwrap();
        }

        drop(stop_mtx); // Release the lock to avoid deadlock

        loop {
            let stop_mtx = stop.lock();
            match stop_mtx {
                Ok(mtx) => {
                    if *mtx {
                        let err = nn.send(Message::new(Task::Terminate, order), counter);
                        if err.is_err() {
                            panic!("Error: {}", err.unwrap_err())
                        }
                        break;
                    }
                }
                Err(_) => panic!("Error: Cannot lock mutex."),
            }

            

            let res = node.run(); // Run the node and get the output

            counter = counter % nn.get_num_of_replicas(); // Get the next node
            let latest = counter; // Save the latest node

            // If scheduling is enabled, get the next free node
            if scheduling {
                match nn.get_free_node() {
                    Some(id) => counter = id,
                    None => (),
                }
            }
            match res {
                Some(output) => {
                    let err = nn.send(Message::new(Task::NewTask(output), order), counter);
                    if err.is_err() {
                        panic!("Error: {}", err.unwrap_err())
                    }
                    order += 1;
                }
                None => {
                    let err = nn.send(Message::new(Task::Terminate, order), counter);
                    if err.is_err() {
                        panic!("Error: {}", err.unwrap_err())
                    }
                    break;
                }
            }
            
            if scheduling {
                counter = latest;
            } else {
                counter += 1;
            }
        }
    }

    /// Start the node.
    pub fn start(&mut self) {
        self.send_start();
    }

    /// Terminate the current node and the following ones.
    pub fn terminate(mut self) {
        self.send_stop();
        self.wait();
        match Arc::try_unwrap(self.next_node) {
            Ok(nn) => {
                nn.collect();
            }
            Err(_) => panic!("Error: Cannot collect results"),
        }
    }

    fn send_start(&self) {
        let mtx = self.stop.lock();
        match mtx {
            Ok(mut stop) => *stop = false,
            Err(_) => panic!("Error: Cannot lock mutex."),
        }
    }

    fn send_stop(&self) {
        let mtx = self.stop.lock();
        match mtx {
            Ok(mut stop) => *stop = true,
            Err(_) => panic!("Error: Cannot lock mutex."),
        }
    }

    fn wait(&mut self) {
        self.job_info.wait();
    }
}
