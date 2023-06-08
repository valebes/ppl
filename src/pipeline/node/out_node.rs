use std::marker::PhantomData;
use std::sync::{Arc, Condvar, Mutex};

use log::trace;

use crate::core::orchestrator::{JobInfo, Orchestrator};
use crate::mpsc::err::SenderError;
use crate::task::{Message, Task};

use super::Node;

/// Trait defining a node that output data.
///
/// # Examples:
///
/// A node emitting a vector containing numbers from 0 to 99 for `streamlen` times:
/// ```
/// use ppl::prelude::*;
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
pub trait Out<TOut>
where
    TOut: 'static + Send,
{
    /// This method is called by the rts until a None is returned.
    /// When None is returned, the node will terminate.
    fn run(&mut self) -> Option<TOut>;
}

// Implement the Out trait for a closure
impl<TOut, F> Out<TOut> for F
where
    F: FnMut() -> Option<TOut>,
    TOut: 'static + Send,
{
    fn run(&mut self) -> Option<TOut> {
        self()
    }
}

pub struct OutNode<TOut, TCollected, TNext>
where
    TOut: Send,
    TNext: Node<TOut, TCollected>,
{
    next_node: Arc<TNext>,
    stop: Arc<Mutex<bool>>,
    cvar: Arc<Condvar>,
    job_info: JobInfo,
    phantom: PhantomData<(TOut, TCollected)>,
}

impl<TIn, TOut, TCollected, TNext> Node<TIn, TCollected> for OutNode<TOut, TCollected, TNext>
where
    TIn: Send,
    TOut: Send + 'static,
    TNext: Node<TOut, TCollected> + Send + Sync + 'static,
{
    fn send(&self, _input: Message<TIn>, _rec_id: usize) -> Result<(), SenderError> {
        Ok(())
    }

    fn collect(mut self) -> Option<TCollected> {
        self.wait();

        match Arc::try_unwrap(self.next_node) {
            Ok(nn) => nn.collect(),
            Err(_) => panic!("Error: Cannot collect results"),
        }
    }

    fn get_num_of_replicas(&self) -> usize {
        1
    }
}

impl<TOut, TCollected, TNext> OutNode<TOut, TCollected, TNext>
where
    TOut: Send + 'static,
    TNext: Node<TOut, TCollected> + Send + Sync + 'static,
{
    /// Create a new output Node.
    /// 
    /// The `handler` is the  struct that implement the trait `Out` and defines
    /// the behavior of the node we're creating.
    /// `next_node` contains the stage that follows the node.
    pub fn new(
        id: usize,
        handler: Box<dyn Out<TOut> + Send + Sync>,
        next_node: TNext,
        orchestrator: Arc<Orchestrator>,
    ) -> OutNode<TOut, TCollected, TNext> {
        trace!("Created a new Source! Id: {}", id);
        let stop = Arc::new(Mutex::new(true));
        let stop_copy = Arc::clone(&stop);

        let cvar = Arc::new(Condvar::new());
        let cvar_copy = cvar.clone();

        let next_node = Arc::new(next_node);

        let nn = Arc::clone(&next_node);

        let job_info = orchestrator
            .push_jobs(vec![move || {
                Self::rts(handler, &nn, &stop_copy, &cvar_copy);
            }])
            .remove(0);

        OutNode {
            next_node,
            stop,
            cvar,
            job_info,
            phantom: PhantomData,
        }
    }

    /// RTS of the node.
    /// It runs the node until a None is returned.
    /// When None is returned, the node will terminate.
    /// The node will send the output to the next node.
    /// The node will terminate also when the stop flag is set to true.
    fn rts(mut node: Box<dyn Out<TOut>>, nn: &TNext, stop: &Mutex<bool>, cvar: &Condvar) {
        let mut order = 0;
        let mut counter = 0;

        let mut stop_mtx = stop.lock().unwrap();

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

            counter %= nn.get_num_of_replicas(); // Get the next node

            match res {
                Some(output) => {
                    let err = nn.send(Message::new(Task::New(output), order), counter);
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

            counter += 1;
        }
    }

    /// Start the node.
    /// The node will start to send the output to the next node.
    pub fn start(&mut self) {
        self.send_start();
    }

    /// Terminate the current node and the following ones.
    /// The node will terminate also when the stop flag is set to true.
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

    /// Start the node.
    /// The node will start to send the output to the next node.
    fn send_start(&self) {
        let mtx = self.stop.lock();
        match mtx {
            Ok(mut stop) => *stop = false,
            Err(_) => panic!("Error: Cannot lock mutex."),
        }
        self.cvar.notify_one();
    }

    /// Terminate the current node and the following ones.
    fn send_stop(&self) {
        let mtx = self.stop.lock();
        match mtx {
            Ok(mut stop) => *stop = true,
            Err(_) => panic!("Error: Cannot lock mutex."),
        }
    }

    /// Wait until the node is terminated.
    fn wait(&mut self) {
        self.job_info.wait();
    }
}
