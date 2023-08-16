use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use log::warn;

use crate::{
    core::orchestrator::{JobInfo, Orchestrator},
    mpsc::{
        channel::{Channel, InputChannel, OutputChannel},
        err::SenderError,
    },
    task::{Message, Task},
};

use super::Node;

/// Trait defining a node that receive data.
///
/// # Examples:
///
/// A node that increment an internal counter each time an input is received
/// and return the total count of input received:
/// ```
/// use ppl::prelude::*;
/// struct Sink {
/// counter: usize,
/// }
/// impl In<u64, usize> for Sink {
///    fn run(&mut self, input: u64) {
///        println!("{}", input);
///       self.counter = self.counter + 1;
///    }
///
///    fn finalize(self) -> Option<usize> {
///        println!("End");
///       Some(self.counter)
///   }
/// }
/// ```
pub trait In<TIn, TCollected>
where
    TIn: 'static + Send,
{
    /// This method is called each time the node receive an input.
    fn run(&mut self, input: TIn);

    /// This method is called before the node terminates. Is useful to take out data
    /// at the end of the computation.
    fn finalize(self) -> Option<TCollected>;

    /// This method return a boolean that represent if the node must receive the input respecting the order of arrival.
    /// Override this method allow choosing if the node must preserve the order of the input.
    fn is_ordered(&self) -> bool {
        false
    }
}

// Implement the In trait for a closure
impl<TIn, TCollected, F> In<TIn, TCollected> for F
where
    F: FnMut(TIn) -> TCollected,
    TIn: 'static + Send,
{
    fn run(&mut self, input: TIn) {
        self(input);
    }

    fn finalize(self) -> Option<TCollected> {
        None
    }
}

/// Struct representing a Sink node.
pub struct InNode<TIn, TCollected>
where
    TIn: Send,
{
    channel: OutputChannel<Message<TIn>>,
    ordered: bool,
    storage: Mutex<BTreeMap<usize, Message<TIn>>>,
    counter: AtomicUsize,
    result: Arc<Mutex<Option<TCollected>>>,
    job_info: JobInfo,
}

impl<TIn, TCollected> Node<TIn, TCollected> for InNode<TIn, TCollected>
where
    TIn: Send + 'static,
    TCollected: Send + 'static,
{
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), SenderError> {
        let Message { op, order } = input;
        match &op {
            Task::New(_e) => {
                if self.ordered && order != self.counter.load(Ordering::Acquire) {
                    self.save_to_storage(Message::new(op, rec_id), order);
                    self.send_pending();
                } else {
                    let res = self.channel.send(Message::new(op, order));
                    if res.is_err() {
                        panic!("Error: Cannot send message!");
                    }
                    self.counter.fetch_add(1, Ordering::AcqRel);
                }
            }
            Task::Dropped => {
                if self.ordered && order != self.counter.load(Ordering::Acquire) {
                    self.save_to_storage(Message::new(op, order), order);
                    self.send_pending();
                } else if self.ordered {
                    self.counter.fetch_add(1, Ordering::AcqRel);
                }
            }
            Task::Terminate => {
                if self.ordered && order != self.counter.load(Ordering::Acquire) {
                    self.save_to_storage(Message::new(op, order), order);
                    self.send_pending();
                } else {
                    let res = self.channel.send(Message::new(op, order));
                    if res.is_err() {
                        panic!("Error: Cannot send message!");
                    }
                }
            }
        }
        Ok(())
    }

    fn collect(mut self) -> Option<TCollected> {
        self.wait();
        let tmp = self.result.lock();
        if tmp.is_err() {
            panic!("Error: Cannot collect results in.");
        }

        let mut res = tmp.unwrap();
        if res.is_none() {
            None
        } else {
            res.take()
        }
    }

    fn get_num_of_replicas(&self) -> usize {
        1
    }
}

impl<TIn, TCollected> InNode<TIn, TCollected>
where
    TIn: Send + 'static,
    TCollected: Send + 'static,
{
    /// Create a new input Node.
    ///
    /// The `handler` is the  struct that implement the trait `In` and defines
    /// the behavior of the node we're creating.
    /// `next_node` contains the stage that follows the node.
    pub fn new(
        handler: Box<dyn In<TIn, TCollected> + Send + Sync>,
        orchestrator: Arc<Orchestrator>,
    ) -> InNode<TIn, TCollected> {
        let (channel_in, channel_out) =
            Channel::channel(orchestrator.get_configuration().get_wait_policy());
        let result = Arc::new(Mutex::new(None));
        let ordered = handler.is_ordered();

        let bucket = Arc::clone(&result);

        let job_info = orchestrator
            .push_jobs(vec![move || {
                if let Some(res) = InNode::rts(handler, channel_in) {
                    match bucket.lock() {
                        Ok(mut lock_bucket) => {
                            *lock_bucket = Some(res);
                        }
                        Err(_) => panic!("Error: Cannot collect results."),
                    }
                }
            }])
            .remove(0);

        InNode {
            job_info,
            channel: channel_out,
            ordered,
            storage: Mutex::new(BTreeMap::new()),
            counter: AtomicUsize::new(0),
            result,
        }
    }

    fn rts(
        mut node: Box<dyn In<TIn, TCollected>>,
        channel: InputChannel<Message<TIn>>,
    ) -> Option<TCollected> {
        loop {
            let input = channel.receive();
            match input {
                Ok(Some(Message { op, order: _ })) => {
                    match op {
                        Task::New(arg) => {
                            node.run(arg);
                        }
                        Task::Dropped => {
                            // Don't do anything
                        }
                        Task::Terminate => {
                            break;
                        }
                    }
                }
                Ok(None) => (),
                Err(e) => {
                    warn!("Error: {}", e);
                }
            }
        }

        node.finalize()
    }

    fn wait(&mut self) {
        self.job_info.wait()
    }

    fn save_to_storage(&self, task: Message<TIn>, order: usize) {
        let mtx = self.storage.lock();

        match mtx {
            Ok(mut queue) => {
                queue.insert(order, task);
            }
            Err(_) => panic!("Error: Cannot lock the storage!"),
        }
    }

    fn send_pending(&self) {
        let mtx = self.storage.lock();

        match mtx {
            Ok(mut queue) => {
                let mut c = self.counter.load(Ordering::Acquire);
                while queue.contains_key(&c) {
                    let msg = queue.remove(&c).unwrap();
                    let Message { op, order } = msg;
                    match &op {
                        Task::New(_e) => {
                            let err = self.send(Message::new(op, c), order);
                            if err.is_err() {
                                panic!("Error: Cannot send message!");
                            }
                        }
                        Task::Dropped => {
                            let err = self.send(Message::new(op, c), 0);
                            if err.is_err() {
                                panic!("Error: Cannot send message!");
                            }
                        }
                        Task::Terminate => {
                            let err = self.send(Message::new(op, c), 0);
                            if err.is_err() {
                                panic!("Error: Cannot send message!");
                            }
                        }
                    }
                    c = self.counter.load(Ordering::Acquire);
                }
            }
            Err(_) => panic!("Error: Cannot lock the storage!"),
        }
    }
}
