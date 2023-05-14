use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc
    },
};

use log::{trace, warn};
use parking_lot::Mutex;

use crate::{
    mpsc::{
        channel::{Channel, InputChannel, OutputChannel},
        err::ChannelError,
    },
    core::orchestrator::{JobInfo, Orchestrator},
    task::{Message, Task},
};

use super::node::Node;

/// Trait defining a node that receive data.
///
/// # Examples:
///
/// A node that increment an internal counter each time an input is received
/// and return the total count of input received:
/// ```
/// use pspp::pipeline::{in_node::{In, InNode}};
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
pub trait In<TIn: 'static + Send, TOut> {
    /// This method is called each time the node receive an input.
    fn run(&mut self, input: TIn);
    /// This method is called before the node terminates. Is useful to take out data
    /// at the end of the computation.
    fn finalize(self) -> Option<TOut>;
    /// This method return a boolean that represent if the node receive the input in an ordered way.
    /// Overload this method allow to choose if the node is ordered or not.
    fn is_ordered(&self) -> bool {
        false
    }
}

pub struct InNode<TIn: Send, TCollected> {
    channel: OutputChannel<Message<TIn>>,
    ordered: bool,
    storage: Mutex<BTreeMap<usize, Message<TIn>>>,
    counter: AtomicUsize,
    result: Arc<Mutex<Option<TCollected>>>,
    job_info: JobInfo,
}

impl<TIn: Send + 'static, TCollected: Send + 'static> Node<TIn, TCollected>
    for InNode<TIn, TCollected>
{
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), ChannelError> {
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
        let mut tmp = self.result.lock();
        
        if tmp.is_none() {
            None
        } else {
            tmp.take()
        }
    }

    fn get_num_of_replicas(&self) -> usize {
        1
    }
}

impl<TIn: Send + 'static, TCollected: Send + 'static> InNode<TIn, TCollected> {
    /// Create a new input Node.
    /// The `handler` is the  struct that implement the trait `In` and defines
    /// the behavior of the node we're creating.
    /// `next_node` contains the stage that follows the node.
    pub fn new(
        id: usize,
        handler: Box<dyn In<TIn, TCollected> + Send + Sync>,
        orchestrator: Arc<Orchestrator>,
    ) -> InNode<TIn, TCollected> {
        trace!("Created a new Sink! Id: {}", id);

        let (channel_in, channel_out) =
            Channel::channel(orchestrator.get_configuration().get_blocking_channel());
        let result = Arc::new(Mutex::new(None));
        let ordered = handler.is_ordered();

        let bucket = Arc::clone(&result);

        let job_info = orchestrator.push(move || {
            if let Some(res) = InNode::rts(handler, channel_in) {
                let mut tmp = bucket.lock();
                *tmp = Some(res);
            }
        });

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
                            //TODO: Manage dropped
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
        let mut mtx = self.storage.lock();
        mtx.insert(order, task);
    }

    fn send_pending(&self) {
        let mut mtx = self.storage.lock();

                let mut c = self.counter.load(Ordering::Acquire);
                while mtx.contains_key(&c) {
                    let msg: Message<TIn> = mtx.remove(&c).unwrap();
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
}
