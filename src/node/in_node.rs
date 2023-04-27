use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use log::{trace, warn};

use crate::{
    channel::{
        channel::{Channel, InputChannel, OutputChannel},
        err::ChannelError,
    },
    task::{Message, Task},
     core::orchestrator::{JobInfo, self, Orchestrator},
};

use super::node::Node;

/// Trait defining a node that receive data.
///
/// # Examples:
///
/// A node that increment an internal counter each time an input is received
/// and return the total count of input received:
/// ```
/// use pspp::node::{in_node::{In, InNode}};
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
    job_info: JobInfo,
    channel: OutputChannel<Message<TIn>>,
    ordered: bool,
    storage: Mutex<BTreeMap<usize, Message<TIn>>>,
    counter: AtomicUsize,
    result: Arc<Mutex<Option<TCollected>>>,
}

impl<TIn: Send + 'static, TCollected: Send + 'static> Node<TIn, TCollected>
    for InNode<TIn, TCollected>
{
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), ChannelError> {
        let Message { op, order } = input;
        match &op {
            Task::NewTask(_e) => {
                if self.ordered && order != self.counter.load(Ordering::SeqCst) { //change to acquire ordering
                    self.save_to_storage(Message::new(op, rec_id), order);
                    self.send_pending();
                } else {
                    let res = self.channel.send(Message::new(op, order));
                    if res.is_err() {
                        panic!("Error: Cannot send message!");
                    }
                    let old_c = self.counter.load(Ordering::SeqCst);
                    self.counter.store(old_c + 1, Ordering::SeqCst);
                }
            }
            Task::Dropped => {
                if self.ordered && order != self.counter.load(Ordering::SeqCst) {
                    self.save_to_storage(Message::new(op, order), order);
                    self.send_pending();
                } else if self.ordered {
                    let old_c = self.counter.load(Ordering::SeqCst);
                    self.counter.store(old_c + 1, Ordering::SeqCst);
                }
            }
            Task::Terminate => {
                if self.ordered && order != self.counter.load(Ordering::SeqCst) {
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

impl<TIn: Send + 'static, TCollected: Send + 'static> InNode<TIn, TCollected> {
    /// Create a new input Node.
    /// The `handler` is the  struct that implement the trait `In` and defines
    /// the behavior of the node we're creating.
    /// `next_node` contains the stage that follows the node.
    /// If `blocking` is true the node will perform blocking operation on receive.
    /// If `pinning` is `true` the node will be pinned to the thread in position `id`.
    ///
    pub fn new(
        id: usize,
        handler: Box<dyn In<TIn, TCollected> + Send + Sync>,
        blocking: bool,
        pinning: bool,
        orchestrator: Arc<Orchestrator>,
    ) -> InNode<TIn, TCollected> {
        trace!("Created a new Sink! Id: {}", id);

        let (channel_in, channel_out) = Channel::channel(blocking);
        let result = Arc::new(Mutex::new(None));
        let ordered = handler.is_ordered();

        let bucket = Arc::clone(&result);

        let job_info = orchestrator.push(
            move || {
                let res = InNode::rts(handler, channel_in);
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
        );

        let mut node = InNode {
            job_info,
            channel: channel_out,
            ordered,
            storage: Mutex::new(BTreeMap::new()),
            counter: AtomicUsize::new(0),
            result,
        };

        node
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
                        Task::NewTask(arg) => {
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

    fn wait(&mut self)  {
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
                let mut c = self.counter.load(Ordering::SeqCst);
                while queue.contains_key(&c) {
                    let msg = queue.remove(&c).unwrap();
                    let Message { op, order } = msg;
                    match &op {
                        Task::NewTask(_e) => {
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
                    c = self.counter.load(Ordering::SeqCst);
                }
            }
            Err(_) => panic!("Error: Cannot lock the storage!"),
        }
    }
}
