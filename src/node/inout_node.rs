use std::{
    collections::VecDeque,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Condvar, Mutex,
    },
    thread,
};

use crossbeam_deque::{Steal, Stealer, Worker};
use dyn_clone::DynClone;
use log::{trace, warn};
use std::collections::BTreeMap;

use crate::{
    mpsc::{
        channel::{Channel, InputChannel, OutputChannel},
        err::ChannelError,
    },
    core::orchestrator::{JobInfo, Orchestrator},
    task::{Message, Task},
};

use super::node::Node;

/// Trait defining a node that receive an input and produce an output.
///
/// # Examples:
///
/// A node that receive an integer and increment it by one:
/// ```
/// use pspp::node::{inout_node::{InOut, InOutNode}};
/// #[derive(Clone)]
/// struct Worker {}
/// impl InOut<i32, i32> for Worker {
///    fn run(&mut self, input: i32) -> Option<i32> {
///        Some(input + 1)
///    }
/// }
/// ```
///
///
pub trait InOut<TIn, TOut>: DynClone {
    /// This method is called each time the node receive an input.
    fn run(&mut self, input: TIn) -> Option<TOut>;
    /// If `is_producer` is `true` then this method will be called by the rts immediately
    /// after the execution of `run`.
    /// This method is called by the rts until a None is returned.
    /// When None is returned, the node will wait for another input.
    /// This method can be useful when we have a node that produce multiple output.
    fn produce(&mut self) -> Option<TOut> {
        None
    }
    /// This method return the number of replicas of the node.
    /// Overload this method allow to choose the number of replicas of the node.
    fn number_of_replicas(&self) -> usize {
        1
    }
    /// This method return a boolean that represent if the node receive the input in an ordered way.
    /// Overload this method allow to choose if the node is ordered or not.
    fn is_ordered(&self) -> bool {
        false
    }
    /// This method return a boolean that represent if the node is a producer or not.
    /// Overload this method allow to choose if the node produce multiple output or not.
    fn is_producer(&self) -> bool {
        false
    }
}

struct OrderedSplitter {
    latest: usize,
    start: usize,
}
impl OrderedSplitter {
    fn new() -> OrderedSplitter {
        OrderedSplitter {
            latest: 0,
            start: 0,
        }
    }
    fn get(&self) -> (usize, usize) {
        (self.latest, self.start)
    }
    fn set(&mut self, latest: usize, start: usize) {
        self.latest = latest;
        self.start = start;
    }
}

/// Struct that represent a node that receive an input and produce an output.
/// This struct is used by the rts to execute the node.
struct NodeWorker<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    id: usize,
    channel_rx: Option<InputChannel<Message<TIn>>>,
    node: Box<dyn InOut<TIn, TOut> + Send + Sync>,
    next_node: Arc<TNext>,
    splitter: Option<Arc<(Mutex<OrderedSplitter>, Condvar)>>,
    local_queue: Worker<Message<TIn>>, // steable queue
    system_queue: Vec<Message<TIn>>,   // non steable queue
    stealers: Option<Vec<Stealer<Message<TIn>>>>,
    stop: bool,
    num_replicas: usize,

    phantom: PhantomData<(TOut, TCollected)>,
}
impl<TIn: Send + 'static, TOut: Send, TCollected, TNext: Node<TOut, TCollected>>
    NodeWorker<TIn, TOut, TCollected, TNext>
{
    // Create a new workernode with default values.
    // Is created a new channel for the input and the output.
    // Splitter is set to None.
    fn new(
        id: usize,
        node: Box<dyn InOut<TIn, TOut> + Send + Sync>,
        next_node: Arc<TNext>,
        num_replicas: usize,
    ) -> NodeWorker<TIn, TOut, TCollected, TNext> {
        NodeWorker {
            id,
            channel_rx: None,
            node,
            next_node,
            splitter: None,
            local_queue: Worker::new_fifo(),
            stealers: None,
            system_queue: Vec::new(),
            num_replicas,
            stop: false,
            phantom: PhantomData,
        }
    }

    // Steal a messages from the other workers.
    fn get_message_from_others(&mut self) -> Option<Message<TIn>> {
        match &mut self.stealers {
            Some(stealers) => {
                for stealer in stealers.iter() {
                    loop {
                        match stealer.steal() {
                            Steal::Success(message) => {
                                return Some(message);
                            }
                            Steal::Empty => {
                                break;
                            }
                            Steal::Retry => {
                                continue;
                            }
                        }
                    }
                }
                None
            }
            None => None,
        }
    }

    // Get a new message from the local queue or steal a message from the other workers.
    fn get_message_from_local_queue(&mut self) -> Option<Message<TIn>> {
        self.local_queue.pop()
    }

    // Get a new message from the channel.
    // If there are more than one message in the channel, then the worker steal all the messages
    // and put them in the local queue, after return the first message.
    // If the channel is empty, then the worker return None.
    fn get_message_from_channel(&mut self) -> Option<Message<TIn>> {
        // If we have received a terminating message, then we don't want to receive any other message.
        // In blocking mode this help to avoid to block the thread on a channel that won't receive any other message.
        //TODO: Instead to doing this, drop the channel_tx when the terminating message is received.
        if self.stop {
            return None;
        }
        match &mut self.channel_rx {
            Some(channel_rx) => match channel_rx.receive() {
                Ok(Some(message)) => {
                    self.steal_all_from_channel();
                    return Some(message);
                }
                Ok(None) => return None,
                Err(e) => {
                    warn!("Error: {}", e);
                }
            },
            None => return None,
        }
        None
    }

    // Steal all the message from the channel.
    // All the messages, apart from terminating message, are put in the local queue.
    // If there are a terminating message, then that is put in the system queue.
    fn steal_all_from_channel(&mut self) {
        match &mut self.channel_rx {
            Some(channel_rx) => match channel_rx.receive_all() {
                Ok(messages) => {
                    messages.into_iter().for_each(|message| {
                        if message.is_terminate() {
                            self.system_queue.push(message);
                        } else {
                            self.local_queue.push(message);
                        }
                    });
                }
                Err(e) => {
                    warn!("Error: {}", e);
                }
            },
            None => {}
        }
    }

    // Get a message from system queue.
    // If the system queue is empty, then return None.
    fn get_message_from_system_queue(&mut self) -> Option<Message<TIn>> {
        self.system_queue.pop()
    }

    // Get a new message.
    // Pop a message from the local queue, if the local queue is not empty.
    // Look in the system queue, if there are a message.
    // Try to receive a message from the channel, if the channel is not empty.
    // Steal a message from the other workers, if the local queue and the channel are empty.
    fn get_message(&mut self) -> Option<Message<TIn>> {
        match self.get_message_from_local_queue() {
            Some(message) => Some(message),
            None => match self.get_message_from_system_queue() {
                Some(message) => Some(message),
                None => match self.get_message_from_channel() {
                    // If we are in blocking mode, we stop here until we receive a message.
                    Some(message) => Some(message),
                    None => self.get_message_from_others(), // Otherwise, If we are in non blocking mode and there arent msg in the channel, we steal a message from the other workers.
                },
            },
        }
    }

    fn get_stealer(&self) -> Stealer<Message<TIn>> {
        self.local_queue.stealer()
    }

    fn register_stealers(&mut self, stealers: Vec<Stealer<Message<TIn>>>) {
        match &mut self.stealers {
            Some(my_stealers) => {
                my_stealers.extend(stealers);
            }
            None => {
                self.stealers = Some(stealers);
            }
        }
    }

    // Create a new channel for the input.
    // Return the sender of the channel.
    fn create_channel(&mut self, blocking: bool) -> OutputChannel<Message<TIn>> {
        let (channel_rx, channel_tx) = Channel::channel(blocking);
        self.channel_rx = Some(channel_rx);
        channel_tx
    }

    // Set the ordered splitter handler for the worker.
    fn set_splitter(&mut self, splitter: Arc<(Mutex<OrderedSplitter>, Condvar)>) {
        self.splitter = Some(splitter);
    }

    fn init_counter(&self) -> usize {
        // If next node have more replicas, i specify the first next node where i send my msg
        let mut counter = 0;
        if (self.next_node.get_num_of_replicas() > self.num_replicas) && self.num_replicas != 1 {
            counter = self.id * (self.next_node.get_num_of_replicas() / self.num_replicas);
        } else if self.next_node.get_num_of_replicas() <= self.num_replicas {
            // Standard case, not a2a
            counter = self.id;
        }
        counter
    }

    // Run the node.
    // Depending on the options, the node can be run in different ways.
    fn rts(&mut self) {
        if self.node.is_producer() {
            self.rts_producer();
        } else {
            self.rts_default();
        }
    }

    // Run the node in the default way.
    fn rts_default(&mut self) {
        let mut counter = self.init_counter();
        trace!("InOutNode {} started", self.id);

        loop {
            let input = self.get_message();

            match input {
                Some(Message { op, order }) => {
                    counter %= self.next_node.get_num_of_replicas();

                    match op {
                        Task::New(task) => {
                            let result = self.node.run(task);

                            match result {
                                Some(msg) => {
                                    let err = self
                                        .next_node
                                        .send(Message::new(Task::New(msg), order), counter);
                                    if err.is_err() {
                                        panic!("Error: {}", err.unwrap_err())
                                    }
                                }
                                None => {
                                    let err = self
                                        .next_node
                                        .send(Message::new(Task::Dropped, order), counter);
                                    if err.is_err() {
                                        panic!("Error: {}", err.unwrap_err())
                                    }
                                }
                            }
                        }
                        Task::Dropped => {
                            let err = self
                                .next_node
                                .send(Message::new(Task::Dropped, order), counter);
                            if err.is_err() {
                                panic!("Error: {}", err.unwrap_err())
                            }
                        }
                        Task::Terminate => {
                            self.stop = true;
                        }
                    }
                    counter += 1;
                }
                None => {
                    // There are no more messages to process.
                    if self.stop {
                        break;
                    } else {
                        thread::yield_now();
                    }
                }
            }
        }
    }

    // RTS method for producer nodes.
    fn rts_producer(&mut self) {
        let mut counter = self.init_counter();
        trace!("InOutNode {} started", self.id);

        loop {
            let input = self.get_message();

            match input {
                Some(Message { op, order }) => {
                    counter %= self.next_node.get_num_of_replicas();

                    match op {
                        Task::New(task) => {
                            let _ = self.node.run(task);
                            let mut tmp = VecDeque::new();
                            loop {
                                let producer_out = self.node.produce();
                                match producer_out {
                                    Some(producer_out) => {
                                        tmp.push_back(producer_out);
                                    }
                                    None => {
                                        break;
                                    }
                                }
                            }

                            // If the node is ordered, then i have to send the messages in order
                            // to the next node.
                            if self.node.is_ordered() {
                                match &self.splitter {
                                    Some(splitter_ref) => {
                                        let mut splitter = splitter_ref.0.lock().unwrap();
                                        let cvar = &splitter_ref.1;
                                        loop {
                                            let (expected, start) = splitter.get();
                                            if expected == order {
                                                let mut tmp_counter = start;
                                                while !tmp.is_empty() {
                                                    let msg = Message {
                                                        op: Task::New(tmp.pop_front().unwrap()),
                                                        order: tmp_counter,
                                                    };
                                                    let err = self.next_node.send(msg, counter);
                                                    if err.is_err() {
                                                        panic!("Error: {}", err.unwrap_err());
                                                    }
                                                    tmp_counter += 1;
                                                }
                                                splitter.set(order + 1, tmp_counter);
                                                cvar.notify_all();
                                                break;
                                            } else {
                                                match cvar.wait(splitter) {
                                                    Ok(mtx) => {
                                                        splitter = mtx;
                                                    }
                                                    Err(err) => {
                                                        panic!("Error: {}", err);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Error: You shouldnt be here! (ordered node without splitter)");
                                    }
                                }
                            } else {
                                // If the node is not ordered, then i can send the messages to the next node
                                // without any order.
                                loop {
                                    let msg = Message {
                                        op: Task::New(tmp.pop_front().unwrap()),
                                        order: 0,
                                    };
                                    let err = self.next_node.send(msg, counter);
                                    if err.is_err() {
                                        panic!("Error: {}", err.unwrap_err());
                                    }
                                    if tmp.is_empty() {
                                        break;
                                    }
                                }
                            }
                        }
                        Task::Dropped => {
                            let err = self.next_node.send(
                                Message {
                                    op: Task::Dropped,
                                    order,
                                },
                                counter,
                            );
                            if err.is_err() {
                                panic!("Error: {}", err.unwrap_err())
                            }
                        }
                        Task::Terminate => {
                            self.stop = true;
                        }
                    }
                    counter += 1;
                }
                None => {
                    // There are no more messages to process.
                    if self.stop {
                        break;
                    } else {
                        thread::yield_now();
                    }
                }
            }
        }
    }
}

/// Struct representing a stage, with an input and an output, of a pipeline.
pub struct InOutNode<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    channels: Vec<OutputChannel<Message<TIn>>>,
    next_node: Arc<TNext>,
    ordered: bool,
    producer: bool,
    ordered_splitter: Option<Arc<(Mutex<OrderedSplitter>, Condvar)>>,
    storage: Mutex<BTreeMap<usize, Message<TIn>>>,
    next_msg: AtomicUsize,
    job_infos: Vec<JobInfo>,
    phantom: PhantomData<(TOut, TCollected)>,
}

impl<
        TIn: Send + 'static,
        TOut: Send + Sync + 'static,
        TCollected: Sync + Send + 'static,
        TNext: Node<TOut, TCollected> + Send + Sync + 'static,
    > Node<TIn, TCollected> for InOutNode<TIn, TOut, TCollected, TNext>
{
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), ChannelError> {
        let mut rec_id = rec_id;
        if rec_id >= self.job_infos.len() {
            rec_id %= self.job_infos.len();
        }

        let Message { op, order } = input;
        match &op {
            Task::New(_e) => {
                if self.channels.len() == 1
                    && self.ordered
                    && order != self.next_msg.load(Ordering::SeqCst)
                {
                    self.save_to_storage(Message::new(op, rec_id), order);
                    self.send_pending();
                } else {
                    let res = self.channels[rec_id].send(Message::new(op, order));
                    if res.is_err() {
                        panic!("Error: Cannot send message!");
                    }

                    if self.ordered {
                        self.next_msg.fetch_add(1, Ordering::AcqRel);
                    }
                }
            }
            Task::Dropped => {
                if self.channels.len() == 1
                    && self.ordered
                    && order != self.next_msg.load(Ordering::Acquire)
                {
                    self.save_to_storage(Message::new(op, rec_id), order);
                    self.send_pending();
                } else {
                    let res = self.channels[rec_id].send(Message::new(op, order));
                    if res.is_err() {
                        panic!("Error: Cannot send message!");
                    }

                    if self.ordered {
                        self.next_msg.fetch_add(1, Ordering::AcqRel);
                    }
                }
            }
            Task::Terminate => {
                if self.channels.len() == 1
                    && self.ordered
                    && order != self.next_msg.load(Ordering::Acquire)
                {
                    self.save_to_storage(Message::new(op, order), order);
                    self.send_pending();
                } else {
                    for channel in &self.channels {
                        let res = channel.send(Message::new(Task::Terminate, order));
                        if res.is_err() {
                            panic!("Error: Cannot send message!");
                        }
                    }

                    if self.ordered {
                        self.next_msg.store(order, Ordering::Release);
                    }
                }
            }
        }
        Ok(())
    }

    fn collect(mut self) -> Option<TCollected> {
        self.wait();
        match Arc::try_unwrap(self.next_node) {
            Ok(nn) => nn.collect(),
            Err(_) => panic!("Error: Cannot collect results inout."),
        }
    }

    fn get_num_of_replicas(&self) -> usize {
        self.job_infos.len()
    }
}

impl<
        TIn: Send + 'static,
        TOut: Send + 'static + Sync,
        TCollected: Sync + Send + 'static,
        TNext: Node<TOut, TCollected> + Sync + Send + 'static,
    > InOutNode<TIn, TOut, TCollected, TNext>
{
    /// Create a new Node.
    /// The `handler` is the  struct that implement the trait `InOut` and defines
    /// the behavior of the node we're creating.
    /// `next_node` contains the stage that follows the node.
    /// If `blocking` is true the node will perform blocking operation on receive.
    /// If `pinning` is `true` the node will be pinned to the thread in position `id`.
    ///
    pub fn new(
        id: usize,
        handler: Box<dyn InOut<TIn, TOut> + Send + Sync>,
        next_node: TNext,
        orchestrator: Arc<Orchestrator>,
    ) -> InOutNode<TIn, TOut, TCollected, TNext> {
        trace!("Created a new InOutNode! Id: {}", id);
        let mut funcs = Vec::new();
        let mut channels = Vec::new();
        let next_node = Arc::new(next_node);
        let replicas = handler.number_of_replicas();

        let blocking = orchestrator.get_configuration().get_blocking_channel();

        let ordered = handler.is_ordered();
        let producer = handler.is_producer();

        let mut splitter = None;
        if ordered && producer {
            splitter = Some(Arc::new((
                Mutex::new(OrderedSplitter::new()),
                Condvar::new(),
            )));
        }

        let mut handler_copies = Vec::with_capacity(replicas);
        for _i in 0..replicas - 1 {
            handler_copies.push(dyn_clone::clone_box(&*handler));
        }
        handler_copies.push(handler);

        let mut worker_nodes: Vec<NodeWorker<TIn, TOut, TCollected, TNext>> =
            Vec::with_capacity(replicas);

        // Create the workers
        for i in 0..replicas {
            let mut worker =
                NodeWorker::new(i, handler_copies.remove(0), next_node.clone(), replicas);

            // If the node is ordered and a producer, we need to set the ordered splitter handler
            if producer && ordered {
                worker.set_splitter(splitter.clone().unwrap());
            }

            // Create the channel
            channels.push(worker.create_channel(blocking));

            worker_nodes.push(worker);
        }

        // If workstealing is enabled (and the node isn't an ordered producer), we need to register the stealers
        if splitter.is_none() && orchestrator.get_configuration().get_scheduling() {
            // Register stealers to each worker
            let mut stealers = Vec::new();
            for worker in &worker_nodes {
                stealers.push(worker.get_stealer());
            }
            // Remove the stealer of the current worker
            (0..replicas).for_each(|i| {
                let mut stealer_cp = stealers.clone();
                stealer_cp.remove(i);
                worker_nodes[i].register_stealers(stealer_cp);
            });
        }

        // Create the barrier
        let barrier = Arc::new(Barrier::new(replicas));

        for _i in 0..replicas {
            let mut worker = worker_nodes.remove(0);

            let local_barrier = barrier.clone();
            let func = move || {
                // The worker will wait for the barrier before starting
                local_barrier.wait();
                worker.rts();
            };

            funcs.push(func);
        }

        InOutNode {
            channels,
            next_node,
            ordered,
            producer,
            ordered_splitter: splitter,
            storage: Mutex::new(BTreeMap::new()),
            next_msg: AtomicUsize::new(0),
            job_infos: orchestrator.push_multiple(funcs), // Push the workers into the orchestrator
            phantom: PhantomData,
        }
    }

    /// Wait for all the workers to finish
    fn wait(&mut self) {
        for job in &self.job_infos {
            job.wait();
        }

        // TODO: Change this, it can be done better
        // If the node is ordered and a producer, we need to wait for the ordered splitter to arrive at the correct order
        let mut c = 0;
        if self.ordered && !self.producer {
            c = self.next_msg.load(Ordering::Acquire);
        } else if self.ordered && self.producer {
            match &self.ordered_splitter {
                Some(lock) => {
                    let ordered_splitter = lock.0.lock().unwrap();
                    (_, c) = ordered_splitter.get();
                }
                None => panic!("Error: Ordered splitter not initialized!"),
            }
        }
        let err = self.next_node.send(Message::new(Task::Terminate, c), 0);
        if err.is_err() {
            panic!("Error: Cannot send message!");
        }
    }

    /// If we cant send a message immediately, we need to save it to the storage.
    /// Take in example a pipeline that is ordered.
    fn save_to_storage(&self, msg: Message<TIn>, order: usize) {
        let mtx = self.storage.lock();

        match mtx {
            Ok(mut queue) => {
                queue.insert(order, msg);
            }
            Err(_) => panic!("Error: Cannot lock the storage!"),
        }
    }

    /// Send pending messages in the storage to the next node.
    fn send_pending(&self) {
        let mtx = self.storage.lock();

        match mtx {
            Ok(mut queue) => {
                let mut c = self.next_msg.load(Ordering::Acquire);
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
                            let err = self.send(Message::new(op, c), order);
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
                    c = self.next_msg.load(Ordering::Acquire);
                }
            }
            Err(_) => panic!("Error: Cannot lock the storage!"),
        }
    }
}
