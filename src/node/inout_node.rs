use std::{
    collections::VecDeque,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Condvar, Mutex, RwLock,
    },
    thread,
};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use dyn_clone::DynClone;
use log::{trace, warn};
use std::collections::BTreeMap;

use crate::{
    channel::{
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
    fn broadcasting(&self) -> bool {
        // to be implemented
        false
    }
    fn a2a(&self) -> bool {
        // to be implemented
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

struct WorkerNode<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    id: usize,
    channel_rx: Option<InputChannel<Message<TIn>>>,
    node: Box<dyn InOut<TIn, TOut> + Send + Sync>,
    next_node: Arc<TNext>,
    feedback: Option<Arc<Injector<usize>>>,
    splitter: Option<Arc<(Mutex<OrderedSplitter>, Condvar)>>,
    global_queue: Option<Arc<Injector<Message<TIn>>>>,
    local_queue: Option<Mutex<Worker<Message<TIn>>>>,
    stealers: Option<RwLock<Vec<Stealer<Message<TIn>>>>>,
    num_replicas: usize,
    phantom: PhantomData<(TOut, TCollected)>,
}
impl<TIn: Send + 'static, TOut: Send, TCollected, TNext: Node<TOut, TCollected>>
    WorkerNode<TIn, TOut, TCollected, TNext>
{
    // Create a new workernode with default values.
    // Is created a new channel for the input and the output.
    // Feedback is set to None.
    // Splitter is set to None.
    fn new(
        id: usize,
        node: Box<dyn InOut<TIn, TOut> + Send + Sync>,
        next_node: Arc<TNext>,
        num_replicas: usize,
    ) -> WorkerNode<TIn, TOut, TCollected, TNext> {
        WorkerNode {
            id,
            channel_rx: None,
            node,
            next_node,
            feedback: None,
            splitter: None,
            global_queue: None,
            local_queue: None,
            stealers: None,
            num_replicas,
            phantom: PhantomData,
        }
    }

    // Get id
    fn get_id(&self) -> usize {
        self.id
    }

    // Steal a messages from the other workers.
    fn get_message_from_others(&mut self) -> Option<Message<TIn>> {
        match &mut self.stealers {
            Some(stealers) => loop {
                for stealer in stealers.read().unwrap().iter() {
                    match stealer.steal() {
                        Steal::Success(message) => {
                            return Some(message);
                        }
                        Steal::Empty => {
                            continue;
                        }
                        Steal::Retry => {
                            continue;
                        }
                    }
                }
                return None;
            },
            None => None,
        }
    }

    // Get a new message from the local queue or steal a message from the other workers.
    fn get_message_from_local_queue(&mut self) -> Option<Message<TIn>> {
        match &mut self.local_queue {
            Some(local_queue) => match local_queue.lock().unwrap().pop() {
                Some(message) => Some(message),
                None => None,
            },
            None => None,
        }
    }

    // Get a new message from the global queue.
    fn get_message_from_global_queue(&mut self) -> Option<Message<TIn>> {
        match &mut self.global_queue {
            Some(global_queue) => loop {
                match global_queue.steal() {
                    Steal::Success(message) => {
                        return Some(message);
                    }
                    Steal::Empty => {
                        return None;
                    }
                    Steal::Retry => {
                        continue;
                    }
                }
            },
            None => None,
        }
    }

    // Get a new message from the channel.
    // If there are more than one message in the channel, then the worker steal all the messages
    // and put them in the local queue, after return the first message.
    // If the channel is empty, then the worker return None.
    fn get_message_from_channel(&mut self) -> Option<Message<TIn>> {
        match &mut self.channel_rx {
            Some(channel_rx) => match channel_rx.receive() {
                Ok(Some(message)) => {
                    match &mut self.local_queue {
                        Some(local_queue) => {
                            let local_queue = local_queue.lock().unwrap();
                            channel_rx
                                .receive_all()
                                .unwrap()
                                .into_iter()
                                .for_each(|message| {
                                    local_queue.push(message);
                                });
                        }
                        None => (),
                    }
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

    // Get a new message.
    // Pop a message from the local queue, if the local queue is not empty.
    // Try to receive a message from the channel, if the channel is not empty.
    // Steal a message from the other workers, if the local queue and the channel are empty.
    // Then look for a message in the global queue.
    // If there are no messages, then return None.
    fn get_message(&mut self) -> Option<Message<TIn>> {
        match self.get_message_from_local_queue() {
            Some(message) => Some(message),
            None => match self.get_message_from_channel() {
                Some(message) => Some(message),
                None => match self.get_message_from_others() {
                    Some(message) => Some(message),
                    None => self.get_message_from_global_queue(),
                },
            },
        }
    }

    fn get_stealer(&self) -> Option<Stealer<Message<TIn>>> {
        match &self.local_queue {
            Some(local_queue) => Some(local_queue.lock().unwrap().stealer()),
            None => None,
        }
    }

    fn register_stealer(&mut self, stealer: Stealer<Message<TIn>>) {
        match &mut self.stealers {
            Some(stealers) => {
                stealers.write().unwrap().push(stealer);
            }
            None => {
                let mut stealers = Vec::new();
                stealers.push(stealer);
                self.stealers = Some(RwLock::new(stealers));
            }
        }
    }

    // Add global queue to the worker.
    fn set_global_queue(&mut self, global_queue: Arc<Injector<Message<TIn>>>) {
        self.global_queue = Some(global_queue);
    }

    // Create a local queue for the worker.
    fn create_local_queue(&mut self) {
        self.local_queue = Some(Mutex::new(Worker::new_fifo()));
    }
    // Create a new channel for the input.
    // Return the sender of the channel.
    fn create_channel(&mut self, blocking: bool) -> OutputChannel<Message<TIn>> {
        let (channel_rx, channel_tx) = Channel::channel(blocking);
        self.channel_rx = Some(channel_rx);
        channel_tx
    }

    // Set a splitter for the node.
    fn set_splitter(&mut self, splitter: Arc<(Mutex<OrderedSplitter>, Condvar)>) {
        self.splitter = Some(splitter);
    }

    // Set a feedback queue for the node.
    fn set_feedback(&mut self, feedback: Arc<Injector<usize>>) {
        self.feedback = Some(feedback);
    }

    // Send to global queue a message.
    fn send_to_global_queue(&mut self, message: Message<TIn>) {
        match &mut self.global_queue {
            Some(global_queue) => {
                global_queue.push(message);
            }
            None => (),
        }
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

    fn rts_default(&mut self) {
        let mut counter = self.init_counter();
        trace!("InOutNode {} started", self.id);

        loop {
            let input = self.get_message();
            match input {
                Some(Message { op, order }) => {
                    counter = counter % self.next_node.get_num_of_replicas();
                    let latest = counter;

                    match op {
                        Task::NewTask(task) => {
                            let result = self.node.run(task);

                            // if there is feedback enabled, then i check for a free node
                            // and i send the message to the free node
                            match self.feedback {
                                Some(_) => match self.next_node.get_free_node() {
                                    Some(id) => counter = id,
                                    None => (),
                                },
                                None => (),
                            }

                            match result {
                                Some(msg) => {
                                    let err = self
                                        .next_node
                                        .send(Message::new(Task::NewTask(msg), order), counter);
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
                            self.send_to_global_queue(Message::new(Task::Terminate, order));
                            break;
                        }
                    }

                    // if there is feedback enabled, then i send the id of the node to the feedback queue
                    match &self.feedback {
                        Some(feedback) => {
                            feedback.push(self.id);
                        }
                        None => counter += 1,
                    }
                }
                None => thread::yield_now(),
            }
        }
    }

    fn rts_producer(&mut self) {
        let mut counter = self.init_counter();
        trace!("InOutNode {} started", self.id);

        loop {
            let input = self.get_message();
            match input {
                Some(Message { op, order }) => {
                    counter = counter % self.next_node.get_num_of_replicas();
                    let latest = counter;

                    match op {
                        Task::NewTask(task) => {
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
                                            println!("{} {}", order, splitter.get().0);
                                            let (expected, start) = splitter.get();
                                            if expected == order {
                                                // if there is feedback enabled, then i check for a free node
                                                // and i send the message to the free node
                                                match self.feedback {
                                                    Some(_) => {
                                                        match self.next_node.get_free_node() {
                                                            Some(id) => counter = id,
                                                            None => (),
                                                        }
                                                    }
                                                    None => (),
                                                }

                                                let mut tmp_counter = start;
                                                while !tmp.is_empty() {
                                                    let msg = Message {
                                                        op: Task::NewTask(tmp.pop_front().unwrap()),
                                                        order: tmp_counter,
                                                    };
                                                    let err = self.next_node.send(msg, counter);
                                                    if err.is_err() {
                                                        panic!("Error: {}", err.unwrap_err());
                                                    }
                                                    tmp_counter += 1;
                                                }
                                                println!("YES {} {}", order, tmp_counter);
                                                splitter.set(order + 1, tmp_counter);
                                                cvar.notify_all();
                                                break;
                                            } else {
                                                println!("Waiting {} {}", order, splitter.get().0);
                                                let err = cvar.wait(splitter);
                                                if err.is_err() {
                                                    panic!("Error: Poisoned mutex!");
                                                } else {
                                                    splitter = err.unwrap();
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
                                // if there is feedback enabled, then i check for a free node
                                // and i send the message to the free node
                                match self.feedback {
                                    Some(_) => match self.next_node.get_free_node() {
                                        Some(id) => counter = id,
                                        None => (),
                                    },
                                    None => (),
                                }
                                loop {
                                    let msg = Message {
                                        op: Task::NewTask(tmp.pop_front().unwrap()),
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
                                    order: order,
                                },
                                counter,
                            );
                            if err.is_err() {
                                panic!("Error: {}", err.unwrap_err())
                            }
                        }
                        Task::Terminate => {
                            self.send_to_global_queue(Message::new(Task::Terminate, order));
                            break;
                        }
                    }

                    // If there is feedback enabled, then i send a message to the feedback queue
                    // to notify that i finished my job.
                    match &self.feedback {
                        Some(feedback) => {
                            feedback.push(self.id);
                        }
                        None => counter += 1,
                    }
                }
                None => thread::yield_now(),
            }
        }
    }
}

pub struct InOutNode<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    channels: Vec<OutputChannel<Message<TIn>>>,
    next_node: Arc<TNext>,
    ordered: bool,
    producer: bool,
    ordered_splitter: Option<Arc<(Mutex<OrderedSplitter>, Condvar)>>,
    storage: Mutex<BTreeMap<usize, Message<TIn>>>,
    next_msg: AtomicUsize,
    scheduler: Option<Arc<Injector<usize>>>,
    global_queue: Arc<Injector<Message<TIn>>>,
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
            Task::NewTask(_e) => {
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
                    self.global_queue.push(Message::new(op, order));
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

    fn get_free_node(&self) -> Option<usize> {
        match &self.scheduler {
            Some(sched) => loop {
                match sched.steal() {
                    Steal::Success(id) => {
                        return Some(id);
                    }
                    Steal::Empty => {
                        return None;
                    }
                    Steal::Retry => {
                        continue;
                    }
                }
            },
            None => None,
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
        let mut funcs = Vec::new();
        let mut channels = Vec::new();
        let next_node = Arc::new(next_node);
        let replicas = handler.number_of_replicas();

        let mut feedback_queue = None;
        let feedback = orchestrator.get_configuration().get_scheduling();
        if feedback {
            feedback_queue = Some(Arc::new(Injector::new()));
        }

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

        let mut worker_nodes: Vec<WorkerNode<TIn, TOut, TCollected, TNext>> =
            Vec::with_capacity(replicas);

        let global_queue = Arc::new(Injector::new());

        for i in 0..replicas {
            let mut worker =
                WorkerNode::new(i, handler_copies.remove(0), next_node.clone(), replicas);

            // If the node is a producer, we need to set the splitter
            if producer && ordered {
                worker.set_splitter(splitter.clone().unwrap());
            }
            // If the node have the feedback enabled, we need to set the feedback queue
            if feedback {
                worker.set_feedback(feedback_queue.clone().unwrap());
            }

            // todo put if here
            worker.create_local_queue();

            // Create the channel
            channels.push(worker.create_channel(blocking));

            // Add global queue
            worker.set_global_queue(global_queue.clone());

            worker_nodes.push(worker);
        }

        if splitter.is_none() {
        // Register stealers to each worker
        let mut stealers = Vec::with_capacity(replicas);
        for worker in &worker_nodes {
            match worker.get_stealer() {
                Some(stealer) => stealers.push(stealer),
                None => (),
            }
        }

        for worker in &mut worker_nodes {
            for stealer in &stealers {
                worker.register_stealer(stealer.clone());
            }
        }
        }


        let barrier = Arc::new(Barrier::new(replicas));

        for i in 0..replicas {
            let mut worker = worker_nodes.remove(0);
            
            let local_barrier = barrier.clone();
            let func = move || {
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
            scheduler: feedback_queue,
            global_queue,
            job_infos: orchestrator.push_multiple(funcs),
            phantom: PhantomData,
        }
    }

    fn wait(&mut self) {
        for job in &self.job_infos {
            job.wait();
        }

        // Change this that is really shitty
        let mut c = 0;
        if self.ordered && !self.producer {
            c = self.next_msg.load(Ordering::Acquire); // No need to be seq_cst
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

    fn save_to_storage(&self, msg: Message<TIn>, order: usize) {
        let mtx = self.storage.lock();

        match mtx {
            Ok(mut queue) => {
                queue.insert(order, msg);
            }
            Err(_) => panic!("Error: Cannot lock the storage!"),
        }
    }

    fn send_pending(&self) {
        let mtx = self.storage.lock();

        match mtx {
            Ok(mut queue) => {
                let mut c = self.next_msg.load(Ordering::Acquire);
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
