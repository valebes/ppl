use std::{
    collections::VecDeque,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
};

use dyn_clone::DynClone;
use log::{trace, warn};
use std::collections::BTreeMap;

use crate::{
    channel::{Channel, ChannelError, InputChannel, OutputChannel},
    task::{Message, Task},
    thread::{Thread, ThreadError},
};

use super::node::Node;

/*
Public API
*/
pub trait InOut<TIn, TOut>: DynClone {
    fn run(&mut self, input: TIn) -> Option<TOut>;
    fn splitter(&mut self) -> Option<TOut> {
        None
    }
    fn number_of_replicas(&self) -> usize {
        1
    }
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
    fn is_splitter(&self) -> bool {
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

pub struct InOutNode<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    threads: Vec<Thread>,
    channels: Vec<OutputChannel<Message<TIn>>>,
    next_node: Arc<TNext>,
    ordered: bool,
    splitter: bool,
    ordered_splitter: Arc<(Mutex<OrderedSplitter>, Condvar)>,
    storage: Mutex<BTreeMap<usize, Message<TIn>>>,
    next_msg: AtomicUsize,
    phantom: PhantomData<(TOut, TCollected)>,
}

impl<
        TIn: Send + 'static,
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Send + Sync + 'static,
    > Node<TIn, TCollected> for InOutNode<TIn, TOut, TCollected, TNext>
{
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), ChannelError> {
        let mut rec_id = rec_id;
        if rec_id >= self.threads.len() {
            rec_id = rec_id % self.threads.len();
        }

        match input {
            Message { op, order } => match &op {
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
                            let old_c = self.next_msg.load(Ordering::SeqCst);
                            self.next_msg.store(old_c + 1, Ordering::SeqCst);
                        }
                    }
                }
                Task::Dropped => {
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
                            let old_c = self.next_msg.load(Ordering::SeqCst);
                            self.next_msg.store(old_c + 1, Ordering::SeqCst);
                        }
                    }
                }
                Task::Terminate => {
                    if self.channels.len() == 1
                        && self.ordered
                        && order != self.next_msg.load(Ordering::SeqCst)
                    {
                        self.save_to_storage(Message::new(op, order), order);
                        self.send_pending();
                    } else {
                        for ch in &self.channels {
                            let err = ch.send(Message::new(Task::Terminate, order));
                            if err.is_err() {
                                panic!("Error: Cannot send message!");
                            }
                        }

                        if self.ordered {
                            self.next_msg.store(order, Ordering::SeqCst)
                        }
                    }
                }
            },
        }
        Ok(())
    }

    fn collect(mut self) -> Option<TCollected> {
        let err = self.wait();
        if err.is_err() {
            panic!("Error: Cannot wait thread.");
        }
        match Arc::try_unwrap(self.next_node) {
            Ok(nn) => nn.collect(),
            Err(_) => panic!("Error: Cannot collect results inout."),
        }
    }

    fn get_num_of_replicas(&self) -> usize {
        self.threads.len()
    }
}

impl<
        TIn: Send + 'static,
        TOut: Send + 'static,
        TCollected,
        TNext: Node<TOut, TCollected> + Sync + Send + 'static,
    > InOutNode<TIn, TOut, TCollected, TNext>
{
    pub fn new(
        id: usize,
        handler: Box<dyn InOut<TIn, TOut> + Send + Sync>,
        next_node: TNext,
        blocking: bool,
        pinning: bool,
    ) -> Result<InOutNode<TIn, TOut, TCollected, TNext>, ThreadError> {
        let mut threads = Vec::new();
        let mut channels = Vec::new();
        let next_node = Arc::new(next_node);
        let replicas = handler.number_of_replicas();

        let splitter = Arc::new((Mutex::new(OrderedSplitter::new()), Condvar::new()));
        for i in 0..replicas {
            let (channel_in, channel_out) = Channel::new(blocking);
            channels.push(channel_out);
            let nn = Arc::clone(&next_node);
            let copy = dyn_clone::clone_box(&*handler);

            let splitter_copy = Arc::clone(&splitter);
            let mut thread = Thread::new(
                i + id,
                move || {
                    Self::rts(i + id, copy, channel_in, &nn, replicas, &splitter_copy);
                },
                pinning,
            );
            let err = thread.start();
            if err.is_err() {
                return Err(err.unwrap_err());
            }
            threads.push(thread);
        }

        let node = InOutNode {
            channels: channels,
            threads: threads,
            next_node: next_node,
            ordered: handler.is_ordered(),
            splitter: handler.is_splitter(),
            ordered_splitter: splitter,
            storage: Mutex::new(BTreeMap::new()),
            next_msg: AtomicUsize::new(0),
            phantom: PhantomData,
        };

        Ok(node)
    }

    fn rts(
        id: usize,
        mut node: Box<dyn InOut<TIn, TOut>>,
        channel_in: InputChannel<Message<TIn>>,
        next_node: &TNext,
        n_replicas: usize,
        ordered_splitter_handler: &(Mutex<OrderedSplitter>, Condvar),
    ) {
        // If next node have more replicas, i specify the first next node where i send my msg
        let mut counter = 0;
        if (next_node.get_num_of_replicas() > n_replicas) && n_replicas != 1 {
            counter = id * (next_node.get_num_of_replicas() / n_replicas);
        } else if next_node.get_num_of_replicas() <= n_replicas {
            // Standard case, not a2a
            counter = id;
        }
        trace!("Created a new Node! Id: {}", id);
        loop {
            // If next node have more replicas, when counter > next_replicas i reset the counter
            if (next_node.get_num_of_replicas() > n_replicas)
                && counter >= next_node.get_num_of_replicas()
            {
                counter = 0;
            }

            let input = channel_in.receive();

            match input {
                Ok(Some(Message { op, order })) => match op {
                    Task::NewTask(arg) => {
                        let output = node.run(arg);
                        if !node.is_splitter() {
                            match output {
                                Some(msg) => {
                                    let err = next_node
                                        .send(Message::new(Task::NewTask(msg), order), counter);
                                    if err.is_err() {
                                        warn!("Error: {}", err.unwrap_err())
                                    }
                                }
                                None => {
                                    let err =
                                        next_node.send(Message::new(Task::Dropped, order), counter);
                                    if err.is_err() {
                                        warn!("Error: {}", err.unwrap_err())
                                    }
                                }
                            }
                        } else {
                            let mut tmp = VecDeque::new();
                            loop {
                                let splitter_out = node.splitter();
                                match splitter_out {
                                    Some(msg) => {
                                        tmp.push_back(msg);
                                    }
                                    None => break,
                                }
                            }

                            if node.is_ordered() {
                                let (lock, cvar) = &*ordered_splitter_handler;
                                let mut ordered_splitter = lock.lock().unwrap();
                                loop {
                                    let (latest, end) = ordered_splitter.get();
                                    if latest == order {
                                        let mut count_splitter = end;
                                        while !tmp.is_empty() {
                                            let err = next_node.send(
                                                Message::new(
                                                    Task::NewTask(tmp.pop_front().unwrap()),
                                                    count_splitter,
                                                ),
                                                counter,
                                            );
                                            if err.is_err() {
                                                warn!("Error: {}", err.unwrap_err())
                                            }
                                            count_splitter = count_splitter + 1;
                                        }
                                        ordered_splitter.set(order + 1, count_splitter);
                                        cvar.notify_all();
                                        break;
                                    } else {
                                        let err = cvar.wait(ordered_splitter);
                                        if err.is_err() {
                                            panic!("Error: Poisoned mutex!");
                                        } else {
                                            ordered_splitter = err.unwrap();
                                        }
                                    }
                                }
                            } else {
                                while !tmp.is_empty() {
                                    let err = next_node.send(
                                        Message::new(
                                            Task::NewTask(tmp.pop_front().unwrap()),
                                            order,
                                        ),
                                        counter,
                                    );
                                    if err.is_err() {
                                        warn!("Error: {}", err.unwrap_err())
                                    }
                                }
                            }
                        }
                    }
                    Task::Dropped => {
                        let err = next_node.send(Message::new(Task::Dropped, order), counter);
                        if err.is_err() {
                            warn!("Error: {}", err.unwrap_err())
                        }
                    }
                    Task::Terminate => {
                        break;
                    }
                },
                Ok(None) => (),
                Err(e) => {
                    warn!("Error: {}", e);
                }
            }
            if next_node.get_num_of_replicas() > n_replicas {
                counter = counter + 1;
            }
        }
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        for th in &mut self.threads {
            let err = th.wait();
            if err.is_err() {
                return Err(err.unwrap_err());
            }
        }
        let mut c = 0;
        if self.ordered && !self.splitter {
            c = self.next_msg.load(Ordering::SeqCst);
        } else if self.ordered && self.splitter {
            let (lock, _) = self.ordered_splitter.as_ref();
            let ordered_splitter = lock.lock().unwrap();
            (_, c) = ordered_splitter.get();
        }
        let err = self.next_node.send(Message::new(Task::Terminate, c), 0);
        if err.is_err() {
            panic!("Error: Cannot send message!");
        }
        Ok(())
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
                let mut c = self.next_msg.load(Ordering::SeqCst);
                while queue.contains_key(&c) {
                    let msg = queue.remove(&c).unwrap();
                    match msg {
                        Message { op, order } => match &op {
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
                        },
                    }
                    c = self.next_msg.load(Ordering::SeqCst);
                }
            }
            Err(_) => panic!("Error: Cannot lock the storage!"),
        }
    }
}
