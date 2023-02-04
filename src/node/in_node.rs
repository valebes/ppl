use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use log::{trace, warn};

use crate::{
    channel::{Channel, ChannelError},
    task::{Message, Task},
    thread::{Thread, ThreadError},
};

use super::node::Node;

/*
Public API
*/
pub trait In<TIn: 'static + Send, TOut> {
    fn run(&mut self, input: TIn);
    fn finalize(&mut self) -> Option<TOut>;
    fn ordered(&self) -> bool {
        false
    }
}

pub struct InNode<TIn: Send, TCollected> {
    thread: Thread,
    channel: Arc<Channel<Message<TIn>>>,
    ordered: bool,
    storage: Mutex<BTreeMap<usize, Message<TIn>>>,
    counter: AtomicUsize,
    result: Arc<Mutex<Option<TCollected>>>,
}

impl<TIn: Send + 'static, TCollected: Send + 'static> Node<TIn, TCollected>
    for InNode<TIn, TCollected>
{
    fn send(&self, input: Message<TIn>, rec_id: usize) -> Result<(), ChannelError> {
        match input {
            Message { op, order } => match &op {
                Task::NewTask(_e) => {
                    if self.ordered && order != self.counter.load(Ordering::SeqCst) {
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
                    } else {
                        if self.ordered {
                            let old_c = self.counter.load(Ordering::SeqCst);
                            self.counter.store(old_c + 1, Ordering::SeqCst);
                        }
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
            },
        }
        Ok(())
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

    fn get_num_of_replicas(&self) -> usize {
        1
    }
}

impl<TIn: Send + 'static, TCollected: Send + 'static> InNode<TIn, TCollected> {
    pub fn new(
        id: usize,
        handler: Box<dyn In<TIn, TCollected> + Send + Sync>,
        blocking: bool,
        pinning: bool,
    ) -> Result<InNode<TIn, TCollected>, ThreadError> {
        trace!("Created a new Sink! Id: {}", id);

        let channel = Arc::new(Channel::new(blocking));
        let result = Arc::new(Mutex::new(None));
        let ordered = handler.ordered();

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
            pinning,
        );

        let mut node = InNode {
            thread: thread,
            channel: channel,
            ordered: ordered,
            storage: Mutex::new(BTreeMap::new()),
            counter: AtomicUsize::new(0),
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
        channel: &Channel<Message<TIn>>,
    ) -> Option<TCollected> {
        loop {
            let input = channel.receive();
            match input {
                Ok(Message { op, order: _ }) => {
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
                    match msg {
                        Message { op, order } => match &op {
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
                        },
                    }
                    c = self.counter.load(Ordering::SeqCst);
                }
            }
            Err(_) => panic!("Error: Cannot lock the storage!"),
        }
    }
}
