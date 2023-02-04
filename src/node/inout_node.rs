use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use dyn_clone::DynClone;
use log::{trace, warn};
use std::collections::BTreeMap;

use crate::{
    channel::{Channel, ChannelError},
    task::{Message, Task},
    thread::{Thread, ThreadError},
};

use super::node::Node;

/*
Public API
*/
pub trait InOut<TIn, TOut>: DynClone {
    fn run(&mut self, input: TIn) -> Option<TOut>;
    fn number_of_replicas(&self) -> usize {
        1
    }
    fn ordered(&self) -> bool {
        false
    }
    fn broadcasting(&self) -> bool { // to be implemented
        false
    }
    fn a2a(&self) -> bool {  // to be implemented
        false
    }
}

pub struct InOutNode<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    threads: Vec<Thread>,
    channels: Vec<Arc<Channel<Message<TIn>>>>,
    next_node: Arc<TNext>,
    ordered: bool,
    storage: Mutex<BTreeMap<usize, Message<TIn>>>,
    counter: AtomicUsize,
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
            Message { op, order } => {
                match &op {
                    Task::NewTask(_e) => {
                        if self.channels.len() == 1
                            && self.ordered
                            && order != self.counter.load(Ordering::SeqCst)
                        {
                            self.save_to_storage(Message::new(op, rec_id), order);
                            self.send_pending();
                        } else {
                            let res = self.channels[rec_id].send(Message::new(op, order));
                            if res.is_err() {
                                panic!("Error: Cannot send message!");
                            }

                            if self.ordered {
                            let old_c = self.counter.load(Ordering::SeqCst);
                            self.counter.store(old_c + 1, Ordering::SeqCst);
                            }
                        }
                    }
                    Task::Dropped => {
                        if self.channels.len() == 1
                            && self.ordered
                            && order != self.counter.load(Ordering::SeqCst)
                        {
                            self.save_to_storage(Message::new(op, rec_id), order);
                            self.send_pending();
                        } else {
                            let res = self.channels[rec_id].send(Message::new(op, order));
                            if res.is_err() {
                                panic!("Error: Cannot send message!");
                            }

                            if self.ordered {
                                let old_c = self.counter.load(Ordering::SeqCst);
                                self.counter.store(old_c + 1, Ordering::SeqCst);
                            }
                        }
                    }
                    Task::Terminate => {
                        if self.channels.len() == 1
                            && self.ordered
                            && order != self.counter.load(Ordering::SeqCst)
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
                                self.counter.store(order, Ordering::SeqCst)
                            }
                            
                        }
                    }
                }
            }
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
    ) -> Result<InOutNode<TIn, TOut, TCollected, TNext>, ThreadError> {
        let mut threads = Vec::new();
        let mut channels = Vec::new();
        let next_node = Arc::new(next_node);

        let replicas = handler.number_of_replicas();
        for i in 0..replicas {
            channels.push(Arc::new(Channel::new(blocking)));
            let ch_in = Arc::clone(&channels[i]);
            let nn = Arc::clone(&next_node);
            let copy = dyn_clone::clone_box(&*handler);
            let mut thread = Thread::new(
                id,
                move || {
                    Self::rts(i + id, copy, &ch_in, &nn, replicas);
                },
                true,
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
            ordered: handler.ordered(),
            storage: Mutex::new(BTreeMap::new()),
            counter: AtomicUsize::new(0),
            phantom: PhantomData,
        };

        Ok(node)
    }

    fn rts(
        id: usize,
        mut node: Box<dyn InOut<TIn, TOut>>,
        channel_in: &Channel<Message<TIn>>,
        next_node: &TNext,
        n_replicas: usize,
    ) {
        // If next node have more replicas, i specify the first next node where i send my msg
        let mut counter = 0;
        if (next_node.get_num_of_replicas() > n_replicas) && n_replicas != 1 {
            counter = id * (next_node.get_num_of_replicas() / n_replicas);
        }
        else if next_node.get_num_of_replicas() <= n_replicas {
            // Standard case, not a2a
            counter = id;
        }
        trace!("Created a new Node! Id: {}", id);
        loop {
            // If next node have more replicas, when counter > next_replicas i reset the counter
            if (next_node.get_num_of_replicas() > n_replicas) && counter >= next_node.get_num_of_replicas() {
                counter = 0;
            }

            let input = channel_in.receive();

            match input {
                Ok(Message { op, order }) => match op {
                    Task::NewTask(arg) => {
                        let output = node.run(arg);
                        if output.is_some() {
                            let err = next_node
                                .send(Message::new(Task::NewTask(output.unwrap()), order), counter);
                            if err.is_err() {
                                warn!("Error: {}", err.unwrap_err())
                            }
                        } else {
                            let err = next_node.send(Message::new(Task::Dropped, order), counter);
                            if err.is_err() {
                                warn!("Error: {}", err.unwrap_err())
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

        if self.ordered {
            c = self.counter.load(Ordering::SeqCst);
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
                    c = self.counter.load(Ordering::SeqCst);
                }
            }
            Err(_) => panic!("Error: Cannot lock the storage!"),
        }
    }
}
