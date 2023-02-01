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
    task::Task,
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
}

pub struct InOutNode<TIn: Send, TOut: Send, TCollected, TNext: Node<TOut, TCollected>> {
    threads: Vec<Thread>,
    channels: Vec<Arc<Channel<Task<TIn>>>>,
    next_node: Arc<TNext>,
    ordered: bool,
    storage: Mutex<BTreeMap<usize, Task<TIn>>>,
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
    fn send(&self, input: Task<TIn>, rec_id: usize) -> Result<(), ChannelError> {
        match input {
            Task::NewTask(e, order) => {
                if self.channels.len() == 1
                    && self.ordered
                    && order != self.counter.load(Ordering::SeqCst)
                {
                    println!(
                        "Arrived{}, expected{}",
                        order,
                        self.counter.load(Ordering::SeqCst)
                    );
                    self.save_to_storage(Task::NewTask(e, rec_id), order);
                    self.send_pending();
                } else {
                    //println!("Arrived{}", order);
                    let mut rec_id = rec_id;
                    if rec_id >= self.threads.len() {
                        rec_id = rec_id % self.threads.len();
                    }
                    let res = self.channels[rec_id].send(Task::NewTask(e, order));
                    if res.is_err() {
                        panic!("Error: Cannot send message!");
                    }

                    let old_c = self.counter.load(Ordering::SeqCst);
                    self.counter.store(old_c + 1, Ordering::SeqCst);
                }
            }
            Task::Dropped(order) => {
                //println!("Dropped{}", order);
                if self.channels.len() == 1
                    && self.ordered
                    && order != self.counter.load(Ordering::SeqCst)
                {
                    self.save_to_storage(Task::Dropped(order), order);
                    self.send_pending();
                } else {
                    let mut rec_id = rec_id;
                    if rec_id >= self.threads.len() {
                        rec_id = rec_id % self.threads.len();
                    }
                    let res = self.channels[rec_id].send(Task::Dropped(order));
                    if res.is_err() {
                        panic!("Error: Cannot send message!");
                    }

                    let old_c = self.counter.load(Ordering::SeqCst);
                    self.counter.store(old_c + 1, Ordering::SeqCst);
                }
            }
            Task::Terminate(order) => {
                if self.channels.len() == 1
                    && self.ordered
                    && order != self.counter.load(Ordering::SeqCst)
                {
                    self.save_to_storage(Task::Terminate(order), order);
                    self.send_pending();
                } else {
                    for ch in &self.channels {
                        let err = ch.send(Task::Terminate(order));
                        if err.is_err() {
                            panic!("Error: Cannot send message!");
                        }
                    }

                    // Only if replicas > 1
                    if self.channels.len() > 1 && self.ordered {
                        self.counter.store(order, Ordering::SeqCst)
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
        trace!("Created a new InOutNode!");

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
                    Self::rts(i, copy, &ch_in, &nn, replicas);
                },
                false,
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
        channel_in: &Channel<Task<TIn>>,
        next_node: &TNext,
        n_replicas: usize,
    ) {
        // If next node have more replicas, i specify the first next node where i send my msg
        let mut counter = 0;
        if (next_node.get_num_of_replicas() > n_replicas) && n_replicas != 1 {
            counter = id * (next_node.get_num_of_replicas() / n_replicas);
        }
        loop {
            if counter >= next_node.get_num_of_replicas() {
                counter = 0;
            }

            let input = channel_in.receive();
            trace!("Node {}", id);

            match input {
                Ok(Task::NewTask(arg, order)) => {
                    let output = node.run(arg);
                    if output.is_some() {
                        let err = next_node.send(Task::NewTask(output.unwrap(), order), id);
                        if err.is_err() {
                            warn!("Error: {}", err.unwrap_err())
                        }
                    } else {
                        let err = next_node.send(Task::Dropped(order), id);
                        if err.is_err() {
                            warn!("Error: {}", err.unwrap_err())
                        }
                    }
                }
                Ok(Task::Dropped(order)) => {
                    let err = next_node.send(Task::Dropped(order), id);
                    if err.is_err() {
                        warn!("Error: {}", err.unwrap_err())
                    }
                }
                Ok(Task::Terminate(_)) => {
                    break;
                }
                Err(e) => {
                    warn!("Error: {}", e);
                }
            }
            counter = counter + 1;
        }
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        for th in &mut self.threads {
            let err = th.wait();
            if err.is_err() {
                return Err(err.unwrap_err());
            }
        }
        let c = self.counter.load(Ordering::SeqCst);
        let err = self.next_node.send(Task::Terminate(c), 0);
        if err.is_err() {
            panic!("Error: Cannot send message!");
        }
        Ok(())
    }

    fn save_to_storage(&self, task: Task<TIn>, order: usize) {
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
                        Task::NewTask(e, rec_id) => {
                            let err = self.send(Task::NewTask(e, c), rec_id);
                            if err.is_err() {
                                panic!("Error: Cannot send message!");
                            }
                        }
                        Task::Dropped(e) => {
                            let err = self.send(Task::Dropped(e), 0);
                            if err.is_err() {
                                panic!("Error: Cannot send message!");
                            }
                        }
                        Task::Terminate(e) => {
                            let err = self.send(Task::Terminate(e), 0);
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
