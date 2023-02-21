use crossbeam_deque::{Injector, Steal, Worker, Stealer};
use std::sync::{Arc};
use std::thread::JoinHandle;
use std::{ iter, thread};

use crate::thread::Thread;

struct ThreadPool {
    threads: Vec<JoinHandle<()>>,
    injector: Arc<Injector<Box<dyn FnOnce() + Send + 'static>>>,
}

impl ThreadPool {
    fn new(num_threads: usize, pinning: bool) -> Self {
        let mut threads = Vec::with_capacity(num_threads);
        let mut workers: Vec<Worker<Box<dyn FnOnce() + Send>>> = Vec::with_capacity(num_threads);
        let mut stealers = Vec::with_capacity(num_threads);
        let injector = Arc::new(Injector::new());

        for _ in 0..num_threads {
            workers.push(Worker::new_fifo());
        }
        for w in &workers {
            stealers.push(w.stealer());
        }

        for i in 0..num_threads {   
            let local_injector = Arc::clone(&injector);
            let local_worker = workers.remove(i);
            let local_stealers = stealers.clone();

            
            threads.push(thread::spawn(move || {
                loop {
                    let res = Self::find_task(&local_worker, &local_injector, &local_stealers);
                    match res {
                        Some(task) => (task)(),
                        None => continue,
                    }
                }
            }));
        }

        Self { threads, injector }
    }

    fn find_task<F>(
        local: &Worker<F>,
        global: &Injector<F>,
        stealers: &Vec<Stealer<F>>,
    ) -> Option<F> 
        where F: FnOnce() + Send + 'static,  {
        // Pop a task from the local queue, if not empty.
        local.pop().or_else(|| {
            // Otherwise, we need to look for a task elsewhere.
            iter::repeat_with(|| {
                // Try stealing a batch of tasks from the global queue.
                global.steal_batch_and_pop(local)
                    // Or try stealing a task from one of the other threads.
                    .or_else(|| stealers.iter().map(|s| s.steal()).collect())
            })
            // Loop while no task was stolen and any steal operation needs to be retried.
            .find(|s| !s.is_retry())
            // Extract the stolen task, if there is one.
            .and_then(|s| s.success())
        })
    }

    fn execute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.injector.push(Box::new(task));
    }
}