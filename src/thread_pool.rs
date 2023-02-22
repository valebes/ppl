use crossbeam_deque::{Injector, Worker, Stealer};
use log::trace;
use std::sync::{Arc};
use std::thread::JoinHandle;
use std::{ iter, thread};

use crate::thread::Thread;

type Func = Box<dyn FnOnce() + Send + 'static>;

enum Job {
    NewJob(Func),
    Terminate,
}

struct ThreadPool {
    threads: Vec<Option<JoinHandle<()>>>,
    injector: Arc<Injector<Job>>,
}

impl ThreadPool {
    fn new(num_threads: usize, pinning: bool) -> Self {
        let mut threads = Vec::with_capacity(num_threads);
        let mut workers: Vec<Worker<Job>> = Vec::with_capacity(num_threads);
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
            let local_worker = workers.remove(0);
            let local_stealers = stealers.clone();

            
            threads.push(Some(thread::spawn(move || {
                let mut stop = false;
                loop {
                    let res = Self::find_task(&local_worker, &local_injector, &local_stealers);
                    match res {
                        Some(task) => {
                            match task {
                                Job::NewJob(func) => (func)(),
                                Job::Terminate => stop = true,
                            }
                        },
                        None => {
                            if stop {
                                local_injector.push(Job::Terminate);
                                break;
                            } else {
                                continue;
                            }
                        },
                    }
                }
            })));
        }

        Self { threads, injector }
    }

    fn find_task<F>(
        local: &Worker<F>,
        global: &Injector<F>,
        stealers: &Vec<Stealer<F>>,
    ) -> Option<F> 
         {
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
        self.injector.push(Job::NewJob(Box::new(task)));
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        trace!("Closing threadpool");
        self.injector.push(Job::Terminate);
        for th in &mut self.threads {
            match th.take() {
                Some(thread) => thread.join().expect("Error while joining thread!"),
                None => (), // thread already joined
            }
        }
    }
}
pub fn fibonacci_reccursive(n: i32) -> u64 {
    if n < 0 {
        panic!("{} is negative!", n);
    }
    match n {
        0 => panic!("zero is not a right argument to fibonacci_reccursive()!"),
        1 | 2 => 1,
        3 => 2,
        /*
        50    => 12586269025,
        */
        _ => fibonacci_reccursive(n - 1) + fibonacci_reccursive(n - 2),
    }
}

#[test]
fn test_threadpool() {
    let mut tp = ThreadPool::new(8, false);
    for i in 1..45 {
        tp.execute(move || { println!("Fib({}): {}", i, fibonacci_reccursive(i)) })
    }
}