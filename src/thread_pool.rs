use crossbeam_deque::{Injector, Stealer, Worker};
use log::{error, trace};
use std::collections::BTreeMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Barrier, Mutex};
use std::thread::JoinHandle;
use std::{hint, iter, mem, thread, fmt};

use crate::channel::channel::Channel;
use crate::core::registry::{Registry, JobInfo, get_global_registry};


type Func<'a> = Box<dyn FnOnce() + Send + 'a>;

enum Job {
    NewJob(Func<'static>),
    Terminate,
}

#[derive(Debug)]
pub struct ThreadPoolError {
    details: String,
}

impl ThreadPoolError {
    fn new(msg: &str) -> ThreadPoolError {
        ThreadPoolError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for ThreadPoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for ThreadPoolError {
    fn description(&self) -> &str {
        &self.details
    }
}


///Struct representing a thread pool.
pub struct ThreadPool {
    num_threads: usize,
    workers_info: Vec<JobInfo>,
    total_tasks: Arc<AtomicUsize>,
    injector: Arc<Injector<Job>>,
    registry: Arc<Registry>,
}

impl Clone for ThreadPool {
    /// Create a new threadpool from an existing one, using the same number of threads.
    fn clone(&self) -> Self {
        let registry = self.registry.clone();
        let start = registry.get_range_of_contiguos_free_workers(self.num_threads);
        match start {
            Some(start) => {
                ThreadPool::new(self.num_threads, start, registry)
            },
            None => panic!("Not enough free workers"),
        }
    }
}

impl ThreadPool {
    fn new(num_threads: usize, from: usize, registry: Arc<Registry>) -> Self {
        trace!("Creating new threadpool");
        let mut start = 0;
        
        // todo: maybe change this in the case of non pinned threads
        match registry.get_range_of_contiguos_free_workers(num_threads) {
            Some(s) => start = s,
            None => panic!("Not enough free threads"),
        }
        
        let mut workers_info = Vec::with_capacity(num_threads);
        let mut workers: Vec<Worker<Job>> = Vec::with_capacity(num_threads);
        let mut stealers = Vec::with_capacity(num_threads);
        let injector = Arc::new(Injector::new());


        for _ in 0..num_threads {
            workers.push(Worker::new_fifo());
        }
        for w in &workers {
            stealers.push(w.stealer());
        }

        let total_tasks = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(num_threads));

        for i in 0..num_threads {
            let local_injector = Arc::clone(&injector);
            let local_worker = workers.remove(0);
            let local_stealers = stealers.clone();
            let local_barrier = Arc::clone(&barrier);
            let total_tasks_cp = Arc::clone(&total_tasks);

            let err = registry.execute_on(start + i, move || {
                let mut stop = false;
                // We wait that all threads start
                local_barrier.wait();
                loop {
                    let res = Self::find_task(&local_worker, &local_injector, &local_stealers);
                    match res {
                        Some(task) => match task {
                            Job::NewJob(func) => {
                                (func)();
                                total_tasks_cp.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                            }
                            Job::Terminate => stop = true,
                        },
                        None => {
                            if stop {
                                local_injector.push(Job::Terminate);
                                break;
                            } else {
                                continue;
                            }
                        }
                    }
                }
                
            }
            );
            match err {
                Ok(job) => workers_info.push(job),
                Err(e) => panic!("Error while executing thread: {}", e),
                }
        }


        Self {
            num_threads,
            workers_info,
            total_tasks,
            injector,
            registry,
        }

    }

    pub fn new_with_local_registry(num_threads: usize, pinning: bool) -> Self {
        let registry = crate::core::registry::new_local_registry(num_threads, pinning);
        Self::new(num_threads, 0, registry)
    }

    pub fn new_with_global_registry(num_threads: usize, from: usize) -> Self {
        let registry = get_global_registry();
        Self::new(num_threads, from, registry)
    }

    fn find_task<F>(
        local: &Worker<F>,
        global: &Injector<F>,
        stealers: &Vec<Stealer<F>>,
    ) -> Option<F> {
        // Pop a task from the local queue, if not empty.
        local.pop().or_else(|| {
            // Otherwise, we need to look for a task elsewhere.
            iter::repeat_with(|| {
                // Try stealing a batch of tasks from the global queue.
                global
                    .steal_batch_and_pop(local)
                    // Or try stealing a task from one of the other threads.
                    .or_else(|| stealers.iter().map(|s| s.steal()).collect())
            })
            // Loop while no task was stolen and any steal operation needs to be retried.
            .find(|s| !s.is_retry())
            // Extract the stolen task, if there is one.
            .and_then(|s| s.success())
        })
    }

    /// Execute a function `task` on a thread in the thread pool.
    pub fn execute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.injector.push(Job::NewJob(Box::new(task)));
        self.total_tasks
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
    
    /// Block until all current jobs in the thread pool are finished.
    pub fn wait(&self) {
        while (self.total_tasks.load(std::sync::atomic::Ordering::Acquire) != 0)
            || !self.injector.is_empty()
        {
            hint::spin_loop();
        }
    }
    /// Applies in parallel the function `f` on a iterable object `iter`.
    ///
    /// # Examples
    ///
    /// Increment of 1 all the elements in a vector concurrently:
    ///
    /// ```
    /// use pspp::thread_pool::ThreadPool;
    ///
    /// let mut pool = ThreadPool::new_with_local_registry(8, false);
    /// let mut vec = vec![0; 100];
    ///
    /// pool.par_for(&mut vec, |el: &mut i32| *el = *el + 1);
    /// pool.wait(); // wait the threads to finish the jobs
    ///
    pub fn par_for<Iter: IntoIterator, F>(&mut self, iter: Iter, f: F)
    where
        F: FnOnce(Iter::Item) + Send + 'static + Copy,
        <Iter as IntoIterator>::Item: Send,
    {
        self.scoped(|s| {
            iter.into_iter().for_each(|el| s.execute(move || (f)(el)));
        });
    }
    /// Applies in parallel the function `f` on a iterable object `iter`,
    /// producing a new iterator with the results.
    ///
    /// # Examples
    ///
    /// Produce a vec of `String` from the elements of a vector `vec` concurrently:
    ///
    /// ```
    /// use pspp::thread_pool::ThreadPool;
    ///
    /// let mut pool = ThreadPool::new_with_local_registry(8, false);
    /// let mut vec = vec![0i32; 100];
    ///
    /// let res: Vec<String> = pool.par_map(&mut vec, |el| -> String {
    ///            String::from("Hello from: ".to_string() + &el.to_string())
    ///       }).collect();
    ///
    pub fn par_map<Iter: IntoIterator, F, R>(&mut self, iter: Iter, f: F) -> impl Iterator<Item = R>
    where
        F: FnOnce(Iter::Item) -> R + Send + Copy,
        <Iter as IntoIterator>::Item: Send,
        R: Send + 'static,
    {
        let (rx, tx) = Channel::channel(true);
        let arc_tx = Arc::new(tx);
        let mut unordered_map = BTreeMap::<usize, R>::new();
        self.scoped(|s| {
            iter.into_iter().enumerate().for_each(|el| {
                let cp = Arc::clone(&arc_tx);
                s.execute(move || {
                    let err = cp.send((el.0, f(el.1)));
                    if err.is_err() {
                        panic!("Error: {}", err.unwrap_err());
                    }
                });
            });
        });
        self.wait();
        while !rx.is_empty() {
            let msg = rx.receive();
            match msg {
                Ok(Some((order, result))) => {
                    unordered_map.insert(order, result);
                }
                Ok(None) => continue,
                Err(e) => panic!("Error: {}", e),
            };
        }
        unordered_map.into_values()
    }

    /// Borrows the thread pool and allows executing jobs on other
    /// threads during that scope via the argument of the closure.
    pub fn scoped<'pool, 'scope, F, R>(&'pool mut self, f: F) -> R
    where
        F: FnOnce(&Scope<'pool, 'scope>) -> R,
    {
        let scope = Scope {
            pool: self,
            _marker: PhantomData,
        };
        f(&scope)
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        trace!("Closing threadpool");
        self.injector.push(Job::Terminate);

        for job in &self.workers_info {
            job.wait();
        }
    }
}
/// A scope to executes scoped jobs in the thread pool.
pub struct Scope<'pool, 'scope> {
    pool: &'pool mut ThreadPool,
    _marker: PhantomData<::std::cell::Cell<&'scope mut ()>>,
}

impl<'pool, 'scope> Scope<'pool, 'scope> {
    /// Execute a function `task` on a thread in the thread pool.
    pub fn execute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'scope,
    {
        let task = unsafe { mem::transmute::<Func<'scope>, Func<'static>>(Box::new(task)) };
        self.pool.injector.push(Job::NewJob(Box::new(task)));
        self.pool
            .total_tasks
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::ThreadPool;

    fn fib(n: i32) -> u64 {
        if n < 0 {
            panic!("{} is negative!", n);
        }
        match n {
            0 => panic!("zero is not a right argument to fib()!"),
            1 | 2 => 1,
            3 => 2,
            /*
            50    => 12586269025,
            */
            _ => fib(n - 1) + fib(n - 2),
        }
    }

    #[test]
    fn test_threadpool() {
        let tp = ThreadPool::new_with_local_registry(8, true);
        for i in 1..45 {
            tp.execute(move || {
                fib(i);
            });
        }
    }

    #[test]
    fn test_scoped_thread() {
        let mut vec = vec![0; 100];
        let mut tp = ThreadPool::new_with_local_registry(8, true);

        tp.scoped(|s| {
            for e in vec.iter_mut() {
                s.execute(move || {
                    *e += 1;
                });
            }
        });

        tp.wait();
        assert_eq!(vec, vec![1i32; 100])
    }

    #[test]
    fn test_par_for() {
        let mut vec = vec![0; 100];
        let mut tp = ThreadPool::new_with_local_registry(8, true);

        tp.par_for(&mut vec, |el: &mut i32| *el += 1);
        tp.wait();
        assert_eq!(vec, vec![1i32; 100])
    }

    #[test]
    fn test_par_map() {
        env_logger::init();
        let mut vec = Vec::new();
        let mut tp = ThreadPool::new_with_local_registry(8, true);

        for i in 0..1000 {
            vec.push(i);
        }
        let res: Vec<String> = tp
            .par_map(vec, |el| -> String {
                String::from("Hello from: ".to_string() + &el.to_string())
            })
            .collect();

        let mut check = true;
        let mut i = 0;
        for str in res {
            if str != "Hello from: ".to_string() + &i.to_string() {
                check = false;
            }
            i += 1;
        }
        assert!(check)
    }
}
