//! Work-stealing based thread pool.
//!
//! This module contains the implementation of a work-stealing based thread pool.
//! This implementation of the thread pool supports scoped jobs.
//! This module offers struct as [`ThreadPool`] that allows to create a thread pool.
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use log::{trace, warn};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::{hint, mem, thread};

use crate::core::orchestrator::{get_global_orchestrator, JobInfo, Orchestrator};
use crate::mpsc::channel::Channel;

type Func<'a> = Box<dyn FnOnce() + Send + 'a>;

enum Job {
    NewJob(Func<'static>),
    Terminate,
}

/// Struct representing a worker in the thread pool.
struct ThreadPoolWorker {
    id: usize,
    worker: Worker<Job>,
    stealers: Option<Vec<Stealer<Job>>>,
    global: Arc<Injector<Job>>,
    total_tasks: Arc<AtomicUsize>,
}
impl ThreadPoolWorker {
    fn new(id: usize, global: Arc<Injector<Job>>, total_tasks: Arc<AtomicUsize>) -> Self {
        Self {
            id,
            worker: Worker::new_fifo(),
            stealers: None,
            global,
            total_tasks,
        }
    }

    /// Get stealer.
    fn get_stealer(&self) -> Stealer<Job> {
        self.worker.stealer()
    }

    // Set the stealers vector of the worker.
    fn set_stealers(&mut self, stealers: Vec<Stealer<Job>>) {
        self.stealers = Some(stealers);
    }

    /// Fetch a task. If the local queue is empty, try to steal a batch of tasks from the global queue.
    /// If the global queue is empty, try to steal a task from one of the other threads.
    fn fetch_task(&self) -> Option<Job> {
        if let Some(job) = self.pop() {
            return Some(job);
        } else if let Some(job) = self.steal_from_global() {
            return Some(job);
        } else if let Some(job) = self.steal() {
            return Some(job);
        }
        None
    }

    /// This is the main loop of the thread.
    fn run(&self) {
        trace!("Worker {} started", self.id);
        let mut stop = false;
        loop {
            let res = self.fetch_task();
            match res {
                Some(task) => match task {
                    Job::NewJob(func) => {
                        (func)();
                        self.task_done();
                    }
                    Job::Terminate => stop = true,
                },
                None => {
                    if stop {
                        self.global.push(Job::Terminate);
                        break;
                    } else {
                        thread::yield_now();
                    }
                }
            }
        }
    }

    // Pop a job from the local queue.
    fn pop(&self) -> Option<Job> {
        self.worker.pop()
    }

    // Steal a job from another worker.
    fn steal(&self) -> Option<Job> {
        if let Some(stealers) = &self.stealers {
            for stealer in stealers {
                loop {
                    match stealer.steal() {
                        Steal::Success(job) => return Some(job),
                        Steal::Empty => break,
                        Steal::Retry => continue,
                    }
                }
            }
        }

        None
    }

    // Steal a job from the global queue.
    fn steal_from_global(&self) -> Option<Job> {
        loop {
            match self.global.steal_batch_and_pop(&self.worker) {
                Steal::Success(job) => return Some(job),
                Steal::Empty => return None,
                Steal::Retry => continue,
            };
        }
    }

    // Warn task done.
    fn task_done(&self) {
        self.total_tasks.fetch_sub(1, Ordering::AcqRel);
    }
}
///Struct representing a thread pool.
pub struct ThreadPool {
    jobs_info: Vec<JobInfo>,
    num_workers: usize,
    total_tasks: Arc<AtomicUsize>,
    injector: Arc<Injector<Job>>,
    orchestrator: Arc<Orchestrator>,
}

impl Clone for ThreadPool {
    /// Create a new thread pool from an existing one, using the same number of threads.
    fn clone(&self) -> Self {
        let orchestrator = self.orchestrator.clone();
        ThreadPool::build(self.num_workers, orchestrator)
    }
}

impl Default for ThreadPool {
    /// Create a new thread pool with all the availables threads.
    /// # Examples
    ///
    /// ```
    /// use ppl::thread_pool::ThreadPool;
    ///     
    /// let mut pool = ThreadPool::default();
    /// ```
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadPool {
    fn build(num_threads: usize, orchestrator: Arc<Orchestrator>) -> Self {
        trace!("Creating new threadpool");
        let jobs_info;
        let mut workers = Vec::with_capacity(num_threads);

        let mut stealers = Vec::with_capacity(num_threads);

        let total_tasks = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut funcs = Vec::with_capacity(num_threads);

        let injector = Arc::new(Injector::new());

        // Create workers.
        for i in 0..num_threads {
            let global = Arc::clone(&injector);
            let total_tasks_cp = Arc::clone(&total_tasks);
            let worker = ThreadPoolWorker::new(i, global, total_tasks_cp);
            workers.push(worker);
        }

        // Get stealers.
        for worker in &workers {
            let stealer = worker.get_stealer();
            stealers.push(stealer);
        }

        // For each worker, set the stealers vector.
        // I remove the stealer of the worker itself from the vector.
        (0..num_threads).for_each(|i| {
            let mut stealers_cp = stealers.clone();
            stealers_cp.remove(i);
            workers[i].set_stealers(stealers_cp);
        });

        // Push workers to the orchestrator.
        while !workers.is_empty() {
            let barrier = Arc::clone(&barrier);
            let worker = workers.remove(0);
            let func = move || {
                barrier.wait();
                worker.run();
            };
            funcs.push(Box::new(func));
        }

        jobs_info = orchestrator.push_jobs(funcs);

        Self {
            num_workers: num_threads,
            jobs_info,
            total_tasks,
            injector,
            orchestrator,
        }
    }

    /// Create a new thread pool using all the threads availables.
    ///
    /// # Examples
    ///
    /// ```
    /// use ppl::thread_pool::ThreadPool;
    ///     
    /// let mut pool = ThreadPool::new();
    /// ```
    pub fn new() -> Self {
        Self::with_capacity(num_cpus::get())
    }

    /// Create a new thread pool with `num_threads` threads.
    ///
    /// # Examples
    ///
    /// ```
    /// use ppl::thread_pool::ThreadPool;
    ///     
    /// let mut pool = ThreadPool::with_capacity(8);
    /// ```
    pub fn with_capacity(num_threads: usize) -> Self {
        let orchestrator = get_global_orchestrator();
        Self::build(num_threads, orchestrator)
    }

    /// Execute a function `task` on a thread in the thread pool.
    /// This method is non-blocking, so the developer must call `wait` to wait for the task to finish.
    pub fn execute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.injector.push(Job::NewJob(Box::new(task)));
        self.total_tasks.fetch_add(1, Ordering::AcqRel);
    }

    /// Check if there are jobs in the thread pool.
    pub fn is_empty(&self) -> bool {
        self.total_tasks.load(Ordering::Acquire) == 0 && self.injector.is_empty()
    }

    /// Block until all current jobs in the thread pool are finished.
    pub fn wait(&self) {
        while !self.is_empty() {
            hint::spin_loop();
        }
    }

    /// Given a function `f`, a range of indices `range`, and a chunk size `chunk_size`,
    /// it distributes works of size `chunk_size` to the threads in the pool.
    /// The function `f` is applied to each element in the range.
    /// The range is split in chunks of size `chunk_size` and each chunk is assigned to a thread.
    pub fn par_for<F>(&mut self, range: Range<usize>, chunk_size: usize, mut f: F)
    where
        F: FnMut(usize) + Send + Copy,
    {
        let mut start = range.start;
        let mut end = start + chunk_size;

        self.scope(|s| {
            while start < range.end {
                if end > range.end {
                    end = range.end;
                }

                let range = start..end;

                s.execute(move || {
                    for i in range {
                        (f)(i);
                    }
                });
                start = end;
                end = start + chunk_size;
            }
        });
    }

    /// Applies in parallel the function `f` on a iterable object `iter`.
    ///
    /// # Examples
    ///
    /// Increment of 1 all the elements in a vector concurrently:
    ///
    /// ```
    /// use ppl::thread_pool::ThreadPool;
    ///
    /// let mut pool = ThreadPool::new();
    /// let mut vec = vec![0; 100];
    ///
    /// pool.par_for_each(&mut vec, |el: &mut i32| *el = *el + 1);
    ///
    pub fn par_for_each<Iter, F>(&mut self, iter: Iter, f: F)
    where
        F: FnOnce(Iter::Item) + Send + Copy,
        <Iter as IntoIterator>::Item: Send,
        Iter: IntoIterator,
    {
        self.scope(|s| {
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
    /// use ppl::thread_pool::ThreadPool;
    ///
    /// let mut pool = ThreadPool::new();
    /// let mut vec = vec![0i32; 100];
    ///
    /// let res: Vec<String> = pool.par_map(&mut vec, |el| -> String {
    ///            String::from("Hello from: ".to_string() + &el.to_string())
    ///       }).collect();
    ///
    pub fn par_map<Iter, F, R>(&mut self, iter: Iter, f: F) -> impl Iterator<Item = R>
    where
        F: FnOnce(Iter::Item) -> R + Send + Copy,
        <Iter as IntoIterator>::Item: Send,
        R: Send + 'static,
        Iter: IntoIterator,
    {
        let blocking = self.orchestrator.get_configuration().get_wait_policy();

        let (rx, tx) = Channel::channel(blocking);
        let arc_tx = Arc::new(tx);
        let mut ordered_map = BTreeMap::<usize, R>::new();

        self.scope(|s| {
            iter.into_iter().enumerate().for_each(|el| {
                let cp = Arc::clone(&arc_tx);
                s.execute(move || {
                    let err = cp.send((el.0, f(el.1)));
                    if err.is_err() {
                        panic!("Error: {}", err.unwrap_err());
                    }
                });
            });

            drop(arc_tx);

            let mut disconnected = false;

            while !disconnected {
                match rx.receive() {
                    Ok(Some((k, v))) => {
                        ordered_map.insert(k, v);
                    }
                    Ok(None) => {
                        continue;
                    }
                    Err(e) => {
                        // The channel is closed. We can exit the loop.
                        warn!("Error: {}", e);
                        disconnected = true;
                    }
                }
            }
        });
        ordered_map.into_values()
    }

    /// Parallel Map Reduce.
    /// Applies in parallel the function `f` on a iterable object `iter`,
    /// producing a new object with the results.
    /// The function `f` must return a tuple of two elements, the first one
    /// is the key and the second one is the value.
    /// The results are grouped by key and reduced by the function `reduce`.
    /// The function `reduce` must take two arguments, the first one is the
    /// key and the second one is a vector of values.
    /// The function `reduce` must return a tuple of two elements, the first one
    /// is the key and the second one is the value.
    /// This method return an iterator of tuples of two elements, the first one
    /// is the key and the second one is the value.
    ///
    /// # Examples
    ///
    /// ```
    /// use ppl::thread_pool::ThreadPool;
    ///     
    /// let mut pool = ThreadPool::with_capacity(8);
    /// let mut vec = Vec::new();
    ///
    /// for i in 0..100 {
    ///    vec.push(i);
    /// }
    ///
    /// let res: Vec<(i32, i32)> = pool.par_map_reduce(&mut vec, |el| -> (i32, i32) {
    ///           (*el % 10, *el)
    ///      }, |k, v| -> (i32, i32) {
    ///          (k, v.iter().sum())
    ///     }).collect();
    /// assert_eq!(res.len(), 10);
    /// ```
    pub fn par_map_reduce<Iter, F, K, V, R, Reduce>(
        &mut self,
        iter: Iter,
        f: F,
        reduce: Reduce,
    ) -> impl Iterator<Item = (K, R)>
    where
        F: FnOnce(Iter::Item) -> (K, V) + Send + Copy,
        <Iter as IntoIterator>::Item: Send,
        K: Send + Ord + 'static,
        V: Send + 'static,
        R: Send + 'static,
        Reduce: FnOnce(K, Vec<V>) -> (K, R) + Send + Copy,
        Iter: IntoIterator,
    {
        let map = self.par_map(iter, f);
        self.par_reduce(map, reduce)
    }

    /// Reduces in parallel the elements of an iterator `iter` by the function `f`.
    /// The function `f` must take two arguments, the first one is the
    /// key and the second one is a vector of values.
    /// The function `f` must return a tuple of two elements, the first one
    /// is the key and the second one is the value.
    /// This method take in input an iterator, it groups the elements by key and then
    /// reduces them by the function `f`.
    /// This method return an iterator of tuples of two elements, the first one
    /// is the key and the second one is the value obtained by the function `f`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ppl::thread_pool::ThreadPool;
    ///
    /// let mut pool = ThreadPool::new();
    ///
    /// let mut vec = Vec::new();
    ///
    /// for i in 0..100 {
    ///   vec.push((i % 10, i));
    /// }
    ///
    /// let res: Vec<(i32, i32)> = pool.par_reduce(vec, |k, v| -> (i32, i32) {
    ///          (k, v.iter().sum())
    ///    }).collect();
    /// assert_eq!(res.len(), 10);
    /// ```
    pub fn par_reduce<Iter, K, V, R, F>(&mut self, iter: Iter, f: F) -> impl Iterator<Item = (K, R)>
    where
        <Iter as IntoIterator>::Item: Send,
        K: Send + Ord + 'static,
        V: Send + 'static,
        R: Send + 'static,
        F: FnOnce(K, Vec<V>) -> (K, R) + Send + Copy,
        Iter: IntoIterator<Item = (K, V)>,
    {
        // Shuffle by grouping the elements by key.
        let mut ordered_map = BTreeMap::new();
        for (k, v) in iter {
            ordered_map.entry(k).or_insert_with(Vec::new).push(v);
        }
        // Reduce the elements by key.
        self.par_map(ordered_map, move |(k, v)| f(k, v))
    }

    /// Create a new scope to execute jobs on other threads.
    /// The function passed to this method will be provided with a [`Scope`] object,
    /// which can be used to spawn new jobs through the [`Scope::execute`] method.
    /// The scope will block the current thread until all jobs spawned from this scope
    /// have completed.
    ///
    /// # Examples
    ///
    /// ```
    /// use ppl::thread_pool::ThreadPool;
    ///
    /// let mut pool = ThreadPool::new();
    ///
    /// let mut vec = vec![0; 100];
    ///
    /// pool.scope(|scope| {
    ///    for el in &mut vec {
    ///       scope.execute(move || {
    ///          *el += 1;
    ///      });
    ///   }
    /// });
    ///
    /// assert_eq!(vec.iter().sum::<i32>(), 100);
    pub fn scope<'pool, 'scope, F, R>(&'pool mut self, f: F) -> R
    where
        F: FnOnce(&Scope<'pool, 'scope>) -> R,
    {
        let scope = Scope {
            pool: self,
            _marker: PhantomData,
        };
        let res = f(&scope);
        scope.pool.wait();
        res
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.injector.push(Job::Terminate);

        for job in &self.jobs_info {
            job.wait();
        }
    }
}
/// A scope to execute jobs on other threads.
pub struct Scope<'pool, 'scope> {
    pool: &'pool mut ThreadPool,
    _marker: PhantomData<::std::cell::Cell<&'scope mut ()>>,
}
impl<'pool, 'scope> Scope<'pool, 'scope> {
    /// Execute a function `task` on a thread in the thread pool.
    /// At the end of the scope, all the job will be terminated.
    pub fn execute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'scope,
    {
        let task = unsafe { mem::transmute::<Func<'scope>, Func<'static>>(Box::new(task)) };
        self.pool.execute(task);
    }
}
