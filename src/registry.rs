use std::{sync::{Arc, Mutex, Barrier, RwLock, atomic::{Ordering, AtomicBool}}, thread, error::Error, fmt};

use crossbeam_deque::{Stealer, Injector, Worker, Steal};
use num_cpus;
use log::{trace, error};

type Func<'a> = Box<dyn FnOnce() + Send + 'a>;

pub(super) enum Job {
    NewJob(Func<'static>),
    Terminate,
}

#[derive(Debug)]
pub struct RegistryError {
    details: String,
}

impl RegistryError {
    fn new(msg: &str) -> RegistryError {
        RegistryError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for RegistryError {
    fn description(&self) -> &str {
        &self.details
    }
}


pub struct Registry {
    workers: Vec<Arc<WorkerThread>>,
    threads: Vec<Thread>,
    global: Arc<Injector<Job>>,
}

/// Global Registry of threads.
static mut REGISTRY: Option<Arc<Registry>> = None;
static REGISTRY_INIT: std::sync::Once = std::sync::Once::new();

/// Initialize the global registry.
pub(super) fn new_global_registry(nthreads: usize, pinning: bool) -> Result<Arc<Registry>, RegistryError>  {
    match unsafe { REGISTRY.as_ref() } {
        Some(_) => Err(RegistryError::new("Global registry already initialized.")),
        None => {
            let registry = Arc::new(Registry::new(nthreads, pinning));
            unsafe { REGISTRY = Some(Arc::clone(&registry)) };
            Ok(registry)
        }
    }
}

pub(super) fn new_local_registry(nthreads: usize, pinning: bool) -> Arc<Registry> {
    Arc::new(Registry::new(nthreads, pinning))
}
/// Initialize the global registry with default settings.
pub(super) fn default_global_registry() -> Result<Arc<Registry>, RegistryError> {
    match unsafe { REGISTRY.as_ref() } {
        Some(_) => Err(RegistryError::new("Global registry already initialized.")),
        None => {
            let registry = Arc::new(Registry::new( num_cpus::get(), false));
            set_global_registry(registry.clone());
            Ok(registry)
        }
    }
}

/// Get the global registry.
pub fn get_global_registry() ->Arc<Registry> {
    match unsafe { REGISTRY.as_ref() } {
        Some(registry) => Arc::clone(registry),
        None => default_global_registry().unwrap(),
    }
}

/// Set the global registry.
pub(super) fn set_global_registry(registry: Arc<Registry>) {
    REGISTRY_INIT.call_once(|| {
        unsafe { REGISTRY = Some(registry) };
    });
}

impl Registry {
    /// Create a new threadpool with `nthreads` threads.
    /// If `pinning` is true, threads will be pinned to their cores.
    /// If `pinning` is false, threads will be free to move between cores.
    pub fn new(nthreads: usize, pinning: bool) -> Registry {
        if nthreads == 0 {
            panic!("Cannot create a threadpool with 0 threads.");
        } else if (nthreads > num_cpus::get()) && pinning {
            panic!("Cannot create a threadpool with more pinned threads than available cores. ({} > {})", nthreads, num_cpus::get());
        }

        trace!("Creating new thread registry.");
        let mut workers = Vec::new();
        let mut threads = Vec::new();
        let global = Arc::new(Injector::new());

        let barrier = Arc::new(Barrier::new(nthreads));

        for i in 0..nthreads {
            let worker = WorkerThread::new(i, pinning, Arc::clone(&global));
            workers.push(Arc::new(worker));
        }

        for worker in &workers {
            for other in &workers {
                if Arc::ptr_eq(worker, other) {
                    continue;
                }
                worker.register_stealer(other.get_stealer());
            }
            let worker_copy = Arc::clone(&worker);
            let local_barrier = Arc::clone(&barrier);

            let thread = Thread::new(worker_copy.id,  move ||
               { 
                local_barrier.wait();
                worker_copy.run();
               }
            , pinning);

            threads.push(thread);
        }
        
        Registry {
            workers,
            threads,
            global,
        }
    }

    /// Add a new thread to the threadpool.
    pub(super) fn add_worker(&mut self, pinning: bool) {
        let worker = WorkerThread::new(self.workers.len(), pinning, Arc::clone(&self.global));
        for other in &self.workers {
            worker.register_stealer(other.get_stealer());
        }
        self.workers.push(Arc::new(worker));
        let worker_copy = Arc::clone(&self.workers[self.workers.len() - 1]);
        let thread = Thread::new(worker_copy.id, move || worker_copy.run(), pinning);
        self.threads.push(thread);
    }

    ///
    pub(super) fn get_free_workers(&self, pinning: bool) -> usize {
        let mut count = 0;
        for worker in &self.workers {
            if !worker.is_busy() && worker.is_pinned() == pinning {
                count += 1;
            }
        }
        count
    }

    pub(super) fn get_range_of_contiguos_free_workers(&self, n: usize) -> Option<usize> {
        let mut count = 0;
        let mut start = 0;
        for i in 0..self.workers.len() {
            if !self.workers[i].is_busy() {
                count += 1;
                if count == n {
                    return Some(start);
                }
            } else {
                count = 0;
                start = i + 1;
            }
        }
        None
    }

    
    /// Execute a function on a specific thread.
    pub fn execute_on<F>(&self, id: usize, f: F) -> Result<(), RegistryError>
    where
        F: FnOnce() + Send + 'static,
    {
        if id >= self.get_nthreads() {
            return Err(RegistryError::new("Invalid thread id."));
        }
        let job = Job::NewJob(Box::new(f));
        self.workers[id].push(job);
        Ok(())
    }

    /// Execute a function in the threadpool.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Job::NewJob(Box::new(f));
        self.global.push(job);
    }

    pub fn get_nthreads(&self) -> usize {
        self.workers.len()
    }
    
}
impl Drop for Registry {
    fn drop(&mut self) {
        trace!("Closing thread registry.");
        self.global.push(Job::Terminate);
        for thread in &mut self.threads {
            thread.join();
        }
    }
}
/// A thread in the threadpool.
struct WorkerThread {
    id: usize,
    busy: AtomicBool,
    pinning: bool,
    global: Arc<Injector<Job>>,
    worker: Mutex<Worker<Job>>,
    stealers: RwLock<Vec<Stealer<Job>>>,
}
impl WorkerThread {
    fn new(id: usize, pinning: bool, global: Arc<Injector<Job>>) -> WorkerThread {
        let worker = Worker::new_fifo();
        WorkerThread {
            id,
            busy: AtomicBool::new(false),
            pinning,
            global,
            worker: Mutex::new(worker),
            stealers: RwLock::new(Vec::new()),
        }
    }

    fn is_pinned(&self) -> bool {
        self.pinning
    }

    fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Relaxed)
    }

    fn get_stealer(&self) -> Stealer<Job> {
        self.worker.lock().unwrap().stealer()
    }

    fn register_stealer(&self, stealer: Stealer<Job>) {
        self.stealers.write().unwrap().push(stealer);
    }

    fn run(&self) {
        trace!("Thread {} started.", self.id);
        let mut stop = false;
        loop {
            if let Some(job) = self.pop() {
                match job {
                    Job::NewJob(f) => {
                        self.busy.store(true, Ordering::SeqCst);
                        f();
                        self.busy.store(false, Ordering::SeqCst);
                    },
                    Job::Terminate => {
                        stop = true;
                    }
                }
            } else if let Some(job) = self.steal() {
                match job {
                    Job::NewJob(f) => {
                        self.busy.store(true, Ordering::SeqCst);
                        f();
                        self.busy.store(false, Ordering::SeqCst);
                    },
                    Job::Terminate => {
                        stop = true;
                    }
                }
            } else if let Some(job) = self.steal_from_global() {
                match job {
                    Job::NewJob(f) => {
                        self.busy.store(true, Ordering::SeqCst);
                        f();
                        self.busy.store(false, Ordering::SeqCst);
                    },
                    Job::Terminate => {
                        stop = true;
                    }
                }
            } else {
                if stop {
                    self.global.push(Job::Terminate);
                    break;
                }
                thread::yield_now();
            }
        }
        trace!("Thread {} stopped.", self.id);
    }

    fn pop(&self) -> Option<Job> {
        self.worker.lock().unwrap().pop()
    }
    
    pub(super) fn push(&self, job: Job) {
        self.worker.lock().unwrap().push(job);
    }

    fn steal(&self) -> Option<Job> {
        let stealers = self.stealers.read().unwrap();
        for stealer in stealers.iter() {
            loop {
                match stealer.steal() {
                    Steal::Success(job) => return Some(job),
                    Steal::Empty => break,
                    Steal::Retry => continue,
                }
            }
        }
        None
    }

    fn steal_from_global(&self) -> Option<Job> {
        loop {
            match self.global.steal() {
                Steal::Success(job) => return Some(job),
                Steal::Empty => return None,
                Steal::Retry => continue,
            };
        }
    }
}

/// A thread in the threadpool.
pub struct Thread {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
    pinning: bool,
}
impl Thread {
    /// Create a new thread.
    fn new<F>(id: usize, f: F, pinning: bool) -> Thread
    where
        F: FnOnce() + Send + 'static,
    {
        let mut pinning = pinning;
        if id > num_cpus::get() && pinning {
            error!("Cannot pin a thread in a position greater than the number of cores. Proceding without pinning.");
            pinning = false;
        }
        Thread {
            id,
            thread: Some(thread::spawn(move || {
                if pinning {
                    let mut core_ids = core_affinity::get_core_ids().unwrap();
                    if core_ids.get(id).is_none() {
                        panic!("Cannot pin the thread in the choosen position.");
                    } else {
                        let core = core_ids.remove(id);
                        let err = core_affinity::set_for_current(core);
                        if !err {
                            error!("Thread pinning for thread[{}] failed!", id);
                        } else {
                            trace!("Thread[{}] correctly pinned on {}!", id, core.id);
                        }
                    }
                }
                trace!("{:?} started", thread::current().id());
                (f)();
                trace!("{:?} now will end.", thread::current().id());
            })),
            pinning,
        }
    }

    fn id(&self) -> usize {
        self.id
    }

    fn is_pinned(&self) -> bool {
        self.pinning
    }

    /// Join the thread.
    fn join(&mut self) {
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_registry() {
        let registry = Registry::new(4, false);
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..1000 {
            let counter_copy = Arc::clone(&counter);
            registry.execute( move || {
                counter_copy.fetch_add(1, Ordering::SeqCst);
            });
        }
        thread::sleep(Duration::from_millis(100));
        drop(registry);
        assert_eq!(counter.load(Ordering::SeqCst), 1000);
    }

    #[test]
    fn test_only_one_global() {
        let mut check = false;
        let registry_a = new_global_registry(4, true);
        let registry_b = new_global_registry(4, true);
        if registry_b.is_err() && registry_a.is_ok() {
            check = true;
        }

        assert_eq!(check, true);
    }

    #[test]
    fn default_global_registry() {
        let registry = get_global_registry();
        assert_eq!(registry.get_nthreads(), num_cpus::get());
    }
}