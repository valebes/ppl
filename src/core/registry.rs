use std::{sync::{Arc, Mutex, RwLock, atomic::{Ordering, AtomicBool}}, thread, error::Error, fmt, hint};
use once_cell::sync::{Lazy, OnceCell};
use crossbeam_deque::{Stealer, Injector, Worker, Steal};
use num_cpus;
use log::{trace, error};

use super::configuration::{Configuration};

type Func<'a> = Box<dyn FnOnce() + Send + 'a>;

pub enum Job {
    NewJob(Func<'static>),
    Terminate,
}

#[derive(Debug)]
pub(crate) struct JobInfo {
    id_worker: usize,
    status: Arc<AtomicBool>,
}
impl JobInfo {
    fn new(id_worker: usize) -> JobInfo {
        JobInfo {
            id_worker: id_worker,
            status: Arc::new(AtomicBool::new(false)),
        }
    }

    fn get_id_worker(&self) -> usize {
        self.id_worker
    }

    pub(crate) fn wait(&self) {
        while !self.status.load(Ordering::SeqCst) {
            hint::spin_loop();
        }
    }
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
    workers: Vec<WorkerThread>,
    global: Arc<Injector<Job>>,
    configuration: Arc<Configuration>,
}

/// Global Registry of threads.
static mut REGISTRY: OnceCell<Arc<Registry>> = OnceCell::new();

/// Initialize the global registry with custom settings.
pub(crate) fn new_custom_global_registry(nthreads: usize, pinning: bool) -> Result<Arc<Registry>, RegistryError>  {
    let configuration = Configuration::new(nthreads, pinning, false);
    let registry = Arc::new(Registry::new(configuration));
    set_global_registry(registry.clone())
}

/// Initialize a local registry with custom settings.
pub(crate) fn new_local_registry(nthreads: usize, pinning: bool) -> Arc<Registry> {
    let configuration =Configuration::new(nthreads, pinning, false);
    Arc::new(Registry::new(configuration))
}

/// Initialize the global registry with default settings.
pub(crate) fn default_global_registry() -> Result<Arc<Registry>, RegistryError> {
    let configuration = Configuration::new_default();
    let registry = Arc::new(Registry::new(configuration));
    set_global_registry(registry.clone())
}

/// Get the global registry.
pub fn get_global_registry() -> Arc<Registry> {
    unsafe { REGISTRY.get_or_init(|| -> Arc<Registry> {
        let configuration = Configuration::new_default();
        Arc::new(Registry::new(configuration))
    }).clone() }
}

/// Set the global registry.
fn set_global_registry(registry: Arc<Registry>) -> Result<Arc<Registry>, RegistryError> {
    let mut result = Err(RegistryError::new("Woops! Something went wrong.")); // Nobody should see this.
    unsafe { let _err = REGISTRY
        .set(registry)
        .map(|_| {
            result = Ok(get_global_registry())}
        ); };
    result
}

impl Registry {
    /// Create a new registry.
    fn new(configuration: Configuration) -> Registry {
        trace!("Creating new thread registry.");
        let mut workers = Vec::new();
        let global = Arc::new(Injector::new());
        let configuration = Arc::new(configuration);

        for i in 0..configuration.get_max_threads() {
            let worker = WorkerThread::new(i, configuration.clone(), Arc::clone(&global));
            workers.push(worker);
        }

        for worker in &workers {
            for other in &workers {
                if worker.get_id() != other.get_id() {
                    worker.register_stealer(other.get_stealer());
                }
            }
        }

        Registry {
            workers,
            global,
            configuration,
        }
    }

    //todo: this is not the best way to do this
    // CHANGE THIS FUNCTION
    pub(crate) fn get_range_of_contiguos_free_workers(&self, n: usize) -> Option<usize> {
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
    pub(crate) fn execute_on<F>(&self, id: usize, f: F) -> Result<JobInfo, RegistryError>
    where
        F: FnOnce() + Send + 'static,
    {
        if id >= self.get_max_threads() {
            return Err(RegistryError::new("Thread id out of bounds."));
        }

        let job_info = JobInfo::new(id);
        let status_copy = Arc::clone(&job_info.status);

        self.workers[id].push(Job::NewJob(Box::new(move || {
            f();
            status_copy.store(true, Ordering::SeqCst);
    }
    )));

        Ok(job_info)
    }

    /// Execute a function in the threadpool.
    pub(crate) fn execute<F>(&self, f: F) -> JobInfo
    where
        F: FnOnce() + Send + 'static,
    {
        let job_info = JobInfo::new(0); //Maybe change this
        let status_copy = Arc::clone(&job_info.status);

        self.global.push(Job::NewJob(Box::new(move || {
            f();
            status_copy.store(true, Ordering::SeqCst);
    }
    )));

        job_info
    }

    pub(crate) fn threads_pinning(&self) -> bool {
        self.configuration.get_pinning()
    }

    /// Get the number of threads in the registry.
    pub(crate) fn get_max_threads(&self) -> usize {
        self.configuration.get_max_threads()
    }
    
}
impl Drop for Registry {
    fn drop(&mut self) {
        trace!("Closing thread registry.");
        self.global.push(Job::Terminate);
        for worker in &mut self.workers {
            worker.join();
        }
    }
}

/// A thread worker.
/// This is the thread that will execute the jobs.
/// It will steal jobs from other workers if it has nothing to do.
/// It will also execute the jobs in the global queue.
/// It will terminate when it receives a terminate message.
struct WorkerThread {
    worker_info: Arc<WorkerInfo>,
    thread: Thread,
}
impl WorkerThread {
    /// Create a new worker thread.
    /// It will start executing jobs immediately.
    /// It will terminate when it receives a terminate message.
    fn new (id: usize, config: Arc<Configuration>, global: Arc<Injector<Job>>) -> WorkerThread {
        let worker = Arc::new(WorkerInfo::new(id, global));
        let worker_copy = Arc::clone(&worker);
        let thread = Thread::new(worker_copy.id, move || worker_copy.run(), config.clone());
        WorkerThread {
            worker_info: worker,
            thread,
        }
    }

    /// Join the thread.
    fn join(&mut self) {
        self.thread.join();
    }

    /// Check if the thread is busy.
    fn is_busy(&self) -> bool {
        self.worker_info.is_busy()
    }

    /// Get the id of the thread.
    fn get_id(&self) -> usize {
        self.worker_info.id
    }

    /// Push a job to the thread.
    fn push(&self, job: Job) {
        self.worker_info.push(job);
    }

    /// Get the stealer of the thread.
    fn get_stealer(&self) -> Stealer<Job> {
        self.worker_info.get_stealer()
    }

    /// Register a stealer to the thread.
    fn register_stealer(&self, stealer: Stealer<Job>) {
        self.worker_info.register_stealer(stealer);
    }

}

/// Information about a worker thread.
/// This is used to keep track of the state of the thread.
struct WorkerInfo {
    id: usize,
    busy: AtomicBool,
    global: Arc<Injector<Job>>,
    worker: Mutex<Worker<Job>>,
    stealers: RwLock<Vec<Stealer<Job>>>,
}
impl WorkerInfo {
    /// Create a new worker info.
    fn new(id: usize, global: Arc<Injector<Job>>) -> WorkerInfo {
        let worker = Worker::new_fifo();
        WorkerInfo {
            id,
            busy: AtomicBool::new(false),
            global,
            worker: Mutex::new(worker),
            stealers: RwLock::new(Vec::new()),
        }
    }

    /// Check if the thread is busy.
    fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Relaxed)
    }

    /// Get the stealer of the thread.
    fn get_stealer(&self) -> Stealer<Job> {
        self.worker.lock().unwrap().stealer()
    }

    /// Register a stealer to the thread.
    fn register_stealer(&self, stealer: Stealer<Job>) {
        self.stealers.write().unwrap().push(stealer);
    }

    /// This is the main loop of the thread.
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

    /// Pop a job from the thread.
    fn pop(&self) -> Option<Job> {
        self.worker.lock().unwrap().pop()
    }
    
    /// Push a job to the thread queue.
    fn push(&self, job: Job) {
        self.worker.lock().unwrap().push(job);
    }

    /// Steal a job from another thread.
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

    /// Steal a job from the global queue.
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
struct Thread {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}
impl Thread {
    /// Create a new thread.
    fn new<F>(id: usize, f: F, configuration: Arc<Configuration>) -> Thread
    where
        F: FnOnce() + Send + 'static,
    {   
        // Get the pinning position
        let pinning_position = configuration.get_thread_mapping().get(id).unwrap().clone();
        // Create the thread and pin it if needed
        Thread {
            id,
            thread: Some(thread::spawn(move || {
                if configuration.get_pinning() {
                    let mut core_ids = core_affinity::get_core_ids().unwrap();
                    if core_ids.get(pinning_position).is_none() {
                        panic!("Cannot pin the thread in the choosen position.");
                    } else {
                        let core = core_ids.remove(pinning_position);
                        let err = core_affinity::set_for_current(core);
                        if !err {
                            error!("Thread pinning for thread[{}] on core {} failed!", pinning_position, core.id);
                        } else {
                            trace!("Thread[{}] correctly pinned on {}!", id, core.id);
                        }
                    }
                }
                trace!("{:?} started", thread::current().id());
                (f)();
                trace!("{:?} now will end.", thread::current().id());
            })),
        }
    }

    /// Get the id of the thread.
    fn id(&self) -> usize {
        self.id
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
    use serial_test::serial;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    #[serial]
    fn test_registry() {
        let registry = get_global_registry();
        // Drop the registry at the end of the test
        ::scopeguard::defer!( unsafe {drop(REGISTRY.take())});
       

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
    #[serial]
    fn test_only_one_global() {
        let registry_a = new_custom_global_registry(4, true);
        assert!(registry_a.is_ok());
        let registry_b = new_custom_global_registry(4, true);
        assert!(registry_b.is_err());
        // Drop the registry at the end of the test
        ::scopeguard::defer!({ unsafe {
            drop(REGISTRY.take());
            }
        });
    }

    #[test]
    #[serial]
    fn test_default_global_registry() {
        let registry = default_global_registry().unwrap();
                // Drop the registry at the end of the test
                ::scopeguard::defer!({ unsafe {
                    drop(REGISTRY.take());
                }
                });

        assert_eq!(registry.get_max_threads(), num_cpus::get());
    }

    #[test]
    #[serial]
    fn test_custom_global_registry() {
        let registry = new_custom_global_registry(4, true).unwrap();
        // Drop the registry at the end of the test
        ::scopeguard::defer!({ unsafe {
                drop(REGISTRY.take());
            }
        });
        assert_eq!(registry.get_max_threads(), 4);
    }
}