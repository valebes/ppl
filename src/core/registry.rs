use std::{sync::{Arc, Mutex, Barrier, RwLock, atomic::{Ordering, AtomicBool}, Once}, thread, error::Error, fmt, hint};

use crossbeam_deque::{Stealer, Injector, Worker, Steal};
use num_cpus;
use log::{trace, error};

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
    pinning: bool,
}

/// Global Registry of threads.
static mut REGISTRY: Option<Arc<Registry>> = None;
static REGISTRY_INIT: Once = std::sync::Once::new();

/// Initialize the global registry.
pub(crate) fn new_global_registry(nthreads: usize, pinning: bool) -> Result<Arc<Registry>, RegistryError>  {
    match unsafe { REGISTRY.as_ref() } {
        Some(_) => Err(RegistryError::new("Global registry already initialized.")),
        None => {
            let registry = Arc::new(Registry::new(nthreads, pinning));
            unsafe { REGISTRY = Some(Arc::clone(&registry)) };
            Ok(registry)
        }
    }
}

pub(crate) fn new_local_registry(nthreads: usize, pinning: bool) -> Arc<Registry> {
    Arc::new(Registry::new(nthreads, pinning))
}
/// Initialize the global registry with default settings.
pub(crate) fn default_global_registry() -> Result<Arc<Registry>, RegistryError> {
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
pub fn get_global_registry() -> Arc<Registry> {
    match unsafe { REGISTRY.as_ref() } {
        Some(registry) => Arc::clone(registry),
        None => default_global_registry().unwrap(),
    }
}

/// Set the global registry.
fn set_global_registry(registry: Arc<Registry>) {
    REGISTRY_INIT.call_once(|| {
        unsafe { REGISTRY = Some(registry) };
    });
}

impl Registry {
    /// Create a new threadpool with `nthreads` threads.
    /// If `pinning` is true, threads will be pinned to their cores.
    /// If `pinning` is false, threads will be free to move between cores.
    fn new(nthreads: usize, pinning: bool) -> Registry {
        if nthreads == 0 {
            panic!("Cannot create a threadpool with 0 threads.");
        }

        trace!("Creating new thread registry.");
        let mut workers = Vec::new();
        let global = Arc::new(Injector::new());

        for i in 0..nthreads {
            let mut worker = WorkerThread::new(i, pinning, Arc::clone(&global));
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
            pinning,
        }
    }

    /// Add a new thread to the threadpool.
    /// The function passed will be executed as first job on the new thread.
    fn add_worker<F>(&mut self, f: F) -> JobInfo
    where F: FnOnce() + Send + 'static
    {
        let id = self.workers.len();
        let mut worker = WorkerThread::new(id, self.pinning, Arc::clone(&self.global));
        
        let job_info = JobInfo::new(id);
        let status_copy = Arc::clone(&job_info.status);

        worker.push(Job::NewJob(Box::new(move || {
            f();
            status_copy.store(true, Ordering::SeqCst);
        })));

        for other in &self.workers {
            worker.register_stealer(other.get_stealer());
        }

        for other in &self.workers {
            other.register_stealer(worker.get_stealer());
        }

        self.workers.push(worker);
        job_info
    }


    ///
    pub(crate) fn get_free_workers(&self) -> usize {
        let mut count = 0;
        for worker in &self.workers {
            if !worker.is_busy() {
                count += 1;
            }
        }
        count
    }

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
        if id >= self.get_nworkers() {
            return Err(RegistryError::new("Invalid thread id."));
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

    pub fn get_nworkers(&self) -> usize {
        self.workers.len()
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


struct WorkerThread {
    WorkerInfo: Arc<WorkerInfo>,
    Thread: Thread,
}
impl WorkerThread {
    fn new (id: usize, pinning: bool, global: Arc<Injector<Job>>) -> WorkerThread {
        let worker = Arc::new(WorkerInfo::new(id, pinning, global));
        let worker_copy = Arc::clone(&worker);
        let thread = Thread::new(worker_copy.id, move || worker_copy.run(), pinning);
        WorkerThread {
            WorkerInfo: worker,
            Thread: thread,
        }
    }

    fn join(&mut self) {
        self.Thread.join();
    }

    fn is_busy(&self) -> bool {
        self.WorkerInfo.is_busy()
    }

    fn get_id(&self) -> usize {
        self.WorkerInfo.id
    }

    fn push(&self, job: Job) {
        self.WorkerInfo.push(job);
    }

    fn get_stealer(&self) -> Stealer<Job> {
        self.WorkerInfo.get_stealer()
    }

    fn register_stealer(&self, stealer: Stealer<Job>) {
        self.WorkerInfo.register_stealer(stealer);
    }

}

/// A thread in the threadpool.
struct WorkerInfo {
    id: usize,
    busy: AtomicBool,
    global: Arc<Injector<Job>>,
    worker: Mutex<Worker<Job>>,
    stealers: RwLock<Vec<Stealer<Job>>>,
}
impl WorkerInfo {
    fn new(id: usize, pinning: bool, global: Arc<Injector<Job>>) -> WorkerInfo {
        let worker = Worker::new_fifo();
        WorkerInfo {
            id,
            busy: AtomicBool::new(false),
            global,
            worker: Mutex::new(worker),
            stealers: RwLock::new(Vec::new()),
        }
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
    
    fn push(&self, job: Job) {
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
struct Thread {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}
impl Thread {
    /// Create a new thread.
    fn new<F>(id: usize, f: F, pinning: bool) -> Thread
    where
        F: FnOnce() + Send + 'static,
    {
        let pinning_position = id % num_cpus::get();
        Thread {
            id,
            thread: Some(thread::spawn(move || {
                if pinning {
                    let mut core_ids = core_affinity::get_core_ids().unwrap();
                    if core_ids.get(pinning_position).is_none() {
                        panic!("Cannot pin the thread in the choosen position.");
                    } else {
                        let core = core_ids.remove(pinning_position);
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
        }
    }

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
        assert_eq!(registry.get_nworkers(), num_cpus::get());
    }
}