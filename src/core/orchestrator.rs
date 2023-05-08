use std::{
    cell::OnceCell,
    hint,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    thread::{self},
};

use crossbeam_deque::{Injector, Steal, Worker};
use log::{error, trace};

use super::configuration::Configuration;

type Func<'a> = Box<dyn FnOnce() + Send + 'a>;

pub enum Job {
    NewJob(Func<'static>),
    Terminate,
}

#[derive(Debug)]
pub(crate) struct JobInfo {
    status: Arc<AtomicBool>,
}
impl JobInfo {
    fn new() -> JobInfo {
        JobInfo {
            status: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn wait(&self) {
        while !self.status.load(Ordering::Acquire) {
            hint::spin_loop();
        }
    }
}

/// An executor.
/// It is a thread that execute jobs.
/// It will fetch jobs from it's own queue or from the global queue.
struct Executor {
    worker_info: Arc<ExecutorInfo>,
    thread: Thread,
}
impl Executor {
    /// Create a executor.
    /// It will start executing jobs immediately.
    /// It will terminate when it receives a terminate message.
    fn new(
        core_id: usize,
        config: Arc<Configuration>,
        available_workers: Arc<AtomicUsize>,
        global: Arc<Injector<Job>>,
    ) -> Executor {
        let worker = Arc::new(ExecutorInfo::new(core_id, available_workers, global));
        let worker_copy = Arc::clone(&worker);
        let thread = Thread::new(
            worker_copy.core_id,
            move || {
                worker_copy.run();
            },
            config,
        );
        Executor {
            worker_info: worker,
            thread,
        }
    }

    /// Join the thread running the executor.
    fn join(&mut self) {
        self.thread.join();
    }

    /// Push a job to the executor queue.
    fn push(&self, job: Job) {
        self.worker_info.push(job);
    }
}

/// Information about the executor.
/// It contains the queue of jobs and the number of available executors in it's partition.
struct ExecutorInfo {
    core_id: usize,
    available_workers: Arc<AtomicUsize>,
    global: Arc<Injector<Job>>,
    worker: Mutex<Worker<Job>>,
}
impl ExecutorInfo {
    /// Create a new executor info.
    fn new(
        core_id: usize,
        available_workers: Arc<AtomicUsize>,
        global: Arc<Injector<Job>>,
    ) -> ExecutorInfo {
        let worker = Worker::new_fifo();
        ExecutorInfo {
            core_id,
            available_workers,
            global,
            worker: Mutex::new(worker),
        }
    }

    // Warn that the executor is available.
    fn warn_available(&self) {
        self.available_workers.fetch_add(1, Ordering::Release);
    }

    // Warn that the executor is busy.
    fn warn_busy(&self) {
        if self.available_workers.load(Ordering::Acquire) > 0 {
            self.available_workers.fetch_sub(1, Ordering::Release);
        }
    }

    /// This is the main loop of the executor.
    /// Need refactoring.
    fn run(&self) {
        let mut stop = false;
        loop {
            if let Some(job) = self.pop() {
                match job {
                    Job::NewJob(f) => {
                        self.warn_busy();
                        f();
                        self.warn_available();
                    }
                    Job::Terminate => {
                        stop = true;
                    }
                }
            } else if let Some(job) = self.steal_from_global() {
                match job {
                    Job::NewJob(f) => {
                        self.warn_busy();
                        f();
                        self.warn_available();
                    }
                    Job::Terminate => {
                        stop = true;
                    }
                }
            } else {
                if stop {
                    self.warn_busy();
                    self.global.push(Job::Terminate);
                    break;
                }
                thread::yield_now();
            }
        }
    }

    /// Pop a job from the executor queue.
    fn pop(&self) -> Option<Job> {
        self.worker.lock().unwrap().pop()
    }

    /// Push a job to the executor queue.
    fn push(&self, job: Job) {
        self.worker.lock().unwrap().push(job);
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

/// A thread.
struct Thread {
    thread: Option<thread::JoinHandle<()>>,
}
impl Thread {
    /// Create a new thread.
    fn new<F>(core_id: usize, f: F, configuration: Arc<Configuration>) -> Thread
    where
        F: FnOnce() + Send + 'static,
    {
        // Get the pinning position
        let pinning_position = *configuration.get_thread_mapping().get(core_id).unwrap();
        // Create the thread and pin it if needed
        Thread {
            thread: Some(thread::spawn(move || {
                if configuration.get_pinning() {
                    let mut core_ids = core_affinity::get_core_ids().unwrap();
                    if core_ids.get(pinning_position).is_none() {
                        panic!("Cannot pin the thread in the choosen position.");
                    } else {
                        let core = core_ids.remove(pinning_position);
                        let err = core_affinity::set_for_current(core);
                        if !err {
                            error!("Thread pinning on core {} failed!", core.id);
                        } else {
                            trace!("Thread pinned on core {}.", core.id);
                        }
                    }
                }
                trace!("{:?} started", thread::current().id());
                (f)();
                trace!("{:?} now will end.", thread::current().id());
            })),
        }
    }

    /// Join the thread.
    fn join(&mut self) {
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

pub struct Partition {
    core_id: usize,
    workers: RwLock<Vec<Executor>>,
    total_workers: Arc<AtomicUsize>, // total number of workers in the partition
    available_workers: Arc<AtomicUsize>, // number of available workers in the partition
    global: Arc<Injector<Job>>,
    configuration: Arc<Configuration>,
}

impl Partition {
    /// Create a new partition.
    fn new(core_id: usize, configuration: Arc<Configuration>) -> Partition {
        let global = Arc::new(Injector::new());
        let workers = Vec::new();

        Partition {
            core_id,
            workers: RwLock::new(workers),
            total_workers: Arc::new(AtomicUsize::new(0)),
            available_workers: Arc::new(AtomicUsize::new(0)),
            global,
            configuration,
        }
    }

    /// Add a worker (executor) to the partition.
    /// The executor will be created and will execute the given closure.
    /// After the closure is executed, the executor will fetch other jobs from
    /// the global queue or its own queue.
    fn add_worker<F>(&self, f: F) -> JobInfo
    where
        F: FnOnce() + Send + 'static,
    {
        let mut workers = self.workers.write().unwrap();
        let worker = Executor::new(
            self.core_id,
            self.configuration.clone(),
            Arc::clone(&self.available_workers),
            self.global.clone(),
        );

        // Create a job info to track the job status.
        let job_info = JobInfo::new();
        let job_info_clone = Arc::clone(&job_info.status);
        // Create the job and push it to the executor.
        let job = Job::NewJob(Box::new(move || {
            f();
            job_info_clone.store(true, Ordering::Release);
        }));
        worker.push(job);

        // Push the new executor to the partition.
        workers.push(worker);

        // Update the number of executor in the partition.
        self.total_workers.fetch_add(1, Ordering::Release);

        job_info
    }

    /// Get the number of executor in the partition.
    fn get_worker_count(&self) -> usize {
        self.total_workers.load(Ordering::Acquire)
    }

    /// Get the number of busy executors (in this instant) in the partition.
    fn get_busy_worker_count(&self) -> usize {
        self.get_worker_count() - self.get_free_worker_count()
    }

    /// Get the number of free executors (in this instant) in the partition.
    fn get_free_worker_count(&self) -> usize {
        self.available_workers.load(Ordering::Acquire)
    }

    /// Create a new job from a function and push it to the partition.
    /// This method return a JobInfo that can be used to wait for the Job to finish.
    /// If there aren't executors in the partition or all the existing executors are busy, a new executor is created.
    /// Otherwise, the function is put in the global queue of the partition.
    fn push<F>(&self, f: F) -> JobInfo
    where
        F: FnOnce() + Send + 'static,
    {
        if self.get_free_worker_count() == 0 {
            return self.add_worker(f);
        }

        let job_info = JobInfo::new();
        let job_info_clone = Arc::clone(&job_info.status);

        let job = Job::NewJob(Box::new(move || {
            f();
            job_info_clone.store(true, Ordering::Release);
        }));

        self.global.push(job);

        job_info
    }
}

impl Drop for Partition {
    fn drop(&mut self) {
        trace!(
            "Dropping partition on core {}, total worker: {}.",
            self.core_id,
            self.get_worker_count()
        );

        // Terminate all the workers.
        self.global.push(Job::Terminate);

        let mut worker = self.workers.write().unwrap();

        // Join all the workers.
        for worker in worker.iter_mut() {
            worker.join();
        }
    }
}

#[derive(Debug)]
pub struct OrchestratorError {
    details: String,
}
impl OrchestratorError {
    fn new(msg: &str) -> OrchestratorError {
        OrchestratorError {
            details: msg.to_string(),
        }
    }
}
impl std::fmt::Display for OrchestratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.details)
    }
}
impl std::error::Error for OrchestratorError {
    fn description(&self) -> &str {
        &self.details
    }
}

pub struct Orchestrator {
    partitions: Vec<Partition>,
    configuration: Arc<Configuration>,
}

static mut ORCHESTRATOR: OnceCell<Arc<Orchestrator>> = OnceCell::new();

pub(crate) fn new_global_orchestrator(
    configuration: Arc<Configuration>,
) -> Result<Arc<Orchestrator>, OrchestratorError> {
    let orchestrator = Orchestrator::new(configuration);
    set_global_orchestrator(orchestrator)
}

pub(crate) fn new_default_orchestrator() -> Arc<Orchestrator> {
    Arc::new(Orchestrator::new(Arc::new(Configuration::new_default())))
}

pub fn get_global_orchestrator() -> Arc<Orchestrator> {
    unsafe {
        ORCHESTRATOR
            .get_or_init(|| -> Arc<Orchestrator> { new_default_orchestrator() })
            .clone()
    }
}
fn set_global_orchestrator(
    orchestrator: Orchestrator,
) -> Result<Arc<Orchestrator>, OrchestratorError> {
    unsafe {
        ORCHESTRATOR
            .set(Arc::new(orchestrator))
            .map_err(|_| OrchestratorError::new("Error setting global orchestrator"))?;
        Ok(get_global_orchestrator())
    }
}

impl Orchestrator {
    // Create a new orchestrator.
    fn new(configuration: Arc<Configuration>) -> Orchestrator {
        let mut partitions = Vec::new();
        let mut max_cores = 1;
        if configuration.get_pinning() {
            max_cores = configuration.get_max_cores();
        }

        for i in 0..max_cores {
            partitions.push(Partition::new(i, Arc::clone(&configuration)));
        }

        Orchestrator {
            partitions,
            configuration,
        }
    }

    /// Find the partition with the less busy executors.
    /// If there are more than one partition with the same number of executors, the first one is returned.
    /// If there are no executors in any partition, the first partition is returned.
    fn find_partition(&self) -> Option<&Partition> {
        if self.partitions.is_empty() {
            return None;
        } else if self.partitions.len() == 1 {
            return Some(self.partitions.first().unwrap());
        }
        let mut min = self.partitions.first();
        let mut min_busy = min.unwrap().get_busy_worker_count();
        for partition in self.partitions.iter() {
            let busy = partition.get_busy_worker_count();
            if busy == 0 {
                return Some(partition);
            }
            if busy < min_busy {
                min = Some(partition);
                min_busy = busy;
            }
        }
        min
    }

    /// Find a contiguos interval of 'count' partitions that minimize the number of busy executors contained in each partition of the interval.
    /// If there are more than one partition with the same number of executors, the first sequence found is returned.
    /// If there are no executors in any partition, the first sequence of 'count' partitions is returned.
    fn find_partition_sequence(&self, count: usize) -> Option<Vec<&Partition>> {
        if count > self.partitions.len() {
            return None;
        }

        let mut min = None;
        let mut min_busy = usize::MAX;
        for i in 0..self.partitions.len() - count {
            let mut busy = 0;
            for j in i..i + count {
                busy += self.partitions[j].get_busy_worker_count();
            }
            if busy == 0 {
                return Some(self.partitions[i..i + count].iter().collect());
            }
            if busy < min_busy {
                min = Some(self.partitions[i..i + count].iter().collect());
                min_busy = busy;
            }
        }
        min
    }

    /// Get the number of partitions of the orchestrator.
    pub fn get_partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Push a function into the orchestrator.
    /// This method return a JobInfo that can be used to wait for the Job to finish.
    /// If there aren't executors in the partitions of the orchestrator or all the existing executors are busy, a new executor is created.
    pub(crate) fn push<F>(&self, f: F) -> JobInfo
    where
        F: FnOnce() + Send + 'static,
    {
        let partition = self.find_partition();
        match partition {
            Some(p) => p.push(f),
            None => panic!("No partition found!"), // This should never happen.
        }
    }

    /// Push multiple functions into the orchestrator.
    /// This method return a vector of JobInfo that can be used to wait for the Jobs to finish.
    /// If the number of functions is lower than the number of partitions, this method will try to push all the functions in partitions that are contiguous in the vector.
    /// If the number of functions is greater than the number of partitions, this method will distribute evenly the functions in the sequence of partitions found.
    /// If there aren't executors in the partitions of the orchestrator or all the existing executors are busy, new executor are created.
    pub(crate) fn push_multiple<F>(&self, mut f: Vec<F>) -> Vec<JobInfo>
    where
        F: FnOnce() + Send + 'static,
    {
        let mut job_info = Vec::with_capacity(f.len());

        let partitions = self.find_partition_sequence(f.len());

        match partitions {
            Some(p) => {
                for partition in p {
                    let func = f.remove(0);
                    job_info.push(partition.push(move || {
                        func();
                    }));
                }
            }
            None => {
                for _i in 0..f.len() {
                    let func = f.remove(0);
                    job_info.push(self.push(move || {
                        func();
                    }));
                }
            }
        }
        job_info
    }

    pub(crate) fn get_configuration(&self) -> Arc<Configuration> {
        Arc::clone(&self.configuration)
    }

    pub fn delete_global_orchestrator() {
        unsafe {
            drop(ORCHESTRATOR.take());
        }
    }
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        while self.partitions.len() > 0 {
            drop(self.partitions.remove(0));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::{sync::Arc, thread::sleep, time::Duration};

    #[test]
    #[serial]
    fn test_orchestrator() {
        let configuration = Arc::new(Configuration::new_default());
        let orchestrator = Orchestrator::new(configuration);
        ::scopeguard::defer!({
            unsafe {
                drop(ORCHESTRATOR.take());
            }
        });

        let job_info = orchestrator.push(|| {
            println!("Hello from job!");
        });
        job_info.wait();
    }

    #[test]
    #[serial]
    /// Test that there is possible to set only one global orchestrator.
    /// This test is run in serial because it is testing a global state.
    fn test_global_orchestrator() {
        let configuration = Arc::new(Configuration::new_default());
        let orchestrator = Orchestrator::new(configuration);
        let configuration = Arc::new(Configuration::new_default());
        let orchestrator_clone = Orchestrator::new(configuration);
        ::scopeguard::defer!({
            unsafe {
                drop(ORCHESTRATOR.take());
            }
        });

        let result = set_global_orchestrator(orchestrator);
        assert!(result.is_ok());
        let result = set_global_orchestrator(orchestrator_clone);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    /// Test that all the jobs are executed and completed.
    /// This test is run in serial because it is testing a global state.
    fn test_global_orchestrator_jobs() {
        let configuration = Arc::new(Configuration::new_default());
        let orchestrator = Orchestrator::new(configuration);
        ::scopeguard::defer!({
            unsafe {
                drop(ORCHESTRATOR.take());
            }
        });

        let mut job_infos = Vec::new();
        for _ in 0..100 {
            let job_info = orchestrator.push(|| {
                sleep(Duration::from_millis(100));
                println!("Hello from job!");
            });
            job_infos.push(job_info);
        }
        for job_info in job_infos.iter() {
            job_info.wait();
        }
    }
}
