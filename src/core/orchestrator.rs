use std::{
    cell::OnceCell,
    hint,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc
    },
    thread::{self},
};

use core_affinity::CoreId;
use crossbeam_deque::{Injector, Steal};
use log::{error, trace};
use parking_lot::Mutex;

use super::configuration::Configuration;

type Func<'a> = Box<dyn FnOnce() + Send + 'a>;

pub enum Job {
    NewJob(Func<'static>),
    Terminate,
}

/// Struct that reepresent a job in the queue.
/// It contains a method to wait till the job is done.
/// It is used to wait for the end of a job.
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
/// It will fetch jobs from the global queue of the partition.
struct Executor {
    thread: Thread,
}
impl Executor {
    /// Create a executor.
    /// It will start executing jobs immediately.
    /// It will terminate when it receives a terminate message.
    fn new(
        core_id: CoreId,
        config: Arc<Configuration>,
        available_workers: Arc<AtomicUsize>,
        global: Arc<Injector<Job>>,
    ) -> Executor {
        let worker = ExecutorInfo::new( available_workers, global);
        let thread = Thread::new(
            core_id,
            move || {
                worker.run();
            },
            config,
        );
        Executor {
            thread,
        }
    }

    /// Join the thread running the executor.
    fn join(&mut self) {
        self.thread.join();
    }
}

/// Information about the executor.
/// It contains the queue of jobs and the number of available executors in it's partition.
struct ExecutorInfo {
    available_workers: Arc<AtomicUsize>,
    global: Arc<Injector<Job>>,
}
impl ExecutorInfo {
    /// Create a new executor info.
    fn new(
        available_workers: Arc<AtomicUsize>,
        global: Arc<Injector<Job>>,
    ) -> ExecutorInfo {
        ExecutorInfo {
            available_workers,
            global,
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

    /// Run the executor.
    /// It will fetch jobs from the global queue.
    /// It will terminate when it receives a terminate message.
    fn run(&self) {
        loop {
            if let Some(job) = self.fetch_job() {
                match job {
                    Job::NewJob(f) => {
                        self.warn_busy();
                        f();
                        self.warn_available();
                    }
                    Job::Terminate => {
                       self.warn_busy();
                       break;
                    }
                }
            } else {
                thread::yield_now();
            }
        }
    }

    /// Fetch a job from the global queue.
    fn fetch_job(&self) -> Option<Job> {
        loop {
            match self.global.steal() {
                Steal::Success(job) => return Some(job),
                Steal::Empty => return None,
                Steal::Retry => continue,
            };
        }
    }
}

/// Struct that represent a thread.
/// It contains a thread and a method to join it.
struct Thread {
    thread: Option<thread::JoinHandle<()>>,
}
impl Thread {
    /// Create a new thread.
    /// If pinning is enabled, it will pin the thread to the specified core.
    /// If pinning is disabled, it will not pin the thread.
    fn new<F>(core_id: CoreId, f: F, configuration: Arc<Configuration>) -> Thread
    where
        F: FnOnce() + Send + 'static,
    {
        // Create the thread and pin it if needed
        Thread {
            thread: Some(thread::spawn(move || {
                if configuration.get_pinning() {
                   
                        let err = core_affinity::set_for_current(core_id);
                        if !err {
                            error!("Thread pinning on core {} failed!", core_id.id);
                        } else {
                            trace!("Thread pinned on core {}.", core_id.id);
                        }
                    
                }
                trace!("{:?} started", thread::current().id());
                (f)();
                trace!("{:?} now will end.", thread::current().id());
            })),
        }
    }

    /// Join the thread.
    /// It will wait until the thread is terminated.
    fn join(&mut self) {
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

/// Struct that represent a partition.
/// It contains the list of workers (executors) and the number of available workers.
/// Executors from the same partition share the same global queue.
/// A partition, if pinning is enabled, will be pinned to a specific core.
/// Basically a partition represent a core, the executors are the threads pinned on that core.
pub struct Partition {
    core_id: CoreId,
    workers: Mutex<Vec<Executor>>,
    total_workers: Arc<AtomicUsize>, // total number of workers in the partition
    available_workers: Arc<AtomicUsize>, // number of available workers in the partition
    global: Arc<Injector<Job>>,
    configuration: Arc<Configuration>,
}

impl Partition {
    /// Create a new partition.
    /// It will create the global queue and the list of workers (executors).
    /// If pinning is enabled, it will pin the partition to the specified core.
    fn new(core_id: usize, configuration: Arc<Configuration>) -> Partition {
        let global = Arc::new(Injector::new());
        let workers = Vec::new();
        let core_id = configuration.get_thread_mapping()[core_id];
        Partition {
            core_id,
            workers: Mutex::new(workers),
            total_workers: Arc::new(AtomicUsize::new(0)),
            available_workers: Arc::new(AtomicUsize::new(0)),
            global,
            configuration,
        }
    }

    /// Add a worker (executor) to the partition.
    /// The executor will be created and pushed to the partition.
    fn add_worker(&self)
    {
        let worker = Executor::new(
            self.core_id,
            self.configuration.clone(),
            Arc::clone(&self.available_workers),
            self.global.clone(),
        );

        // Take lock and push the new executor to the partition.
        let mut workers = self.workers.lock();
        workers.push(worker);

        // Update the number of executor in the partition.
        self.total_workers.fetch_add(1, Ordering::Release);
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

    /// Push a new job to the partition.
    /// This method return a JobInfo that can be used to wait for the Job to finish.
    /// If there aren't executors in the partition or all the existing executors are busy, a new executor is created.
    fn push<F>(&self, f: F) -> JobInfo
    where
        F: FnOnce() + Send + 'static,
    {
        if self.get_free_worker_count() == 0 {
            self.add_worker();
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
    /// Drop the partition.
    /// It will terminate all the executors and join them.
    fn drop(&mut self) {
        trace!(
            "Dropping partition on core {}, total worker: {}.",
            self.core_id.id,
            self.get_worker_count()
        );

        // Push terminate messages to the global queue.
        // This will terminate all the executors.
        for _ in 0..self.get_worker_count() {
            self.global.push(Job::Terminate);
        }

        let mut workers = self.workers.lock();

        // Join all the workers.
        for worker in workers.iter_mut() {
            worker.join();
        }
    }
}

/* 
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
*/

/// The orchestrator is the main structure of the library.
/// Is composed by a list of partitions, each partition, if pinning is enabled, is a core.
/// The orchestrator is responsible to create the partitions and to distribute the jobs to the partitions.
/// The orchestractor is global, implemented as a singleton.
/// The main idea is to have a central point that distribuite evenly the jobs to the partitions, exploiting
/// the numa architecture of the system.
pub struct Orchestrator {
    partitions: Vec<Partition>,
    configuration: Arc<Configuration>,
}

/// OnceCell is a structure that allow to create a safe global singleton.
static mut ORCHESTRATOR: OnceCell<Arc<Orchestrator>> = OnceCell::new();

/* 
pub(crate) fn new_global_orchestrator(
    configuration: Arc<Configuration>,
) -> Result<Arc<Orchestrator>, OrchestratorError> {
    let orchestrator = Orchestrator::new(configuration);
    set_global_orchestrator(orchestrator)
}
}
*/

/// Create a new orchestrator with the default configuration.
pub(crate) fn new_default_orchestrator() -> Arc<Orchestrator> {
    Arc::new(Orchestrator::new(Arc::new(Configuration::new_default())))
}

/// Get or initialize the global orchestrator.
/// If the global orchestrator is not initialized, a new one is created with the default configuration.
pub fn get_global_orchestrator() -> Arc<Orchestrator> {
    unsafe {
        ORCHESTRATOR
            .get_or_init(|| -> Arc<Orchestrator> { new_default_orchestrator() })
            .clone()
    }
}

/* 
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
*/

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
        while !self.partitions.is_empty() {
            drop(self.partitions.remove(0));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::{sync::Arc};

    #[test]
    #[serial]
    fn test_orchestrator() {
        let configuration = Arc::new(Configuration::new_default());
        let orchestrator = Orchestrator::new(Arc::clone(&configuration));
        let counter = Arc::new(AtomicUsize::new(0));

        let mut jobs_info = Vec::with_capacity(1000);

        (0..1000).for_each(|_| {
            let counter_clone = counter.clone();
            let job_info = orchestrator.push(move || {
                counter_clone.fetch_add(1, Ordering::AcqRel);
            });
            jobs_info.push(job_info);
        });

        for job_info in jobs_info {
            job_info.wait();
        }

        assert_eq!(counter.load(Ordering::Acquire), 1000);
    }

    #[test]
    #[serial]
    fn test_global_orchestrator() {
        let orchestrator = get_global_orchestrator();
        let counter = Arc::new(AtomicUsize::new(0));

        let mut jobs_info = Vec::with_capacity(1000);

        (0..1000).for_each(|_| {
            let counter_clone = counter.clone();
            let job_info = orchestrator.push(move || {
                counter_clone.fetch_add(1, Ordering::AcqRel);
            });
            jobs_info.push(job_info);
        });

        for job_info in jobs_info {
            job_info.wait();
        }

        assert_eq!(counter.load(Ordering::Acquire), 1000);
        Orchestrator::delete_global_orchestrator();
    }

}
