use std::{
    hint,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex, OnceLock,
    },
    thread::{self},
};

use core_affinity::CoreId;
use log::{debug, error, trace, warn};

use super::configuration::Configuration;

type Func<'a> = Box<dyn FnOnce() + Send + 'a>;

/// Mini Spin Lock
///
/// This can be used to protect critical sections of the code
struct Lock {
    lock: AtomicBool,
}
impl Lock {
    fn new() -> Self {
        Lock {
            lock: AtomicBool::new(false),
        }
    }

    fn lock(&self) {
        while self
            .lock
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            hint::spin_loop()
        }
    }

    fn unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }
}
impl Drop for Lock {
    fn drop(&mut self) {
        self.unlock();
    }
}
/// Enum representing a Job in the orchestrator.
pub enum Job {
    /// A New job
    NewJob(Func<'static>),
    /// Terminate the executor
    Terminate,
}

/// Struct that reepresent a job in the queue.
///
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
        while !self.status.load(Ordering::Relaxed) {
            hint::spin_loop();
        }
    }
}

/// An executor.
/// It is a thread that execute jobs.
/// It will fetch jobs from it's local queue.
struct Executor {
    thread: Thread,
    global_lock: Arc<Lock>,
    status: Arc<AtomicBool>,
    available_workers: Arc<AtomicUsize>,
    queue: Arc<Mutex<Vec<Job>>>,
    cvar: Arc<Condvar>,
}
impl Executor {
    /// Create a executor.
    /// It will start executing jobs immediately.
    /// It will terminate when it receives a terminate message.
    fn new(
        core_id: CoreId,
        config: Arc<Configuration>,
        global_lock: Arc<Lock>,
        available_workers: Arc<AtomicUsize>,
    ) -> Executor {
        let status = Arc::new(AtomicBool::new(false));
        let cvar = Arc::new(Condvar::new());
        let queue = Arc::new(Mutex::new(Vec::with_capacity(1)));

        let worker = ExecutorInfo::new(
            status.clone(),
            global_lock.clone(),
            available_workers.clone(),
            queue.clone(),
            cvar.clone(),
        );
        let thread = Thread::new(
            core_id,
            move || {
                worker.run();
            },
            config,
        );
        Executor {
            thread,
            global_lock,
            status,
            available_workers,
            queue,
            cvar,
        }
    }

    /// Get the status of this executor
    fn get_status(&self) -> bool {
        self.global_lock.lock();

        let res = self.status.load(Ordering::Relaxed);

        self.global_lock.unlock();

        res
    }

    /// Push a job in the executor queue
    fn push(&self, job: Job) {
        self.warn_busy();
        let mut queue = self.queue.lock().unwrap();
        assert!(queue.is_empty());
        queue.push(job);
        self.cvar.notify_one();
    }

    // Warn that the executor is busy.
    fn warn_busy(&self) {
        self.global_lock.lock();

        self.status.store(false, Ordering::Relaxed);
        let _ = self.available_workers.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |x| -> Option<usize> { Some(x.saturating_sub(1)) },
        );

        self.global_lock.unlock();
    }

    /// Join the thread running the executor.
    fn join(&mut self) {
        self.thread.join();
    }
}

/// Information about the executor.
/// It contains the queue of jobs and the number of available executors in it's partition.
struct ExecutorInfo {
    status: Arc<AtomicBool>,
    global_lock: Arc<Lock>,
    available_workers: Arc<AtomicUsize>,
    queue: Arc<Mutex<Vec<Job>>>,
    cvar: Arc<Condvar>,
}
impl ExecutorInfo {
    /// Create a new executor info.
    fn new(
        status: Arc<AtomicBool>,
        global_lock: Arc<Lock>,
        available_workers: Arc<AtomicUsize>,
        queue: Arc<Mutex<Vec<Job>>>,
        cvar: Arc<Condvar>,
    ) -> ExecutorInfo {
        ExecutorInfo {
            status,
            global_lock,
            available_workers,
            queue,
            cvar,
        }
    }

    // Warn that the executor is available.
    fn warn_available(&self) {
        self.global_lock.lock();

        self.status.store(true, Ordering::Relaxed);
        self.available_workers.fetch_add(1, Ordering::Relaxed);

        self.global_lock.unlock();
    }

    /// Run the executor.
    /// It will fetch jobs from the local queue.
    /// It will terminate when it receives a terminate message.
    fn run(&self) {
        loop {
            if let Some(job) = self.fetch_job() {
                match job {
                    Job::NewJob(f) => {
                        f();
                        self.warn_available();
                    }
                    Job::Terminate => {
                        break;
                    }
                }
            }
        }
    }

    /// Fetch a job from the local queue.
    fn fetch_job(&self) -> Option<Job> {
        let mut queue = self.queue.lock().unwrap();
        let mut job = queue.pop();

        while job.is_none() {
            queue = self.cvar.wait(queue).unwrap();
            job = queue.pop();
        }
        job
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
/// A partition, if pinning is enabled, will be pinned to a specific core.
/// Basically a partition represent a core, the executors are the threads pinned on that core.
pub struct Partition {
    core_id: CoreId,
    workers: Mutex<Vec<Executor>>,
    global_lock: Arc<Lock>,
    available_workers: Arc<AtomicUsize>,
    configuration: Arc<Configuration>,
}

impl Partition {
    /// Create a new partition.
    /// It will create the list of workers (executors).
    /// If pinning is enabled, it will pin the partition to the specified core.
    fn new(core_id: usize, configuration: Arc<Configuration>) -> Partition {
        let workers = Vec::new();
        let core_id = configuration.get_thread_mapping()[core_id];
        Partition {
            core_id,
            workers: Mutex::new(workers),
            global_lock: Arc::new(Lock::new()),
            available_workers: Arc::new(AtomicUsize::new(0)),
            configuration,
        }
    }

    /// Get the number of executor in the partition.
    fn get_worker_count(&self) -> usize {
        self.workers.lock().unwrap().len()
    }

    /// Get the number of busy executors (in this instant) in the partition.
    fn get_busy_worker_count(&self) -> usize {
        self.get_worker_count() - self.get_free_worker_count()
    }

    /// Get the number of free executors (in this instant) in the partition.
    fn get_free_worker_count(&self) -> usize {
        self.global_lock.lock();

        let res = self.available_workers.load(Ordering::Acquire);

        self.global_lock.unlock();

        res
    }

    /// Find executor
    fn find_executor(workers: &mut Vec<Executor>) -> Option<Executor> {
        for i in 0..workers.len() {
            if workers[i].get_status() {
                return Some(workers.remove(i));
            }
        }
        None
    }
    /// Push a new job to the partition.
    /// This method return a JobInfo that can be used to wait for the Job to finish.
    /// If there aren't executors in the partition or all the existing executors are busy, a new executor is created.
    fn push<F>(&self, f: F) -> JobInfo
    where
        F: FnOnce() + Send + 'static,
    {
        let job_info = JobInfo::new();
        let job_info_clone = Arc::clone(&job_info.status);

        let job = Job::NewJob(Box::new(move || {
            f();
            job_info_clone.store(true, Ordering::Relaxed);
        }));

        let mut workers = self.workers.lock().unwrap();
        let worker = Self::find_executor(&mut workers);
        match worker {
            Some(executor) => {
                executor.push(job);
                workers.push(executor);
            }
            None => {
                let executor = Executor::new(
                    self.core_id,
                    self.configuration.clone(),
                    self.global_lock.clone(),
                    self.available_workers.clone(),
                );
                executor.push(job);
                workers.push(executor);
            }
        }

        job_info
    }
}

impl Drop for Partition {
    /// Drop the partition.
    /// It will terminate all the executors and join them.
    fn drop(&mut self) {
        let mut workers = self.workers.lock().unwrap();

        // Join all the workers.
        for worker in workers.iter_mut() {
            worker.push(Job::Terminate);
        }

        for worker in workers.iter_mut() {
            worker.join();
        }
    }
}

/// The orchestrator is the main structure of the library.
/// Is composed by a list of partitions, each partition, if pinning is enabled, is a core.
/// The orchestrator is responsible to create the partitions and to distribute the jobs to the partitions.
/// The orchestractor is global, implemented as a singleton.
/// The main idea is to have a central point that distribuite evenly the jobs to the partitions, exploiting
/// the CPU topology of the system.
pub struct Orchestrator {
    partitions: Vec<Partition>,
    configuration: Arc<Configuration>,
}

/// OnceLock is a structure that allow to create a safe global singleton.
static mut ORCHESTRATOR: OnceLock<Arc<Orchestrator>> = OnceLock::new();

/// Create a new orchestrator with the default configuration.
pub(crate) fn new() -> Arc<Orchestrator> {
    Arc::new(Orchestrator::new(Arc::new(Configuration::new_default())))
}

/// Get or initialize the global orchestrator.
/// If the global orchestrator is not initialized, a new one is created with the default configuration.
pub fn get_global_orchestrator() -> Arc<Orchestrator> {
    unsafe {
        ORCHESTRATOR
            .get_or_init(|| -> Arc<Orchestrator> { new() })
            .clone()
    }
}

impl Orchestrator {
    // Create a new orchestrator.
    fn new(configuration: Arc<Configuration>) -> Orchestrator {
        let mut partitions = Vec::new();
        let mut max_cores = 1;
        if configuration.get_pinning() {
            max_cores = configuration.get_max_cores();
            if cfg!(target_os = "macos") {
                warn!("Thread pinning is not currently supported for Apple Silicon.");
            }
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
    fn find_partition(partitions: &[Partition]) -> Option<&Partition> {
        partitions.iter().min_by_key(|p| p.get_busy_worker_count())
    }

    /// Find the subarray of size count of partitions that minimize the number of busy executors contained in each partition of the subarray.
    /// If there are more than one sequence with the same number of busy executors, the first sequence found is returned.
    /// This algorithm will use the sum of the previous subarray, removing the first element, to compute the sum of the next subarray.
    /// This will reduce the complexity from O(n^2) to O(n).
    /// If there are no executors in any partition, the first sequence of 'count' partitions is returned.
    /// This method is used to find the best sequence of partitions to pin a set of jobs.
    fn find_partitions_sequence(partitions: &[Partition], count: usize) -> Option<&[Partition]> {
        if count > partitions.len() {
            return None;
        }

        let mut min = None;
        let mut min_busy = usize::MAX;
        let mut busy = partitions
            .iter()
            .take(count)
            .map(|p| p.get_busy_worker_count())
            .sum();

        if busy == 0 {
            return Some(&partitions[0..count]);
        }

        if busy < min_busy {
            min = Some(&partitions[0..count]);
            min_busy = busy;
        }

        for i in count..partitions.len() {
            busy -= partitions[i - count].get_busy_worker_count();
            busy += partitions[i].get_busy_worker_count();

            if busy == 0 {
                return Some(&partitions[i - count + 1..=i]);
            }

            if busy < min_busy {
                min = Some(&partitions[i - count + 1..=i]);
                min_busy = busy;
            }
        }

        min
    }
    /// Push a function into the orchestrator.
    /// This method return a JobInfo that can be used to wait for the Job to finish.
    /// If there aren't executors in the partitions of the orchestrator or all the existing executors are busy, a new executor is created.
    fn push_single<F>(partitions: &[Partition], f: F) -> JobInfo
    where
        F: FnOnce() + Send + 'static,
    {
        let partition = Self::find_partition(partitions);
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
    pub(crate) fn push_jobs<F>(&self, mut f: Vec<F>) -> Vec<JobInfo>
    where
        F: FnOnce() + Send + 'static,
    {
        let mut job_info = Vec::with_capacity(f.len());

        let mut partitions = None;

        if f.len() > 1 {
            partitions = Self::find_partitions_sequence(&self.partitions, f.len());
        }
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
                    job_info.push(Self::push_single(&self.partitions, move || {
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

    /// Delete the global orchestrator.
    ///
    /// # Safety
    /// This method should be used only when writing tests or benchmarks, or in
    /// the few cases where we want destroy the global orchestrator long before the end
    /// the application.
    pub unsafe fn delete_global_orchestrator() {
        unsafe {
            drop(ORCHESTRATOR.take());
        }
    }
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        while !self.partitions.is_empty() {
            let partition = self.partitions.remove(0);
            debug!(
                "Total worker for Partition[{}]: {}",
                partition.core_id.id,
                partition.get_worker_count()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::{atomic::AtomicUsize, Arc};

    #[test]
    #[serial]
    fn test_orchestrator() {
        let configuration = Arc::new(Configuration::new_default());
        let orchestrator = Orchestrator::new(Arc::clone(&configuration));
        let counter = Arc::new(AtomicUsize::new(0));

        let mut jobs_info = Vec::with_capacity(1000);

        (0..1000).for_each(|_| {
            let counter_clone = counter.clone();
            let mut job_info = orchestrator.push_jobs(vec![move || {
                counter_clone.fetch_add(1, Ordering::AcqRel);
            }]);
            jobs_info.push(job_info.remove(0));
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
            let mut job_info = orchestrator.push_jobs(vec![move || {
                counter_clone.fetch_add(1, Ordering::AcqRel);
            }]);
            jobs_info.push(job_info.remove(0));
        });

        for job_info in jobs_info {
            job_info.wait();
        }

        assert_eq!(counter.load(Ordering::Acquire), 1000);
        unsafe {
            Orchestrator::delete_global_orchestrator();
        }
    }
}
