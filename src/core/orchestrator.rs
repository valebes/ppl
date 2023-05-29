use std::{
    cell::OnceCell,
    collections::VecDeque,
    hint,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{self},
};

use core_affinity::CoreId;
use log::{error, trace};

use super::configuration::Configuration;

type Func<'a> = Box<dyn FnOnce() + Send + 'a>;

pub enum Job {
    NewJob(Func<'static>),
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
    status: Arc<AtomicBool>,
    queue: Arc<Mutex<VecDeque<Job>>>,
    cvar: Arc<Condvar>,
}
impl Executor {
    /// Create a executor.
    /// It will start executing jobs immediately.
    /// It will terminate when it receives a terminate message.
    fn new(core_id: CoreId, config: Arc<Configuration>) -> Executor {
        let status = Arc::new(AtomicBool::new(false));
        let cvar = Arc::new(Condvar::new());
        let queue = Arc::new(Mutex::new(VecDeque::new()));

        let worker = ExecutorInfo::new(status.clone(), queue.clone(), cvar.clone());
        let thread = Thread::new(
            core_id,
            move || {
                worker.run();
            },
            config,
        );
        Executor {
            thread,
            status,
            queue,
            cvar,
        }
    }

    /// Get the status of this executor
    fn get_status(&self) -> bool {
        self.status.load(Ordering::Acquire)
    }

    /// Push a job in the executor queue
    fn push(&self, job: Job) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(job);
        self.cvar.notify_one();
    }

    fn is_empty(&self) -> bool {
        self.queue.lock().unwrap().is_empty()
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
    queue: Arc<Mutex<VecDeque<Job>>>,
    cvar: Arc<Condvar>,
}
impl ExecutorInfo {
    /// Create a new executor info.
    fn new(
        status: Arc<AtomicBool>,
        queue: Arc<Mutex<VecDeque<Job>>>,
        cvar: Arc<Condvar>,
    ) -> ExecutorInfo {
        ExecutorInfo {
            status,
            queue,
            cvar,
        }
    }

    // Warn that the executor is available.
    fn warn_available(&self) {
        self.status.store(true, Ordering::Release)
    }

    // Warn that the executor is busy.
    fn warn_busy(&self) {
        self.status.store(false, Ordering::Release)
    }

    /// Run the executor.
    /// It will fetch jobs from the global queue.
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

    /// Fetch a job from the global queue.
    fn fetch_job(&self) -> Option<Job> {
        let mut queue = self.queue.lock().unwrap();
        let mut job = queue.pop_front();

        while job.is_none() {
            queue = self.cvar.wait(queue).unwrap();
            job = queue.pop_front();
        }
        self.warn_busy();
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
/// Executors from the same partition share the same global queue.
/// A partition, if pinning is enabled, will be pinned to a specific core.
/// Basically a partition represent a core, the executors are the threads pinned on that core.
pub struct Partition {
    core_id: CoreId,
    workers: Mutex<Vec<Executor>>,
    configuration: Arc<Configuration>,
}

impl Partition {
    /// Create a new partition.
    /// It will create the global queue and the list of workers (executors).
    /// If pinning is enabled, it will pin the partition to the specified core.
    fn new(core_id: usize, configuration: Arc<Configuration>) -> Partition {
        let workers = Vec::new();
        let core_id = configuration.get_threads_mapping()[core_id];
        Partition {
            core_id,
            workers: Mutex::new(workers),
            configuration,
        }
    }

    /// Get the number of executor in the partition.
    fn get_worker_count(workers: &Vec<Executor>) -> usize {
        workers.len()
    }

    /// Get the number of busy executors (in this instant) in the partition.
    fn get_busy_worker_count(workers: &Vec<Executor>) -> usize {
        Self::get_worker_count(workers) - Self::get_free_worker_count(workers)
    }

    /// Get the number of free executors (in this instant) in the partition.
    fn get_free_worker_count(workers: &[Executor]) -> usize {
        let mut free_workers = 0;
        workers.iter().for_each(|worker| {
            if worker.get_status() && worker.is_empty() {
                free_workers += 1;
            }
        });
        free_workers
    }

    /// Find executor
    fn find_executor(workers: &mut Vec<Executor>) -> Option<Executor> {
        for i in 0..workers.len() {
            if workers[i].get_status() && workers[i].is_empty() {
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
            job_info_clone.store(true, Ordering::Release);
        }));

        let mut workers = self.workers.lock().unwrap();
        let worker = Self::find_executor(&mut workers);
        match worker {
            Some(executor) => {
                error!("Using existing worker");
                executor.push(job);
                workers.push(executor);
            }
            None => {
                error!("adding worker");
                let executor = Executor::new(self.core_id, self.configuration.clone());
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
/// The orchestrator is responsible to create the partitions and to dis;tribute the jobs to the partitions.
/// The orchestractor is global, implemented as a singleton.
/// The main idea is to have a central point that distribuite evenly the jobs to the partitions, exploiting
/// the numa architecture of the system.
pub struct Orchestrator {
    partitions: Mutex<Vec<Partition>>,
    configuration: Arc<Configuration>,
}

/// OnceCell is a structure that allow to create a safe global singleton.
static mut ORCHESTRATOR: OnceCell<Arc<Orchestrator>> = OnceCell::new();

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
            partitions: Mutex::new(partitions),
            configuration,
        }
    }

    /// Find the partition with the less busy executors.
    /// If there are more than one partition with the same number of executors, the first one is returned.
    /// If there are no executors in any partition, the first partition is returned.
    fn find_partition(partitions: &Vec<Partition>) -> Option<&Partition> {
        if partitions.is_empty() {
            return None;
        } else if partitions.len() == 1 {
            return Some(partitions.first().unwrap());
        }
        let mut min = partitions.first();
        let mut min_busy = Partition::get_busy_worker_count(&min.unwrap().workers.lock().unwrap());
        for partition in partitions.iter() {
            let busy = Partition::get_busy_worker_count(&partition.workers.lock().unwrap());
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

    /// Find the subarray of size count of partition that minimize the number of busy executors contained in each partition of the subarray.
    /// If there are more than one sequence with the same number of busy executors, the first sequence found is returned.
    /// This algorithm will use the sum of the previous subarray, removing the first element, to compute the sum of the next subarray.
    /// This will reduce the complexity from O(n^2) to O(n).
    /// If there are no executors in any partition, the first sequence of 'count' partitions is returned.
    /// This method is used to find the best sequence of partitions to pin a set jobs.
    fn find_partition_sequence(
        partitions: &mut Vec<Partition>,
        count: usize,
    ) -> Option<Vec<&Partition>> {
        if count > partitions.len() {
            return None;
        } 
        
        let mut min = None;
        let mut min_busy = usize::MAX;
        let mut busy = 0;
        (0..count).for_each(|i| {
            busy += Partition::get_busy_worker_count(&partitions[i].workers.lock().unwrap());
        });
        if busy == 0 {
            return Some(partitions[0..count].iter().collect());
        }
        if busy < min_busy {
            min = Some(partitions[0..count].iter().collect());
            min_busy = busy;
        }
        for i in count..partitions.len() {
            busy -=
                Partition::get_busy_worker_count(&partitions[i - count].workers.lock().unwrap());
            busy += Partition::get_busy_worker_count(&partitions[i].workers.lock().unwrap());
            if busy == 0 {
                return Some(partitions[i - count + 1..i + 1].iter().collect());
            }
            if busy < min_busy {
                min = Some(partitions[i - count + 1..i + 1].iter().collect());
                min_busy = busy;
            }
        }
        min
    }

    /// Push a function into the orchestrator.
    /// This method return a JobInfo that can be used to wait for the Job to finish.
    /// If there aren't executors in the partitions of the orchestrator or all the existing executors are busy, a new executor is created.
    fn push_single<F>(partitions: &mut Vec<Partition>, f: F) -> JobInfo
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
    pub(crate) fn push_multiple<F>(&self, mut f: Vec<F>) -> Vec<JobInfo>
    where
        F: FnOnce() + Send + 'static,
    {
        let mut lock = self.partitions.lock().unwrap();

        let mut job_info = Vec::with_capacity(f.len());

        let partitions;

        if let 1 = f.len() {
           match Self::find_partition(&lock) {
            Some(e) => partitions = Some(vec![e]),
            None => partitions = None,
        }
        } else {
            partitions = Self::find_partition_sequence(&mut lock, f.len());
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
                    job_info.push(Self::push_single(&mut lock, move || {
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
        let mut lock = self.partitions.lock().unwrap();
        while !lock.is_empty() {
            drop(lock.remove(0));
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
            let mut job_info = orchestrator.push_multiple(vec![move || {
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
            let mut job_info = orchestrator.push_multiple(vec![move || {
                counter_clone.fetch_add(1, Ordering::AcqRel);
            }]);
            jobs_info.push(job_info.remove(0));
        });

        for job_info in jobs_info {
            job_info.wait();
        }

        assert_eq!(counter.load(Ordering::Acquire), 1000);
        Orchestrator::delete_global_orchestrator();
    }
}
