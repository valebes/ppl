use log::error;
use log::info;
use log::trace;
use std::error::Error;
use std::fmt;
use std::thread;
extern crate core_affinity;

#[derive(Debug)]
pub struct ThreadError {
    details: String,
}

impl ThreadError {
    fn new(msg: &str) -> ThreadError {
        ThreadError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for ThreadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for ThreadError {
    fn description(&self) -> &str {
        &self.details
    }
}

pub struct Thread {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
    job: Option<Box<dyn FnOnce() + Send + Sync + 'static>>,
    pin: bool,
}

impl Thread {
    pub fn new<F>(id: usize, f: F, pinning: bool) -> Thread
    where
        F: FnOnce() + Send + Sync + 'static,
    {
        Thread {
            id,
            thread: None,
            job: Some(Box::new(f)),
            pin: pinning,
        }
    }

    pub fn start(&mut self) -> std::result::Result<(), ThreadError> {
        if self.job.is_none() {
            return Err(ThreadError::new("Thread already started."));
        }

        let f = std::mem::replace(&mut self.job, None).unwrap();

        let id = self.get_id();
        let pinned = self.is_pinned();

        // Before start the rts method, if we enabled pinning, we pin the thread.
        // If we cant pin the thread, maybe because the cpu have less threads than required,
        // we procede without pinning the thread.
        self.thread = Some(thread::spawn(move || {
            if pinned {
                let mut core_ids = core_affinity::get_core_ids().unwrap();
                if core_ids.get(id).is_none() {
                    error!("Cannot pin the thread in the choosen position.");
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
            info!("{:?} started", thread::current().id());
            (f)();
            info!("{:?} now will end.", thread::current().id());
        }));
        Ok(())
    }

    pub fn wait(&mut self) -> std::result::Result<(), ThreadError> {
        let handler = self.thread.take();
        match handler {
            Some(thread) => {
                let err = thread.join();
                match err {
                    Ok(_) => Ok(()),
                    Err(_) => Err(ThreadError::new("Failed joining thread.")),
                }
            }
            None => Err(ThreadError::new("Thread already ended.")),
        }
    }

    #[allow(dead_code)]
    pub fn is_started(&self) -> bool {
        self.job.is_none()
    }

    #[allow(dead_code)]
    pub fn is_ended(&self) -> bool {
        self.thread.is_none()
    }

    pub fn is_pinned(&self) -> bool {
        self.pin
    }

    pub fn get_id(&self) -> usize {
        self.id
    }
}
