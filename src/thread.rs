use log::info;
use std::error::Error;
use std::fmt;
use std::thread;

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
        F: FnOnce() -> () + Send + Sync + 'static,
    {
        Thread {
            id: id,
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

        self.thread = Some(thread::spawn(move || {
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

    pub fn is_started(&self) -> bool {
        match &self.job {
            Some(_) => false,
            None => true,
        }
    }

    pub fn is_ended(&self) -> bool {
        match &self.thread {
            Some(_) => false,
            None => true,
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pin
    }

    pub fn get_id(&self) -> usize {
        self.id
    }
}
