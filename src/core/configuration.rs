use std::sync::{Once, Arc};
use std::env;

static mut CONF: Option<Arc<Configuration>> = None;
static CONF_INIT: Once = Once::new();

pub struct Configuration {
    max_threads: usize,
    thread_mapping: Vec<usize>,
    pinning: bool,
    blocking_channel: bool,
}
fn parse_core_mapping() -> Vec<usize> {
    let mut thread_mapping = Vec::new();
    match env::var("PSPP_THREAD_MAPPING") {
        Ok(val) => {
            let mapping: Vec<&str> = val.split(",").collect();
            for i in 0..mapping.len() {
                thread_mapping.push(mapping[i].parse::<usize>().unwrap());
            }
        }
        Err(_) => {
            for i in 0..num_cpus::get() {
                thread_mapping.push(i);
            }
        }
    }
    thread_mapping
}
pub(crate) fn set_configuration(conf: Arc<Configuration>) {
  CONF_INIT.call_once(|| {
    unsafe {
      CONF = Some(conf);
    }
  });
}
impl Configuration {
    fn new(max_threads: usize, pinning: bool, blocking_channel: bool) -> Arc<Configuration> {
        match unsafe { CONF.as_ref() } {
            Some(_) => panic!("Error: Configuration already set!"),
            None => {
                let thread_mapping = parse_core_mapping();
                if thread_mapping.len() != max_threads {
                    panic!("Error: Thread mapping length does not match max threads!");
                }
                let conf = Arc::new(Configuration {
                    max_threads,
                    thread_mapping,
                    pinning,
                    blocking_channel,
                });
                set_configuration(conf.clone());
                conf
            }
        }
    }

    fn new_with_default() -> Arc<Configuration> {
        let max_threads = num_cpus::get();
        let pinning = false;
        let blocking_channel = false;
        Configuration::new(max_threads, pinning, blocking_channel)
    }

    fn new_from_env() -> Arc<Configuration> {
        let max_threads = match env::var("PSPP_MAX_THREADS") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_) => num_cpus::get(),
        };
        let pinning = match env::var("PSPP_PINNING") {
            Ok(val) => val.parse::<bool>().unwrap(),
            Err(_) => false,
        };
        let blocking_channel = match env::var("PSPP_BLOCKING_CHANNEL") {
            Ok(val) => val.parse::<bool>().unwrap(),
            Err(_) => false,
        };
        Configuration::new(max_threads, pinning, blocking_channel)
    }

    pub(crate) fn get_configuration() -> Arc<Configuration> {
        match unsafe { CONF.as_ref() } {
            Some(conf) => conf.clone(),
            None => Configuration::new_from_env(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_configuration() {
        let conf = Configuration::new_from_env();
        assert_eq!(conf.max_threads, num_cpus::get());
        assert_eq!(conf.pinning, false);
        assert_eq!(conf.blocking_channel, false);
    }
    fn test_configuration_with_env() {
        env::set_var("PSPP_MAX_THREADS", "4");
        env::set_var("PSPP_PINNING", "true");
        env::set_var("PSPP_BLOCKING_CHANNEL", "true");
        let conf = Configuration::new_from_env();
        assert_eq!(conf.max_threads, 4);
        assert_eq!(conf.pinning, true);
        assert_eq!(conf.blocking_channel, true);
    }
    fn test_configuration_with_mapping() {
        env::set_var("PSPP_MAX_THREADS", "4");
        env::set_var("PSPP_THREAD_MAPPING", "1,0,2,3");
        let conf = Configuration::new_from_env();
        assert_eq!(conf.max_threads, 4);
        assert_eq!(conf.pinning, false);
        assert_eq!(conf.blocking_channel, false);
        assert_eq!(conf.thread_mapping, vec![1, 0, 2, 3]);
    }

}