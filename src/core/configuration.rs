use std::env;

#[derive(Debug)]
pub struct ConfError {
    details: String,
}

impl ConfError {
    fn new(msg: &str) -> ConfError {
        ConfError {
            details: msg.to_string(),
        }
    }
}

impl std::fmt::Display for ConfError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl std::error::Error for ConfError {
    fn description(&self) -> &str {
        &self.details
    }
}

/// Global configuration.
pub struct Configuration {
    max_cores: usize,
    thread_mapping: Vec<usize>,
    pinning: bool,
    blocking_channel: bool,
}

/// Parse the core mapping from the environment variable PSPP_THREAD_MAPPING.
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

impl Configuration {
    pub fn new(max_cores: usize, pinning: bool, blocking_channel: bool) -> Configuration {
        let thread_mapping = parse_core_mapping();
        if thread_mapping.len() < max_cores {
            panic!("Error: Thread mapping length does not match max threads!");
        }
        Configuration {
            max_cores,
            thread_mapping,
            pinning,
            blocking_channel,
        }
    }

    pub fn new_default() -> Configuration {
        let max_threads = match env::var("PSPP_MAX_CORES") {
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

    /// Get the maximum number of cores allowed.
    pub(crate) fn get_max_cores(&self) -> usize {
        self.max_cores
    }

    /// Get the thread mapping.
    pub(crate) fn get_thread_mapping(&self) -> &Vec<usize> {
        &self.thread_mapping
    }

    /// Get the pinning flag.
    pub(crate) fn get_pinning(&self) -> bool {
        self.pinning
    }

    /// Get the blocking channel flag.
    pub(crate) fn get_blocking_channel(&self) -> bool {
        self.blocking_channel
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::serial_test::serial;

    fn reset_env() {
        env::remove_var("PSPP_MAX_CORES");
        env::remove_var("PSPP_PINNING");
        env::remove_var("PSPP_BLOCKING_CHANNEL");
        env::remove_var("PSPP_THREAD_MAPPING");
    }

    #[test]
    #[serial]
    fn test_configuration() {
        let conf = Configuration::new_default();
        assert_eq!(conf.max_cores, num_cpus::get());
        assert_eq!(conf.pinning, false);
        assert_eq!(conf.blocking_channel, false);
    }

    #[test]
    #[serial]
    fn test_configuration_with_env() {
        env::set_var("PSPP_MAX_CORES", "4");
        env::set_var("PSPP_PINNING", "true");
        env::set_var("PSPP_BLOCKING_CHANNEL", "true");
        let conf = Configuration::new_default();
        assert_eq!(conf.max_cores, 4);
        assert_eq!(conf.pinning, true);
        assert_eq!(conf.blocking_channel, true);
        reset_env();
    }

    #[test]
    #[serial]
    fn test_configuration_with_mapping() {
        env::set_var("PSPP_MAX_CORES", "4");
        env::set_var("PSPP_THREAD_MAPPING", "1,0,2,3");
        let conf = Configuration::new_default();
        assert_eq!(conf.max_cores, 4);
        assert_eq!(conf.thread_mapping, vec![1, 0, 2, 3]);
        reset_env();
    }
}
