use std::env;

use core_affinity::CoreId;

/// Configuration of the framework.
///
/// The configuration is set by the user through environment variables.
/// The environment variables are:
/// - PPL_MAX_CORES: maximum number of cores allowed. Only valid when pinning is active.
/// - PPL_PINNING: enable pinning of the partitions to the cores. Default is false.
/// - PPL_SCHEDULE: scheduling method used in the pipeline. Default is static.
/// - PPL_WAIT_POLICY: enable blocking channel. Default is false.
/// - PPL_THREAD_MAPPING: core mapping. Default is the order in which the cores are found.
pub struct Configuration {
    max_cores: usize,
    thread_mapping: Vec<CoreId>,
    pinning: bool,
    scheduling: bool,
    blocking_channel: bool,
}

/// Parse the core mapping from the environment variable PPL_THREAD_MAPPING.
/// The core mapping is a string of comma separated integers.
/// Each integer represents a core. The order of the integers
/// in the string is the order in which the cores are used.
/// For example, if the string is "0,1,2,3", the first core
/// is used first, then the second core, and so on.
///
/// If the environment variable is not set, the core mapping
/// is set to the default mapping, i.e., the cores are used
/// in the order in which they are found by the framework.
fn parse_core_mapping() -> Vec<CoreId> {
    let mut thread_mapping = Vec::new();
    match env::var("PPL_THREAD_MAPPING") {
        Ok(val) => {
            let mapping: Vec<&str> = val.split(',').collect();
            (0..mapping.len()).for_each(|i| {
                thread_mapping.push(mapping[i].parse::<usize>().unwrap());
            });
        }
        Err(_) => {
            for i in 0..num_cpus::get() {
                thread_mapping.push(i);
            }
        }
    }

    let core_ids = core_affinity::get_core_ids().unwrap();

    let mut core_mapping = Vec::new();

    for thread in thread_mapping {
        if thread >= core_ids.len() {
            panic!("Error: The thread {} is not a valid core", thread);
        }
        core_mapping.push(core_ids[thread]);
    }

    core_mapping
}

impl Configuration {
    /// Create a new configuration.
    /// The parameters are:
    /// - max_cores: maximum number of cores allowed. Only valid when pinning is active.
    /// - pinning: enable pinning of the partitions to the cores.
    /// - scheduling: scheduling method used in the pipeline.
    /// - blocking_channel: enable blocking channel.
    pub fn new(
        max_cores: usize,
        pinning: bool,
        scheduling: bool,
        blocking_channel: bool,
    ) -> Configuration {
        let thread_mapping = parse_core_mapping();

        Configuration {
            max_cores,
            thread_mapping,
            pinning,
            scheduling,
            blocking_channel,
        }
    }

    /// Create a new configuration with the default values.
    /// The default values are:
    /// - max_cores: the number of cores found by the framework.
    /// - pinning: false.
    /// - scheduling: static.
    /// - blocking_channel: false.
    pub fn new_default() -> Configuration {
        let max_threads = match env::var("PPL_MAX_CORES") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_) => num_cpus::get(),
        };
        let pinning = match env::var("PPL_PINNING") {
            Ok(val) => val.parse::<bool>().unwrap(),
            Err(_) => false,
        };
        let scheduling = match env::var("PPL_SCHEDULE") {
            Ok(val) => {
                if val == "static" {
                    false
                } else if val == "dynamic" {
                    true
                } else {
                    panic!("Invalid scheduling policy");
                }
            }
            Err(_) => false,
        };
        let blocking_channel = match env::var("PPL_WAIT_POLICY") {
            Ok(val) => val.parse::<bool>().unwrap(),
            Err(_) => false,
        };
        Configuration::new(max_threads, pinning, scheduling, blocking_channel)
    }

    /// Get the maximum number of cores allowed.
    /// Only valid when pinning is active.
    /// Obliously, the maximum number of cores allowed is
    /// not the maximum number of threads allowed.
    /// More thread can be pinned to the same core.
    pub(crate) fn get_max_cores(&self) -> usize {
        self.max_cores
    }

    /// Get the thread mapping.
    /// The thread mapping is a vector of usize who specifies the
    /// order in which the partitions are pinned to the cores.
    /// Basically, when pinning is active, is created a partition
    /// for each core. Partition one is pinned to the core specified
    /// in the first position of the thread mapping, partition two
    /// is pinned to the core specified in the second position of the
    /// thread mapping, and so on.
    /// When pinning is enabled, the framework will try to put the workers
    /// of a threadpool, or the replicas of a stage of a pipeline, in
    /// a subset of neighboring cores. This is done to reduce the
    /// communication overhead between the workers.
    pub(crate) fn get_thread_mapping(&self) -> &Vec<CoreId> {
        &self.thread_mapping
    }

    /// Get the pinning flag.
    /// If the pinning flag is set, the workers are pinned to the
    /// cores specified in the thread mapping.
    pub(crate) fn get_pinning(&self) -> bool {
        self.pinning
    }

    /// Get the scheduling flag.
    /// The scheduling policy is applied when using the pipeline.
    /// If the scheduling policy is static, the distribution of the
    /// work is done in a round-robin fashion.
    /// Instead, if the scheduling policy is dynamic, the distribution
    /// of the work is done in a work-stealing fashion.
    pub(crate) fn get_scheduling(&self) -> bool {
        self.scheduling
    }

    /// Get the blocking channel flag.
    /// If the blocking channel flag is set, the channel used to
    /// communicate between the workers is blocking.
    /// Otherwise, the channel is non-blocking.
    /// This choice is enforced both in the pipeline and in the
    /// parallel map.
    pub(crate) fn get_blocking_channel(&self) -> bool {
        self.blocking_channel
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::serial_test::serial;

    fn reset_env() {
        env::remove_var("PPL_MAX_CORES");
        env::remove_var("PPL_PINNING");
        env::remove_var("PPL_WAIT_POLICY");
        env::remove_var("PPL_THREAD_MAPPING");
        env::remove_var("PPL_SCHEDULE");
    }

    #[test]
    #[serial]
    fn test_configuration() {
        let conf = Configuration::new_default();
        assert_eq!(conf.max_cores, num_cpus::get());
        //assert!(!conf.pinning);
        //assert!(!conf.blocking_channel);
    }

    #[test]
    #[serial]
    fn test_configuration_with_env() {
        env::set_var("PPL_MAX_CORES", "4");
        env::set_var("PPL_PINNING", "true");
        env::set_var("PPL_WAIT_POLICY", "true");
        env::set_var("PPL_SCHEDULE", "dynamic");

        let conf = Configuration::new_default();
        assert_eq!(conf.max_cores, 4);
        assert!(conf.pinning);
        assert!(conf.blocking_channel);
        assert!(conf.scheduling);
        reset_env();
    }
}
