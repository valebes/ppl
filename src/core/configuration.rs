use std::env;

use core_affinity::CoreId;

/// Configuration of the framework.
///
/// The configuration is set by the user through environment variables.
/// The environment variables are:
/// - PPL_MAX_CORES: Maximum number of cores were pinning is allowed. Only valid when pinning is active.
/// - PPL_PINNING: Enable threads pinning. Default is false.
/// - PPL_SCHEDULE: Scheduling method used in the pipeline. Default is static.
/// - PPL_WAIT_POLICY: Enable blocking in threads communications. Default is false.
/// - PPL_THREADS_MAPPING: Custom threads mapping to cores. Default is the order in which the cores are found.
pub struct Configuration {
    max_cores: usize,
    threads_mapping: Vec<CoreId>,
    pinning: bool,
    scheduling: bool,
    wait_policy: bool,
}

/// Parse the threads mapping from the environment variable PPL_THREAD_MAPPING.
/// The threads mapping is a string of comma separated integers.
/// Each integer represents a core. The order of the integers
/// in the string is the order in which the cores are used for pinning.
/// For example, if the string is "0,1,2,3", the first core
/// is used first, then the second core, and so on.
///
/// If the environment variable is not set, the threads mapping
/// is set to the default mapping, i.e., the cores are used
/// in the order in which they are found by the framework.
fn parse_threads_mapping() -> Vec<CoreId> {
    let mut thread_mapping = Vec::new();
    match env::var("PPL_THREADS_MAPPING") {
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

    let mut threads_mapping = Vec::new();

    for thread in thread_mapping {
        if thread >= core_ids.len() {
            panic!("Error: The thread mapping is invalid");
        }
        threads_mapping.push(core_ids[thread]);
    }

    threads_mapping
}

impl Configuration {
    /// Create a new configuration.
    /// The parameters are:
    /// - max_cores: maximum number of cores allowed. Only valid when pinning is active.
    /// - pinning: enable pinning of the partitions to the cores.
    /// - scheduling: scheduling method used in the pipeline.
    /// - wait_policy: enable blocking in channels.
    pub fn new(
        max_cores: usize,
        pinning: bool,
        scheduling: bool,
        wait_policy: bool,
    ) -> Configuration {
        let threads_mapping = parse_threads_mapping();

        Configuration {
            max_cores,
            threads_mapping,
            pinning,
            scheduling,
            wait_policy,
        }
    }

    /// Create a new configuration with the default values.
    /// The default values are:
    /// - max_cores: the number of cores found by the framework.
    /// - pinning: false.
    /// - scheduling: static.
    /// - wait_policy: false.
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
        let wait_policy = match env::var("PPL_WAIT_POLICY") {
            Ok(val) => val.parse::<bool>().unwrap(),
            Err(_) => false,
        };
        Configuration::new(max_threads, pinning, scheduling, wait_policy)
    }

    /// Get the maximum number of cores allowed.
    /// Only valid when pinning is active.
    /// Obliously, the maximum number of cores allowed is
    /// not the maximum number of threads allowed.
    /// More thread can be pinned to the same core.
    pub(crate) fn get_max_cores(&self) -> usize {
        self.max_cores
    }

    /// Get the threads mapping.
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
    pub(crate) fn get_threads_mapping(&self) -> &Vec<CoreId> {
        &self.threads_mapping
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

    /// Get the wait policy flag.
    /// If the wait policy flag is set, the channels used to
    /// communicate between the workers are blocking.
    /// Otherwise, the channels are non-blocking.
    /// This choice is enforced both in the pipeline and in the
    /// parallel map.
    pub(crate) fn get_wait_policy(&self) -> bool {
        self.wait_policy
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
        env::remove_var("PPL_THREADS_MAPPING");
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
        assert!(conf.wait_policy);
        assert!(conf.scheduling);
        reset_env();
    }
}
