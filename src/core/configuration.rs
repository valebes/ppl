use std::env;

use core_affinity::CoreId;

/// Types of scheduling methods available for the pipeline.
/// 
/// * `Dynamic`: Workstealing between replicas of the same stage enabled.
/// * `Static`: The work is distribuited among the replicas of a stage in a Round-Robin way.
#[derive(PartialEq, Debug, Copy, Clone)]
pub enum Scheduling{
    Dynamic,
    Static,
}

/// Types of wait policies available.
/// 
/// This serves to provide the framework the desired behavior of waiting threads.
/// 
/// * `Active`: Prefers busy wait, consuming processor cycles while waiting.
/// * `Passive`: Prefers that waiting threads yield the processor to other threads
/// while waiting, in other words not consuming processor cycles.
#[derive(PartialEq, Debug, Copy, Clone)]
pub enum WaitPolicy{
    Active,
    Passive,
}

/// Configuration of the framework.
///
/// The configuration is set by the user through environment variables.
/// The environment variables are:
/// * `PPL_MAX_CORES`: Maximum number of cores were pinning is allowed. Only valid when pinning is active.
/// * `PPL_PINNING`: Enable threads pinning. Default is false.
/// * `PPL_SCHEDULE`: Scheduling method used in the pipeline. Default is static.
/// * `PPL_WAIT_POLICY`: Enable blocking in threads communications. Default is false.
/// * `PPL_THREADS_MAPPING`: Custom threads mapping to cores. Default is the order in which the cores are found.
pub struct Configuration {
    max_cores: usize,
    threads_mapping: Vec<CoreId>,
    pinning: bool,
    scheduling: Scheduling,
    wait_policy: WaitPolicy,
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
    let mut env_mapping = Vec::new();
    match env::var("PPL_THREADS_MAPPING") {
        Ok(val) => {
            let mapping: Vec<&str> = val.split(',').collect();

            for core in mapping {
                env_mapping.push(core.parse::<usize>().unwrap())
            }
        }
        Err(_) => {
            for i in 0..num_cpus::get() {
                env_mapping.push(i);
            }
        }
    }

    let core_ids = core_affinity::get_core_ids().unwrap();

    let mut threads_mapping = Vec::new();

    for thread in env_mapping {
        if thread >= core_ids.len() {
            panic!("Error: The thread mapping is invalid");
        }
        threads_mapping.push(core_ids[thread]);
    }

    threads_mapping
}

impl Configuration {
    /// Create a new configuration.
    /// # Arguments
    /// * `max_cores`: maximum number of cores allowed. Only valid when pinning is active.
    /// * `pinning`: enable threads pinning.
    /// * `scheduling`: scheduling method used in the pipeline.
    /// * `wait_policy`: the threads wait policy that the framework should prefer.
    pub fn new(
        max_cores: usize,
        pinning: bool,
        scheduling: Scheduling,
        wait_policy: WaitPolicy,
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
    /// # Default values
    /// * `max_cores`: the number of cores found by the framework.
    /// * `pinning`: false.
    /// * `scheduling`: static.
    /// * `wait_policy`: passive.
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
                let value = val.to_lowercase();
                if value == "static" {
                    Scheduling::Static
                } else if value == "dynamic" {
                    Scheduling::Dynamic
                } else {
                    panic!("{} is an invalid scheduling policy", value);
                }
            }
            Err(_) => Scheduling::Static,
        };
        let wait_policy = match env::var("PPL_WAIT_POLICY") {
            Ok(val) => {
                let value = val.to_lowercase();
                if value == "active" {
                    WaitPolicy::Active
                } else if value == "passive" {
                    WaitPolicy::Passive
                } else {
                    panic!("{} is an invalid threads wait policy", value);
                }
            },
            Err(_) => WaitPolicy::Passive,
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
    pub(crate) fn get_scheduling(&self) -> Scheduling {
        self.scheduling
    }

    /// Get the wait policy flag.
    /// If the wait policy flag is set, the channels used to
    /// communicate between the workers are blocking.
    /// Otherwise, the channels are non-blocking.
    /// This choice is enforced both in the pipeline and in the
    /// parallel map.
    pub(crate) fn get_wait_policy(&self) -> WaitPolicy {
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
        env::set_var("PPL_WAIT_POLICY", "PASSIVE");
        env::set_var("PPL_SCHEDULE", "DYNAMIC");

        let conf = Configuration::new_default();
        assert_eq!(conf.max_cores, 4);
        assert!(conf.pinning);
        assert_eq!(conf.wait_policy, WaitPolicy::Passive);
        assert_eq!(conf.scheduling, Scheduling::Dynamic);
        reset_env();
    }
}
