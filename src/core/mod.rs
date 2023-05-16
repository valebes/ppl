//! Core components of the framework.
//!
//! This module contains the core components of the framework.
//!
//! The main idea behind the architecture of the framework is to have an orchestrator,
//! composed by a set of partitions. Each partition represent a core of the machine.
//! The orchestrator is responsible for the creation of the partitions, and for the
//! distribution of the tasks to the partitions. Each partition contain a global queue
//! and, if present, one or more executors. The global queue is used to store the tasks
//! that are not yet assigned to an executor. The executors are responsible for the
//! execution of the tasks.
//!
pub mod configuration;
pub mod orchestrator;
