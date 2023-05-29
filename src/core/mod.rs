//! Core components of the framework.
//!
//! This module contains the core components of the framework.
//!
//! The main idea behind the architecture of the framework is to have an orchestrator,
//! composed by a set of partitions. Each partition represent a core of the machine.
//! The orchestrator is responsible for the creation of the partitions, and for the
//! distribution of the tasks to the partitions. Each partition contain, if present, 
//! one or more executors.
//!
pub mod configuration;
pub mod orchestrator;
