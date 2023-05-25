//! Collection of templates that can be used to create a pipeline.
//!
//! This module contains a collection of templates that can be used to create a pipeline.
//! These templates provide a more high level interface, offering a more simple way to
//! create a pipeline.
//! These templates implements the most common patterns used in a pipeline.
//! For example, in [`map`] there are various implementations of the map pattern
//! and derivatives.
//! In [`misc`] there are various implementations of the most common patterns that
//! are not included in the other modules.

/// Implementations of the map pattern and derivatives.
pub mod map;
/// Implementations of common patterns that are not included in the other modules.
pub mod misc;
