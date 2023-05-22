use std::marker::PhantomData;

use crate::pipeline::{in_node::In, inout_node::InOut, out_node::Out};

/// SourceIter
/// This source node produces data from a iterator
pub struct SourceIter<I, T>
where
    I: Iterator<Item = T>,
{
    iterator: I,
    phantom: PhantomData<T>,
}
impl<I, T> SourceIter<I, T>
where
    I: Iterator<Item = T>,
    T: Send + 'static,
{
    /// Creates a new source from a iterator
    /// The source will terminate when the iterator is exhausted
    pub fn build(iterator: I) -> impl Out<T> {
        Self {
            iterator,
            phantom: PhantomData,
        }
    }
}
impl<I, T> Out<T> for SourceIter<I, T>
where
    I: Iterator<Item = T>,
    T: Send + 'static,
{
    fn run(&mut self) -> Option<T> {
        self.iterator.next()
    }
}

/// SinkVec
/// Sink node that accumulates data into a vector
/// The sink will terminate when the upstream terminates
/// The sink will produce a vector containing all the data received
/// from the upstream.
pub struct SinkVec<T> {
    data: Vec<T>,
}
impl<T> SinkVec<T>
where
    T: Send + 'static,
{
    /// Creates a new sink that accumulates data into a vector
    /// The sink will terminate when the upstream terminates
    /// The sink will produce a vector containing all the data received.
    pub fn build() -> impl In<T, Vec<T>> {
        Self { data: Vec::new() }
    }
}
impl<T> In<T, Vec<T>> for SinkVec<T>
where
    T: Send + 'static,
{
    fn run(&mut self, input: T) {
        self.data.push(input);
    }
    fn finalize(self) -> Option<Vec<T>> {
        Some(self.data)
    }
}

/// Splitter
/// This node receives a vector, split it into chunks of size `chunk_size`
/// and send each chunk into the next node.
/// The node will terminate when the upstream terminates.
#[derive(Clone)]
pub struct Splitter<T> {
    chunk_size: usize,
    data: Vec<T>,
}
impl<T> Splitter<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new splitter node
    /// The node will terminate when the upstream terminates.
    pub fn build(chunk_size: usize) -> impl InOut<Vec<T>, Vec<T>> {
        Self {
            chunk_size,
            data: Vec::new(),
        }
    }
}
impl<T> InOut<Vec<T>, Vec<T>> for Splitter<T>
where
    T: Send + 'static + Clone,
{
    fn run(&mut self, input: Vec<T>) -> Option<Vec<T>> {
        self.data.extend(input);
        None
    }
    fn is_producer(&self) -> bool {
        true
    }
    fn produce(&mut self) -> Option<Vec<T>> {
        if self.data.len() >= self.chunk_size {
            let mut chunk = Vec::new();
            std::mem::swap(&mut chunk, &mut self.data);
            Some(chunk)
        } else {
            None
        }
    }
}

/// Aggregator
/// This node receives elements and accumulates them into a vector
/// When the vector reaches the size `chunk_size` it is sent to the next node
/// The node will terminate when the upstream terminates.
#[derive(Clone)]
pub struct Aggregator<T> {
    chunk_size: usize,
    data: Vec<T>,
}
impl<T> Aggregator<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new aggregator node
    /// The node will terminate when the upstream terminates.
    pub fn build(chunk_size: usize) -> impl InOut<T, Vec<T>> {
        Self {
            chunk_size,
            data: Vec::new(),
        }
    }
}
impl<T> InOut<T, Vec<T>> for Aggregator<T>
where
    T: Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<Vec<T>> {
        self.data.push(input);
        if self.data.len() >= self.chunk_size {
            let mut chunk = Vec::new();
            std::mem::swap(&mut chunk, &mut self.data);
            Some(chunk)
        } else {
            None
        }
    }
    fn is_producer(&self) -> bool {
        true
    }
    fn produce(&mut self) -> Option<Vec<T>> {
        if !self.data.is_empty() {
            let mut chunk = Vec::new();
            std::mem::swap(&mut chunk, &mut self.data);
            Some(chunk)
        } else {
            None
        }
    }
}

/// OrderedSinkVec
/// Sink node that accumulates data into a vector
/// This is a ordered version of SinkVec
/// The sink will terminate when the upstream terminates
/// The sink will produce a vector containing all the data received in the same order
/// as it was received from the upstream.
pub struct OrderedSinkVec<T> {
    data: Vec<T>,
}
impl<T> OrderedSinkVec<T>
where
    T: Send + 'static,
{
    /// Creates a new sink that accumulates data into a vector
    /// This is a ordered version of SinkVec
    /// The sink will terminate when the upstream terminates
    /// The sink will produce a vector containing all the data received in the same order
    /// as it was received from the upstream.
    pub fn build() -> impl In<T, Vec<T>> {
        Self { data: Vec::new() }
    }
}
impl<T> In<T, Vec<T>> for OrderedSinkVec<T>
where
    T: Send + 'static,
{
    fn run(&mut self, input: T) {
        self.data.push(input);
    }
    fn is_ordered(&self) -> bool {
        true
    }
    fn finalize(self) -> Option<Vec<T>> {
        Some(self.data)
    }
}

/// OrderedSplitter
/// This node receives a vector, split it into chunks of size `chunk_size`
/// and send each chunk into the next node.
/// This is a ordered version of Splitter
/// The node will terminate when the upstream terminates.
/// The node will produce data in the same order as it is received from the upstream.
#[derive(Clone)]
pub struct OrderedSplitter<T> {
    chunk_size: usize,
    data: Vec<T>,
}
impl<T> OrderedSplitter<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new splitter node
    /// The node will terminate when the upstream terminates.
    /// The node will produce data in the same order as it is received from the upstream.
    pub fn build(chunk_size: usize) -> impl InOut<Vec<T>, Vec<T>> {
        Self {
            chunk_size,
            data: Vec::new(),
        }
    }
}
impl<T> InOut<Vec<T>, Vec<T>> for OrderedSplitter<T>
where
    T: Send + 'static + Clone,
{
    fn run(&mut self, input: Vec<T>) -> Option<Vec<T>> {
        self.data.extend(input);
        None
    }
    fn is_producer(&self) -> bool {
        true
    }
    fn produce(&mut self) -> Option<Vec<T>> {
        if self.data.len() >= self.chunk_size {
            let mut chunk = Vec::new();
            std::mem::swap(&mut chunk, &mut self.data);
            Some(chunk)
        } else {
            None
        }
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

/// OrderedAggregator
/// This node receives elements and accumulates them into a vector
/// When the vector reaches the size `chunk_size` it is sent to the next node
/// This is a ordered version of Aggregator
/// The node will terminate when the upstream terminates.
/// The node will produce data in the same order as it is received from the upstream.
#[derive(Clone)]
pub struct OrderedAggregator<T> {
    chunk_size: usize,
    data: Vec<T>,
}
impl<T> OrderedAggregator<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new aggregator node
    /// The node will terminate when the upstream terminates.
    /// The node will produce data in the same order as it is received from the upstream.
    pub fn build(chunk_size: usize) -> impl InOut<T, Vec<T>> {
        Self {
            chunk_size,
            data: Vec::new(),
        }
    }
}
impl<T> InOut<T, Vec<T>> for OrderedAggregator<T>
where
    T: Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<Vec<T>> {
        self.data.push(input);
        if self.data.len() >= self.chunk_size {
            let mut chunk = Vec::new();
            std::mem::swap(&mut chunk, &mut self.data);
            Some(chunk)
        } else {
            None
        }
    }
    fn is_producer(&self) -> bool {
        true
    }
    fn produce(&mut self) -> Option<Vec<T>> {
        if !self.data.is_empty() {
            let mut chunk = Vec::new();
            std::mem::swap(&mut chunk, &mut self.data);
            Some(chunk)
        } else {
            None
        }
    }
    fn is_ordered(&self) -> bool {
        true
    }
}
