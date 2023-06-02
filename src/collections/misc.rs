use std::marker::PhantomData;

use crate::pipeline::node::{In, InOut, Out};

/// SourceIter.
/// 
/// This source node produces data from a iterator.
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
    /// Creates a new source from a iterator.
    /// The source will terminate when the iterator is exhausted.
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

/// SinkVec.
/// 
/// Sink node that accumulates data into a vector.
/// The sink will terminate when the upstream terminates.
/// The sink will produce a vector containing all the data received
/// from the upstream.
pub struct SinkVec<T> {
    data: Vec<T>,
}
impl<T> SinkVec<T>
where
    T: Send + 'static,
{
    /// Creates a new sink that accumulates data into a vector.
    /// The sink will terminate when the upstream terminates.
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

/// Splitter.
/// 
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
    /// Creates a new splitter node.
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

/// Aggregator.
/// 
/// This node receives elements and accumulates them into a vector.
/// When the vector reaches the size `chunk_size` it is sent to the next node.
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

/// Sequential node.
/// 
/// Given a function that defines the logic of the stage, this method will create a stage with one replica.
#[derive(Clone)]
pub struct Sequential<T, U, F>
where
    T: Send + 'static,
    U: Send + 'static,
    F: FnMut(T) -> U + Send + 'static,
{
    f: F,
    phantom: PhantomData<T>,
}
impl<T, U, F> Sequential<T, U, F>
where
    T: Send + 'static + Clone,
    U: Send + 'static + Clone,
    F: FnMut(T) -> U + Send + 'static + Clone,
{
    /// Creates a new sequential node.
    /// The node will terminate when the upstream terminates.
    pub fn build(f: F) -> impl InOut<T, U> {
        Self {
            f,
            phantom: PhantomData,
        }
    }
}
impl<T, U, F> InOut<T, U> for Sequential<T, U, F>
where
    T: Send + 'static + Clone,
    U: Send + 'static + Clone,
    F: FnMut(T) -> U + Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<U> {
        Some((self.f)(input))
    }
}

/// Parallel node.
/// 
/// Given a function that defines the logic of the stage, this method will create 'n_replicas' replicas of that stage.
#[derive(Clone)]
pub struct Parallel<T, U, F>
where
    T: Send + 'static,
    U: Send + 'static,
    F: FnMut(T) -> U + Send + 'static,
{
    n_replicas: usize,
    f: F,
    phantom: PhantomData<T>,
}
impl<T, U, F> Parallel<T, U, F>
where
    T: Send + 'static + Clone,
    U: Send + 'static + Clone,
    F: FnMut(T) -> U + Send + 'static + Clone,
{
    /// Creates a new parallel node
    /// The node will terminate when the upstream terminates.
    pub fn build(n_replicas: usize, f: F) -> impl InOut<T, U> {
        Self {
            n_replicas,
            f,
            phantom: PhantomData,
        }
    }
}
impl<T, U, F> InOut<T, U> for Parallel<T, U, F>
where
    T: Send + 'static + Clone,
    U: Send + 'static + Clone,
    F: FnMut(T) -> U + Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<U> {
        Some((self.f)(input))
    }
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
    }
}

/// Filter.
/// 
/// This node receives elements and filters them according to the given predicate.
/// The node will terminate when the upstream terminates.
#[derive(Clone)]
pub struct Filter<T, F>
where
    T: Send + 'static,
    F: FnMut(&T) -> bool + Send + 'static,
{
    f: F,
    n_replicas: usize,
    phantom: PhantomData<T>,
}
impl<T, F> Filter<T, F>
where
    T: Send + 'static + Clone,
    F: FnMut(&T) -> bool + Send + 'static + Clone,
{
    /// Creates a new filter node.
    /// The node will terminate when the upstream terminates.
    pub fn build(f: F) -> impl InOut<T, T> {
        Self {
            f,
            n_replicas: 1,
            phantom: PhantomData,
        }
    }
    /// Creates a new filter node with 'n_replicas' replicas of the same node.
    /// The node will terminate when the upstream terminates.
    pub fn build_with_replicas(n_replicas: usize, f: F) -> impl InOut<T, T> {
        Self {
            f,
            n_replicas,
            phantom: PhantomData,
        }
    }
}
impl<T, F> InOut<T, T> for Filter<T, F>
where
    T: Send + 'static + Clone,
    F: FnMut(&T) -> bool + Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<T> {
        if (self.f)(&input) {
            Some(input)
        } else {
            None
        }
    }
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
    }
}

// Ordered versions of the above

/// OrderedSinkVec.
/// 
/// Sink node that accumulates data into a vector.
/// This is a ordered version of SinkVec.
/// The sink will terminate when the upstream terminates.
/// The sink will produce a vector containing all the data received in the same order
/// as it was received from the upstream.
pub struct OrderedSinkVec<T> {
    data: Vec<T>,
}
impl<T> OrderedSinkVec<T>
where
    T: Send + 'static,
{
    /// Creates a new sink that accumulates data into a vector.
    /// This is a ordered version of SinkVec.
    /// The sink will terminate when the upstream terminates.
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

/// OrderedSplitter.
/// 
/// This node receives a vector, split it into chunks of size `chunk_size`
/// and send each chunk into the next node.
/// This is a ordered version of Splitter.
/// The node will terminate when the upstream terminates.
/// The node will produce data in the same order as it is received from the upstream.
#[derive(Clone)]
pub struct OrderedSplitter<T> {
    chunk_size: usize,
    n_replicas: usize,
    data: Vec<T>,
}
impl<T> OrderedSplitter<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new splitter node.
    /// The node will terminate when the upstream terminates.
    /// The node will produce data in the same order as it is received from the upstream.
    pub fn build(chunk_size: usize) -> impl InOut<Vec<T>, Vec<T>> {
        Self {
            chunk_size,
            n_replicas: 1,
            data: Vec::new(),
        }
    }
    pub fn build_with_replicas(chunk_size: usize, n_replicas: usize) -> impl InOut<Vec<T>, Vec<T>> {
        Self {
            chunk_size,
            n_replicas,
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
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
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

/// OrderedAggregator.
/// 
/// This node receives elements and accumulates them into a vector.
/// When the vector reaches the size `chunk_size` it is sent to the next node.
/// This is a ordered version of Aggregator.
/// The node will terminate when the upstream terminates.
/// The node will produce data in the same order as it is received from the upstream.
#[derive(Clone)]
pub struct OrderedAggregator<T> {
    chunk_size: usize,
    n_replicas: usize,
    data: Vec<T>,
}
impl<T> OrderedAggregator<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new aggregator node.
    /// The node will terminate when the upstream terminates.
    /// The node will produce data in the same order as it is received from the upstream.
    pub fn build(chunk_size: usize) -> impl InOut<T, Vec<T>> {
        Self {
            chunk_size,
            n_replicas: 1,
            data: Vec::new(),
        }
    }
    pub fn build_with_replicas(chunk_size: usize, n_replicas: usize) -> impl InOut<T, Vec<T>> {
        Self {
            chunk_size,
            n_replicas,
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
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

/// OrderedSequential.
/// 
/// This node receives elements and applies a function to each element.
/// This is a ordered version of Sequential.
/// The node will terminate when the upstream terminates.
/// The node will produce data in the same order as it is received from the upstream.
#[derive(Clone)]
pub struct OrderedSequential<T, U, F> {
    f: F,
    phantom: PhantomData<(T, U)>,
}
impl<T, U, F> OrderedSequential<T, U, F>
where
    T: Send + 'static + Clone,
    U: Send + 'static + Clone,
    F: FnMut(T) -> U + Send + 'static + Clone,
{
    /// Creates a new sequential node.
    /// The node will terminate when the upstream terminates.
    /// The node will produce data in the same order as it is received from the upstream.
    pub fn build(f: F) -> impl InOut<T, U> {
        Self {
            f,
            phantom: PhantomData,
        }
    }
}
impl<T, U, F> InOut<T, U> for OrderedSequential<T, U, F>
where
    T: Send + 'static + Clone,
    U: Send + 'static + Clone,
    F: FnMut(T) -> U + Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<U> {
        Some((self.f)(input))
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

/// OrderedParallel.
/// 
/// This node receives elements and applies a function to each element.
/// This is a ordered version of Parallel.
/// The node will terminate when the upstream terminates.
/// The node will produce data in the same order as it is received from the upstream.
#[derive(Clone)]
pub struct OrderedParallel<T, U, F> {
    f: F,
    n_replicas: usize,
    phantom: PhantomData<(T, U)>,
}
impl<T, U, F> OrderedParallel<T, U, F>
where
    T: Send + 'static + Clone,
    U: Send + 'static + Clone,
    F: FnMut(T) -> U + Send + 'static + Clone,
{
    /// Creates a new parallel node.
    /// The node will terminate when the upstream terminates.
    /// The node will produce data in the same order as it is received from the upstream.
    pub fn build(n_replicas: usize, f: F) -> impl InOut<T, U> {
        Self {
            f,
            n_replicas,
            phantom: PhantomData,
        }
    }
}
impl<T, U, F> InOut<T, U> for OrderedParallel<T, U, F>
where
    T: Send + 'static + Clone,
    U: Send + 'static + Clone,
    F: FnMut(T) -> U + Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<U> {
        Some((self.f)(input))
    }
    fn is_ordered(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
    }
}

/// OrderedFilter.
/// 
/// This node receives elements and filters them according to a predicate.
/// This is a ordered version of Filter.
/// The node will terminate when the upstream terminates.
#[derive(Clone)]
pub struct OrderedFilter<T, F> {
    f: F,
    n_replicas: usize,
    phantom: PhantomData<T>,
}
impl<T, F> OrderedFilter<T, F>
where
    T: Send + 'static + Clone,
    F: FnMut(&T) -> bool + Send + 'static + Clone,
{
    /// Creates a new filter node.
    /// The node will terminate when the upstream terminates.
    pub fn build(f: F) -> impl InOut<T, T> {
        Self {
            f,
            n_replicas: 1,
            phantom: PhantomData,
        }
    }
    /// Creates a new filter node.
    /// The node will terminate when the upstream terminates.
    pub fn build_with_replicas(f: F, n_replicas: usize) -> impl InOut<T, T> {
        Self {
            f,
            n_replicas,
            phantom: PhantomData,
        }
    }
}
impl<T, F> InOut<T, T> for OrderedFilter<T, F>
where
    T: Send + 'static + Clone,
    F: FnMut(&T) -> bool + Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<T> {
        if (self.f)(&input) {
            Some(input)
        } else {
            None
        }
    }
    fn is_ordered(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
    }
}
