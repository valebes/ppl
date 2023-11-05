use std::{collections::VecDeque, marker::PhantomData};

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
    /// Creates a new source from any type that implements the `Iterator` trait.
    /// The source will terminate when the iterator is exhausted.
    ///
    /// # Examples
    ///
    /// In this example we create a source node using a [`SourceIter`]
    /// template that emits numbers from 1 to 21.
    ///
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, Sequential, SinkVec}};
    ///
    /// let p = pipeline![
    ///     SourceIter::build(1..21),
    ///     Sequential::build(|el| { el }),
    ///     SinkVec::build()
    /// ];
    /// let res = p.start_and_wait_end().unwrap();
    /// ```
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
    ///    
    /// # Examples
    ///
    /// In this example we send element by element the content of a vector
    /// through a pipeline.
    /// Using the [`SinkVec`] template, we create a sink node that collects
    /// the data received.
    ///
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, Sequential, SinkVec}};
    ///
    /// let data = vec![1, 2, 3, 4, 5];
    /// let p = pipeline![
    ///     SourceIter::build(data.into_iter()),
    ///     Sequential::build(|el| { el }),
    ///     SinkVec::build()
    /// ];
    /// let res = p.start_and_wait_end().unwrap();
    /// assert_eq!(res,  vec![1, 2, 3, 4, 5])
    /// ```
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
/// and send each chunk to the next node.
#[derive(Clone)]
pub struct Splitter<T> {
    chunk_size: usize,
    n_replicas: usize,
    data: VecDeque<T>,
}
impl<T> Splitter<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new splitter node.
    /// The node will terminate when the upstream terminates.
    ///
    /// # Examples
    /// Given a stream of numbers, we create a pipeline with a splitter that
    /// create vectors of two elements each.
    ///
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, Splitter, SinkVec, Aggregator}};
    ///
    /// let vec = vec![1, 2, 3, 4, 5, 6, 7, 8];
    /// let p = pipeline![
    ///     SourceIter::build(vec.into_iter()),
    ///     Aggregator::build(8), // We aggregate each element in a vector.
    ///     Splitter::build(2), // We split the received vector in 4 sub-vector of size 2.
    ///     SinkVec::build()
    /// ];
    /// let mut res = p.start_and_wait_end().unwrap();
    /// assert_eq!(res.len(), 4)
    /// ```
    pub fn build(chunk_size: usize) -> impl InOut<Vec<T>, Vec<T>> {
        Self {
            chunk_size,
            n_replicas: 1,
            data: VecDeque::new(),
        }
    }

    /// Creates a new splitter node with 'n_replicas' replicas of the same node.
    /// The node will terminate when the upstream terminates.
    pub fn build_with_replicas(n_replicas: usize, chunk_size: usize) -> impl InOut<Vec<T>, Vec<T>> {
        Self {
            chunk_size,
            n_replicas,
            data: VecDeque::new(),
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
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
    }
    fn is_producer(&self) -> bool {
        true
    }
    fn produce(&mut self) -> Option<Vec<T>> {
        if self.data.len() >= self.chunk_size {
            let mut chunk = Vec::new();
            for _i in 0..self.chunk_size {
                chunk.push(self.data.pop_front().unwrap())
            }
            Some(chunk)
        } else {
            None
        }
    }
}

/// Aggregator.
///
/// This node receives elements and accumulates them into a vector.
/// When the vector reaches the size `chunk_size` it send the vector with the elements accumulated to the next node.
#[derive(Clone)]
pub struct Aggregator<T> {
    chunk_size: usize,
    n_replicas: usize,
    data: VecDeque<T>,
}
impl<T> Aggregator<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new aggregator node.
    /// The node will terminate when the upstream terminates.
    ///
    /// # Examples
    /// Given a stream of numbers, we use an [`Aggregator`] template to
    /// group the elements of this stream in vectors of size 100.
    ///
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, SinkVec, Aggregator}};
    ///
    /// let p = pipeline![
    ///     SourceIter::build(0..2000),
    ///     Aggregator::build(100),
    ///     SinkVec::build()
    /// ];
    /// let res = p.start_and_wait_end().unwrap();
    /// assert_eq!(res.len(), 20);
    /// ```
    pub fn build(chunk_size: usize) -> impl InOut<T, Vec<T>> {
        Self {
            chunk_size,
            n_replicas: 1,
            data: VecDeque::new(),
        }
    }

    /// Creates a new aggregator node with 'n_replicas' replicas of the same node.
    /// The node will terminate when the upstream terminates.
    pub fn build_with_replicas(n_replicas: usize, chunk_size: usize) -> impl InOut<T, Vec<T>> {
        Self {
            chunk_size,
            n_replicas,
            data: VecDeque::new(),
        }
    }
}
impl<T> InOut<T, Vec<T>> for Aggregator<T>
where
    T: Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<Vec<T>> {
        self.data.push_back(input);
        None
    }
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
    }
    fn is_producer(&self) -> bool {
        true
    }
    fn produce(&mut self) -> Option<Vec<T>> {
        if self.data.len() >= self.chunk_size {
            let mut chunk = Vec::new();
            for _i in 0..self.chunk_size {
                chunk.push(self.data.pop_front().unwrap())
            }
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
    ///
    /// # Examples
    /// Given a set of numbers from 0 to 199, we use a [`Filter`]
    /// template to filter the even numbers.
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, SinkVec, Filter}};
    ///
    /// let p = pipeline![
    ///     SourceIter::build(0..200),
    ///     Filter::build(|el| { el % 2 == 0 }),
    ///     SinkVec::build()
    /// ];
    ///
    /// let res = p.start_and_wait_end().unwrap();
    ///  assert_eq!(res.len(), 100)
    /// ```
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
/// This is an ordered version of [`SinkVec`].
/// The sink will produce a vector containing all the data received in the same order
/// as it was received from the upstream.
pub struct OrderedSinkVec<T> {
    data: Vec<T>,
}
impl<T> OrderedSinkVec<T>
where
    T: Send + 'static,
{
    /// Creates a new ordered sink that accumulates data into a vector.
    /// The sink will terminate when the upstream terminates.
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
/// and send each chunk to the next node.
/// This is an ordered versione of [`Splitter`].
/// This node mantains the order of the input in the output.
#[derive(Clone)]
pub struct OrderedSplitter<T> {
    chunk_size: usize,
    n_replicas: usize,
    data: VecDeque<T>,
}
impl<T> OrderedSplitter<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new ordered splitter node.
    /// The node will terminate when the upstream terminates.
    pub fn build(chunk_size: usize) -> impl InOut<Vec<T>, Vec<T>> {
        Self {
            chunk_size,
            n_replicas: 1,
            data: VecDeque::new(),
        }
    }

    /// Creates a new ordered splitter node with 'n_replicas' replicas of the same node.
    /// The node will terminate when the upstream terminates.
    pub fn build_with_replicas(n_replicas: usize, chunk_size: usize) -> impl InOut<Vec<T>, Vec<T>> {
        Self {
            chunk_size,
            n_replicas,
            data: VecDeque::new(),
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
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
    }
    fn is_producer(&self) -> bool {
        true
    }
    fn produce(&mut self) -> Option<Vec<T>> {
        if self.data.len() >= self.chunk_size {
            let mut chunk = Vec::new();
            for _i in 0..self.chunk_size {
                chunk.push(self.data.pop_front().unwrap())
            }
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
/// When the vector reaches the size `chunk_size` it send the vector with the elements accumulated to the next node.
/// This is an ordered version of [`Aggregator`].
/// This node mantains the order of the input in the output.
#[derive(Clone)]
pub struct OrderedAggregator<T> {
    chunk_size: usize,
    n_replicas: usize,
    data: VecDeque<T>,
}
impl<T> OrderedAggregator<T>
where
    T: Send + 'static + Clone,
{
    /// Creates a new ordered aggregator node
    /// The node will terminate when the upstream terminates.
    pub fn build(chunk_size: usize) -> impl InOut<T, Vec<T>> {
        Self {
            chunk_size,
            n_replicas: 1,
            data: VecDeque::new(),
        }
    }

    /// Creates a new ordered aggregator nod with 'n_replicas' replicas of the same node.
    /// The node will terminate when the upstream terminates.
    pub fn build_with_replicas(n_replicas: usize, chunk_size: usize) -> impl InOut<T, Vec<T>> {
        Self {
            chunk_size,
            n_replicas,
            data: VecDeque::new(),
        }
    }
}
impl<T> InOut<T, Vec<T>> for OrderedAggregator<T>
where
    T: Send + 'static + Clone,
{
    fn run(&mut self, input: T) -> Option<Vec<T>> {
        self.data.push_back(input);
        None
    }
    fn number_of_replicas(&self) -> usize {
        self.n_replicas
    }
    fn is_producer(&self) -> bool {
        true
    }
    fn produce(&mut self) -> Option<Vec<T>> {
        if self.data.len() >= self.chunk_size {
            let mut chunk = Vec::new();
            for _i in 0..self.chunk_size {
                chunk.push(self.data.pop_front().unwrap())
            }
            Some(chunk)
        } else {
            None
        }
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

/// OrderedSequential.
///
/// This node receives elements and applies a function to each element.
/// This is an ordered version of [`Sequential`].
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
/// This is an ordered version of [`Parallel`].
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
/// This is an ordered version of [`Filter`].
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
    pub fn build_with_replicas(n_replicas: usize, f: F) -> impl InOut<T, T> {
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

#[cfg(test)]
mod test {
    use crate::{
        prelude::*,
        templates::misc::{
            Filter, OrderedAggregator, OrderedFilter, OrderedParallel, OrderedSequential,
            OrderedSinkVec, OrderedSplitter, Parallel, Sequential, SinkVec, SourceIter,
        },
    };
    use serial_test::serial;

    use super::{Aggregator, Splitter};

    #[test]
    #[serial]
    fn simple_pipeline() {
        let p = pipeline![
            SourceIter::build(1..21),
            Sequential::build(|el| {
                el /*println!("Hello, received: {}", el); */
            }),
            SinkVec::build()
        ];

        let res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 20)
    }

    #[test]
    #[serial]
    fn simple_pipeline_ordered() {
        let p = pipeline![
            SourceIter::build(1..21),
            OrderedSequential::build(|el| {
                el /*println!("Hello, received: {}", el); */
            }),
            OrderedSinkVec::build()
        ];

        let res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 20);

        let mut counter = 1;
        for el in res {
            assert_eq!(el, counter);
            counter += 1;
        }
    }

    #[test]
    #[serial]
    fn simple_farm() {
        let p = pipeline![
            SourceIter::build(1..21),
            Parallel::build(8, |el| {
                el /*println!("Hello, received: {}", el); */
            }),
            SinkVec::build()
        ];

        let res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 20)
    }

    #[test]
    #[serial]
    fn simple_farm_ordered() {
        let p = pipeline![
            SourceIter::build(1..21),
            OrderedParallel::build(8, |el| {
                el /*println!("Hello, received: {}", el); */
            }),
            OrderedSinkVec::build()
        ];

        let res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 20);

        let mut counter = 1;
        for el in res {
            assert_eq!(el, counter);
            counter += 1;
        }
    }

    #[test]
    #[serial]
    fn splitter() {
        let mut counter = 1;
        let mut set = Vec::new();

        for i in 0..1000 {
            let mut vector = Vec::new();
            for _i in 0..20 {
                vector.push((i, counter));
                counter += 1;
            }
            counter = 1;
            set.push(vector);
        }

        let p = pipeline![
            SourceIter::build(set.into_iter()),
            Splitter::build_with_replicas(2, 2),
            Splitter::build(20000),
            SinkVec::build()
        ];

        let mut res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 1);

        let vec = res.pop().unwrap();

        assert_eq!(vec.len(), 20000)
    }

    #[test]
    #[serial]
    fn splitter_ordered() {
        let mut counter = 1;
        let mut set = Vec::new();

        for _i in 0..1000 {
            let mut vector = Vec::new();
            for _i in 0..20 {
                vector.push(counter);
                counter += 1;
            }
            set.push(vector);
        }

        let p = pipeline![
            SourceIter::build(set.into_iter()),
            OrderedSplitter::build_with_replicas(2, 10),
            OrderedSplitter::build_with_replicas(4, 1),
            OrderedSplitter::build(20000),
            OrderedSinkVec::build()
        ];

        let mut res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 1);

        let vec = res.pop().unwrap();

        assert_eq!(vec.len(), 20000);

        counter = 1;
        for el in vec {
            assert_eq!(el, counter);
            counter += 1;
        }
    }

    #[test]
    #[serial]
    fn aggregator() {
        let p = pipeline![
            SourceIter::build(0..2000),
            Aggregator::build(100),
            SinkVec::build()
        ];

        let res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 20);

        for vec in res {
            assert_eq!(vec.len(), 100);
        }
    }

    #[test]
    #[serial]
    fn aggregator_ordered() {
        let p = pipeline![
            SourceIter::build(0..2000),
            OrderedAggregator::build_with_replicas(4, 100),
            OrderedSplitter::build(1),
            OrderedSinkVec::build()
        ];

        let res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 2000);

        for vec in res {
            assert_eq!(vec.len(), 1);
        }
    }

    #[test]
    #[serial]
    fn filter() {
        let p = pipeline![
            SourceIter::build(0..200),
            Filter::build(|el| { el % 2 == 0 }),
            SinkVec::build()
        ];

        let res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 100)
    }

    #[test]
    #[serial]
    fn filter_ordered() {
        let p = pipeline![
            SourceIter::build(0..200),
            OrderedFilter::build_with_replicas(4, |el| { el % 2 == 0 }),
            OrderedSinkVec::build()
        ];

        let res = p.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 100);

        let mut counter = 0;
        for el in res {
            assert_eq!(el, counter);
            counter += 2;
        }
    }
}
