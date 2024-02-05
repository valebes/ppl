use crate::pipeline::node::InOut;
use std::marker::PhantomData;

use crate::thread_pool::ThreadPool;

/// Map
#[derive(Clone)]
pub struct Map<TIn, TOut, F>
where
    TIn: Send,
    TOut: Send,
    F: FnOnce(TIn) -> TOut + Send + Copy,
{
    threadpool: ThreadPool,
    replicas: usize,
    f: F,
    phantom: PhantomData<(TIn, TOut)>,
}
impl<TIn, TOut, F> Map<TIn, TOut, F>
where
    TIn: Send + Clone,
    TOut: Send + Clone + 'static,
    F: FnOnce(TIn) -> TOut + Send + Copy,
{
    /// Create a new Map node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    ///
    /// # Examples
    ///
    /// Given a vector of vectors, each one containing a set of numbers,
    /// compute the square value of each number contained in each
    /// vector.
    ///
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, SinkVec}, templates::map::Map};
    ///
    /// let numbers: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
    /// let mut vector = Vec::new();
    ///
    /// // Create the vector of vectors.
    /// for _i in 0..1000 {
    ///     vector.push(numbers.clone());
    /// }
    /// // Instantiate a new Pipeline with a Map operator.
    /// let pipe = pipeline![
    ///     SourceIter::build(vector.into_iter()),
    ///     Map::build(4, |el: f64| el * el),
    ///     SinkVec::build()
    /// ];
    /// // Start the pipeline and collect the results.
    /// let res: Vec<Vec<f64>> = pipe.start_and_wait_end().unwrap();
    /// ```
    pub fn build<TInIter, TOutIter>(n_worker: usize, f: F) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<TOut>,
    {
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: 1,
            f,
            phantom: PhantomData,
        }
    }
    /// Create a new Map node with n_replicas replicas.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `n_replicas` - Number of replicas.
    /// * `f` - Function to apply to each element of the input.
    /// # Panics
    /// Panics if n_replicas is 0.
    /// # Remarks
    /// The replicas are created by cloning the Map node.
    /// This mean that 4 replicas of a Map node with 2 workers each
    /// will result in the usage of 8 threads.
    pub fn build_with_replicas<TInIter, TOutIter>(
        n_worker: usize,
        n_replicas: usize,
        f: F,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<TOut>,
    {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: n_replicas,
            f,
            phantom: PhantomData,
        }
    }
}
impl<TIn, TInIter, TOut, TOutIter, F> InOut<TInIter, TOutIter> for Map<TIn, TOut, F>
where
    TIn: Send + Clone,
    TInIter: IntoIterator<Item = TIn>,
    TOut: Send + Clone + 'static,
    TOutIter: FromIterator<TOut>,
    F: FnOnce(TIn) -> TOut + Send + Copy,
{
    fn run(&mut self, input: TInIter) -> Option<TOutIter> {
        let res: TOutIter = self.threadpool.par_map(input, self.f).collect();
        Some(res)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
}

/// Ordered Map
///
/// In this Map, the elements are processed in the same order as they are received.
#[derive(Clone)]
pub struct OrderedMap<TIn, TOut, F>
where
    TIn: Send,
    TOut: Send,
    F: FnOnce(TIn) -> TOut + Send + Copy,
{
    threadpool: ThreadPool,
    replicas: usize,
    f: F,
    phantom: PhantomData<(TIn, TOut)>,
}
impl<TIn, TOut, F> OrderedMap<TIn, TOut, F>
where
    TIn: Send + Clone,
    TOut: Send + Clone + 'static,
    F: FnOnce(TIn) -> TOut + Send + Copy,
{
    /// Create a new OrderedMap node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    pub fn build<TInIter, TOutIter>(n_worker: usize, f: F) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<TOut>,
    {
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: 1,
            f,
            phantom: PhantomData,
        }
    }
    /// Create a new OrderedMap node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `n_replicas` - Number of replicas.
    /// * `f` - Function to apply to each element of the input.
    /// # Panics
    /// Panics if n_replicas is 0.
    /// # Remarks
    /// The replicas are created by cloning the OrderedMap node.
    /// This mean that 4 replicas of an Ordered Map node with 2 workers each
    /// will result in the usage of 8 threads.
    ///
    /// # Examples
    ///
    /// Given a vector of vectors, each one containing a set of numbers,
    /// compute the square value of each number contained in each
    /// vector.
    /// In this case, using the OrderedMap template, it is possible
    /// to mantain the order of the input in the output.
    /// Moreover, using the `build_with_replicas` method,
    /// we can create a stage consisting of four map operator,
    /// each one composed by 2 worker.
    ///
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, OrderedSinkVec}, templates::map::OrderedMap};
    /// let mut counter = 1.0;
    /// let mut vector = Vec::new();
    ///
    /// // Create a vector of vectors, each one containing a set of numbers.
    /// for _i in 0..1000{
    ///    let mut numbers = Vec::new();
    ///    for _i in 0..10 {
    ///        numbers.push(counter);
    ///        counter += 1.0;
    ///    }
    ///   vector.push(numbers);
    /// }
    ///
    /// // Instantiate the pipeline.
    /// let pipe = pipeline![
    ///     SourceIter::build(vector.into_iter()),
    ///     OrderedMap::build_with_replicas(4, 2, |el: f64| el * el),
    ///     OrderedSinkVec::build()
    /// ];
    ///
    /// // Start the pipeline and collect the results.
    /// let res: Vec<Vec<f64>> = pipe.start_and_wait_end().unwrap();
    /// ```
    pub fn build_with_replicas<TInIter, TOutIter>(
        n_worker: usize,
        n_replicas: usize,
        f: F,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<TOut>,
    {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: n_replicas,
            f,
            phantom: PhantomData,
        }
    }
}
impl<TIn, TInIter, TOut, TOutIter, F> InOut<TInIter, TOutIter> for OrderedMap<TIn, TOut, F>
where
    TIn: Send + Clone,
    TInIter: IntoIterator<Item = TIn>,
    TOut: Send + Clone + 'static,
    TOutIter: FromIterator<TOut>,
    F: FnOnce(TIn) -> TOut + Send + Copy,
{
    fn run(&mut self, input: TInIter) -> Option<TOutIter> {
        let res: TOutIter = self.threadpool.par_map(input, self.f).collect();
        Some(res)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

/// FlatMap
#[derive(Clone)]
pub struct FlatMap<TIn, TOut, F>
where
    TIn: Send + IntoIterator,
    TOut: Send + Iterator,
    F: FnOnce(TIn) -> TOut + Send + Copy,
{
    threadpool: ThreadPool,
    replicas: usize,
    f: F,
    phantom: PhantomData<(TIn, TOut)>,
}
impl<TIn, TOut, F> FlatMap<TIn, TOut, F>
where
    TIn: Send + Clone + IntoIterator,
    TOut: Send + Clone + Iterator + 'static,
    F: FnOnce(TIn) -> TOut + Send + Copy,
{
    /// Create a new FlatMap node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    ///
    /// # Examples
    ///
    /// Given a vector of vectors, each one containing a set of numbers,
    /// compute the square value of each number contained in each
    /// vector.
    ///
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, SinkVec}, templates::map::FlatMap};
    ///
    /// let a: Vec<Vec<u64>> = vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]];
    /// let mut vector = Vec::new();
    ///
    /// // Create the vector of vectors.
    /// for _i in 0..1000 {
    ///     vector.push(a.clone());
    /// }
    /// // Instantiate a new Pipeline with a Map operator.
    /// let pipe = pipeline![
    ///     SourceIter::build(vector.into_iter()),
    ///     FlatMap::build(4, |x: Vec<u64>| x.into_iter()),
    ///     SinkVec::build()
    /// ];
    /// // Start the pipeline and collect the results.
    /// let mut res: Vec<Vec<u64>> = pipe.start_and_wait_end().unwrap();
    /// let check = res.pop().unwrap();
    /// assert_eq!(&check, &[1, 2, 3, 4, 5, 6, 7, 8]);
    /// ```
    pub fn build<TInIter, TOutIter>(n_worker: usize, f: F) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<<TOut as Iterator>::Item>,
    {
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: 1,
            f,
            phantom: PhantomData,
        }
    }
    /// Create a new FlatMap node with n_replicas replicas.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `n_replicas` - Number of replicas.
    /// * `f` - Function to apply to each element of the input.
    /// # Panics
    /// Panics if n_replicas is 0.
    /// # Remarks
    /// The replicas are created by cloning the Map node.
    /// This mean that 4 replicas of a Map node with 2 workers each
    /// will result in the usage of 8 threads.
    pub fn build_with_replicas<TInIter, TOutIter>(
        n_worker: usize,
        n_replicas: usize,
        f: F,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<<TOut as Iterator>::Item>,
    {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: n_replicas,
            f,
            phantom: PhantomData,
        }
    }
}
impl<TIn, TInIter, TOut, TOutIter, F> InOut<TInIter, TOutIter> for FlatMap<TIn, TOut, F>
where
    TIn: Send + Clone + IntoIterator,
    TInIter: IntoIterator<Item = TIn>,
    TOut: Send + Clone + Iterator + 'static,
    TOutIter: FromIterator<<TOut as Iterator>::Item>,
    F: FnOnce(TIn) -> TOut + Send + Copy,
{
    fn run(&mut self, input: TInIter) -> Option<TOutIter> {
        let res: TOutIter = self.threadpool.par_map(input, self.f).flatten().collect();
        Some(res)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
}

/// Reduce
///
/// Takes in input a type that implements the [`Iterator`] trait
/// and applies a reduce function.
#[derive(Clone)]
pub struct Reduce<TIn, F>
where
    TIn: Send,
    F: FnOnce(TIn, TIn) -> TIn + Send + Copy + Sync,
{
    threadpool: ThreadPool,
    replicas: usize,
    f: F,
    phantom: PhantomData<TIn>,
}
impl<TIn, F> Reduce<TIn, F>
where
    TIn: Send + Clone + 'static,
    F: FnOnce(TIn, TIn) -> TIn + Send + Copy + Sync,
{
    /// Create a new Reduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    ///
    /// # Examples
    ///
    /// Given a collection of vectors of integers, for each vector
    /// compute the summation of its elements.
    ///
    /// ```
    ///
    /// use ppl::{prelude::*, templates::misc::{SourceIter, SinkVec}, templates::map::Reduce};
    ///
    /// // Create the vector of the elements that will be emitted by the Source node.
    ///  // vec![(key,value)]
    /// let vector = vec![
    ///     vec![1; 10],
    ///     vec![1; 10],
    ///     vec![1; 10],
    /// ];
    ///
    /// // Instantiate a new pipeline.
    /// let pipe = pipeline![
    ///     SourceIter::build(vector.into_iter()),
    ///     Reduce::build(4, |a, b| -> i32 {
    ///         a + b
    ///     }),
    ///     SinkVec::build()
    /// ];
    ///
    /// // Start the pipeline and wait for the results.
    /// let res: Vec<i32> = pipe.start_and_wait_end().unwrap();
    ///
    /// // Collect a results for each vector emitted by the Source. In our case we had 3 vectors.
    /// assert_eq!(res.len(), 3);
    ///
    /// // As for each vector emitted we had only one key, we obtain only one result tuple
    /// // for vector.
    /// for el in res {
    ///     assert_eq!(el, 10);
    /// }
    pub fn build<TInIter>(n_worker: usize, f: F) -> impl InOut<TInIter, TIn>
    where
        TInIter: IntoIterator<Item = TIn>,
    {
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: 1,
            f,
            phantom: PhantomData,
        }
    }
    /// Create a new Reduce node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `n_replicas` - Number of replicas.
    /// * `f` - Function to apply to each element of the input.
    /// # Panics
    /// Panics if n_replicas is 0.
    /// # Remarks
    /// The replicas are created by cloning the Reduce node.
    /// This mean that 4 replicas of a Reduce node with 2 workers each
    /// will result in the usage of 8 threads.
    pub fn build_with_replicas<TInIter>(
        n_worker: usize,
        n_replicas: usize,
        f: F,
    ) -> impl InOut<TInIter, TIn>
    where
        TInIter: IntoIterator<Item = TIn>,
    {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: n_replicas,
            f,
            phantom: PhantomData,
        }
    }
}
impl<TIn, TInIter, F> InOut<TInIter, TIn> for Reduce<TIn, F>
where
    TIn: Send + Clone + 'static,
    TInIter: IntoIterator<Item = TIn>,
    F: FnOnce(TIn, TIn) -> TIn + Send + Copy + Sync,
{
    fn run(&mut self, input: TInIter) -> Option<TIn> {
        let res: TIn = self.threadpool.par_reduce(input, self.f);
        Some(res)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
}

/// Ordered Reduce
///
/// In this Reduce, the elements are processed in the same order as they are received.
/// The order of the output is the same as the order of the input.
#[derive(Clone)]
pub struct OrderedReduce<TIn, F>
where
    TIn: Send,
    F: FnOnce(TIn, TIn) -> TIn + Send + Copy + Sync,
{
    threadpool: ThreadPool,
    replicas: usize,
    f: F,
    phantom: PhantomData<TIn>,
}
impl<TIn, F> OrderedReduce<TIn, F>
where
    TIn: Send + Clone + 'static,
    F: FnOnce(TIn, TIn) -> TIn + Send + Copy + Sync,
{
    /// Create a new OrderedReduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    pub fn build<TInIter>(n_worker: usize, f: F) -> impl InOut<TInIter, TIn>
    where
        TInIter: IntoIterator<Item = TIn>,
    {
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: 1,
            f,
            phantom: PhantomData,
        }
    }
    /// Create a new OrderedReduce node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `n_replicas` - Number of replicas.
    /// * `f` - Function to apply to each element of the input.
    /// # Panics
    /// Panics if n_replicas is 0.
    /// # Remarks
    /// The replicas are created by cloning the OrderedReduce node.
    /// This mean that 4 replicas of an OrderedReduce node with 2 workers each
    /// will result in the usage of 8 threads.
    ///
    /// # Examples
    ///
    /// Given a collection of vectors of integers, for each vector
    /// compute the summation of its elements.
    /// In this example we mantain the order of the input in the output.
    ///
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, OrderedSinkVec}, templates::map::OrderedReduce};
    ///
    /// // Create the vector of the elements that will be emitted by the Source node.
    ///  // vec![(key,value)]
    /// let vector = vec![
    ///     vec![1; 10],
    ///     vec![1; 100],
    ///     vec![1; 1000],
    /// ];
    ///
    /// // Instantiate a new pipeline.
    /// let pipe = pipeline![
    ///     SourceIter::build(vector.into_iter()),
    ///     OrderedReduce::build_with_replicas(2, 4, |a, b| -> i32 {
    ///         a + b
    ///     }),
    ///     OrderedSinkVec::build()
    /// ];
    ///
    /// // Start the pipeline and wait for the results.
    /// let res: Vec<i32> = pipe.start_and_wait_end().unwrap();
    ///
    /// // Collect a results for each vector emitted by the Source. In our case we had 3 vectors.
    /// assert_eq!(res.len(), 3);
    /// assert_eq!(res[0], 10);
    /// assert_eq!(res[1], 100);
    /// assert_eq!(res[2], 1000);
    pub fn build_with_replicas<TInIter>(
        n_worker: usize,
        n_replicas: usize,
        f: F,
    ) -> impl InOut<TInIter, TIn>
    where
        TInIter: IntoIterator<Item = TIn>,
    {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: n_replicas,
            f,
            phantom: PhantomData,
        }
    }
}
impl<TIn, TInIter, F> InOut<TInIter, TIn> for OrderedReduce<TIn, F>
where
    TIn: Send + Clone + 'static,
    TInIter: IntoIterator<Item = TIn>,
    F: FnOnce(TIn, TIn) -> TIn + Send + Copy + Sync,
{
    fn run(&mut self, input: TInIter) -> Option<TIn> {
        let res: TIn = self.threadpool.par_reduce(input, self.f);
        Some(res)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

/// Map Reduce
///
/// Nodes of this type are composed of a Map and a Reduce.
/// The Map is applied to each element of the input, and the Reduce is applied to the output of the Map.
#[derive(Clone)]
pub struct MapReduce<TIn, TMapOut, TKey, FMap, FReduce>
where
    TIn: Send,
    TMapOut: Send,
    TKey: Send,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TMapOut, TMapOut) -> TMapOut,
{
    threadpool: ThreadPool,
    replicas: usize,
    f_map: FMap,
    f_reduce: FReduce,
    phantom: PhantomData<(TIn, TMapOut, TKey)>,
}
impl<TIn, TMapOut, TKey, FMap, FReduce> MapReduce<TIn, TMapOut, TKey, FMap, FReduce>
where
    TIn: Send + Clone + 'static,
    TMapOut: Send + Clone + 'static,
    TKey: Send + Clone + 'static + Ord,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TMapOut, TMapOut) -> TMapOut + Send + Copy + Sync,
{
    /// Create a new MapReduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f_map` - Function to apply to each element of the input.
    /// * `f_reduce` - Function to apply to the output of the Map.
    ///
    /// # Examples
    ///
    /// Given a vector of vectors, each one containing a set of numbers,
    /// compute for each vector the square value of each of its elements.
    /// Furthermore, compute for each vector the summation of all its elements.
    ///
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, SinkVec}, templates::map::MapReduce};
    ///
    /// let mut counter = 1.0;
    /// let mut set = Vec::new();
    ///
    /// for i in 0..100000 {
    ///     let mut vector = Vec::new();
    ///     for _i in 0..10 {
    ///         vector.push((i, counter));
    ///         counter += 1.0;
    ///     }
    ///     counter = 1.0;
    ///     set.push(vector);
    /// }
    /// // Instantiate a new pipeline.
    /// let pipe = pipeline![
    ///     SourceIter::build(set.into_iter()),
    ///     MapReduce::build(8,
    ///     |el: (usize, f64)| -> (usize, f64) {
    ///         (el.0, el.1 * el.1)
    ///    },
    ///     |a, b| {
    ///         a + b
    ///     }),
    ///     SinkVec::build()
    /// ];
    ///
    /// let res: Vec<Vec<(usize, f64)>> = pipe.start_and_wait_end().unwrap();
    /// ```
    pub fn build<TInIter, TOutIter>(
        n_worker: usize,
        f_map: FMap,
        f_reduce: FReduce,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TMapOut)>,
    {
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: 1,
            f_map,
            f_reduce,
            phantom: PhantomData,
        }
    }
    /// Create a new MapReduce node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `n_replicas` - Number of replicas.
    /// * `f_map` - Function to apply to each element of the input.
    /// * `f_reduce` - Function to apply to the output of the Map.
    /// # Panics
    /// Panics if n_replicas is 0.
    /// # Remarks
    /// The replicas are created by cloning the MapReduce node.
    /// This mean that 4 replicas of a MapReduce node with 2 workers each
    /// will result in the usage of 8 threads.
    pub fn build_with_replicas<TInIter, TOutIter>(
        n_worker: usize,
        f_map: FMap,
        f_reduce: FReduce,
        n_replicas: usize,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TMapOut)>,
    {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: n_replicas,
            f_map,
            f_reduce,
            phantom: PhantomData,
        }
    }
}
impl<TIn, TMapOut, TInIter, TKey, TOutIter, FMap, FReduce> InOut<TInIter, TOutIter>
    for MapReduce<TIn, TMapOut, TKey, FMap, FReduce>
where
    TIn: Send + Clone + 'static,
    TMapOut: Send + Clone + 'static,
    TInIter: IntoIterator<Item = TIn>,
    TKey: Send + Clone + 'static + Ord,
    TOutIter: FromIterator<(TKey, TMapOut)>,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TMapOut, TMapOut) -> TMapOut + Send + Copy + Sync,
{
    fn run(&mut self, input: TInIter) -> Option<TOutIter> {
        let res: TOutIter = self
            .threadpool
            .par_map_reduce(input, self.f_map, self.f_reduce)
            .collect();
        Some(res)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
}

/// Ordered MapReduce
///
/// Nodes of this type are composed of a Map and a Reduce.
/// The Map is applied to each element of the input, and the Reduce is applied to the output of the Map.
/// The order of the input is preserved in the output.
/// This node is slower than MapReduce but preserves the order of the input.
/// This node is useful when the order of the input is important.
#[derive(Clone)]
pub struct OrderedMapReduce<TIn, TMapOut, TKey, FMap, FReduce>
where
    TIn: Send,
    TMapOut: Send,
    TKey: Send,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TMapOut, TMapOut) -> TMapOut + Send + Copy,
{
    threadpool: ThreadPool,
    replicas: usize,
    f_map: FMap,
    f_reduce: FReduce,
    phantom: PhantomData<(TIn, TMapOut, TKey)>,
}
impl<TIn, TMapOut, TKey, FMap, FReduce> OrderedMapReduce<TIn, TMapOut, TKey, FMap, FReduce>
where
    TIn: Send + Clone + 'static,
    TMapOut: Send + Clone + 'static,
    TKey: Send + Clone + 'static + Ord,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TMapOut, TMapOut) -> TMapOut + Send + Copy + Sync,
{
    /// Create a new OrderedMapReduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f_map` - Function to apply to each element of the input.
    /// * `f_reduce` - Function to apply to the output of the Map.
    pub fn build<TInIter, TOutIter>(
        n_worker: usize,
        f_map: FMap,
        f_reduce: FReduce,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TMapOut)>,
    {
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: 1,
            f_map,
            f_reduce,
            phantom: PhantomData,
        }
    }
    /// Create a new OrderedMapReduce node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `n_replicas` - Number of replicas.
    /// * `f_map` - Function to apply to each element of the input.
    /// * `f_reduce` - Function to apply to the output of the Map.
    /// # Panics
    /// Panics if n_replicas is 0.
    /// # Remarks
    /// The replicas are created by cloning the OrderedMapReduce node.
    /// This mean that 4 replicas of an OrderedMapReduce node with 2 workers each
    /// will result in the usage of 8 threads.
    ///
    /// # Examples
    ///
    /// Given a vector of vectors, each one containing a set of numbers,
    /// compute for each vector the square value of each of its elements.
    /// Furthermore, compute for each vector the summation of all its elements.
    /// In this example we want mantain the order of the input in the output.
    /// ```
    /// use ppl::{prelude::*, templates::misc::{SourceIter, OrderedSinkVec}, templates::map::OrderedMapReduce};
    ///
    /// let mut counter = 1.0;
    /// let mut set = Vec::new();
    ///
    /// for i in 0..100000 {
    ///     let mut vector = Vec::new();
    ///     for _i in 0..10 {
    ///         vector.push((i, counter));
    ///         counter += 1.0;
    ///     }
    ///     counter = 1.0;
    ///     set.push(vector);
    /// }
    /// // Instantiate a new pipeline.
    /// let pipe = pipeline![
    ///     SourceIter::build(set.into_iter()),
    ///     OrderedMapReduce::build_with_replicas(2, 8,
    ///     |el: (usize, f64)| -> (usize, f64) {
    ///         (el.0, el.1 * el.1)
    ///    },
    ///     |a, b| {
    ///         a + b
    ///     }),
    ///     OrderedSinkVec::build()
    /// ];
    ///
    /// let res: Vec<Vec<(usize, f64)>> = pipe.start_and_wait_end().unwrap();
    ///
    /// // We check here also if the order of the input was preserved
    /// // in the output.
    /// for (check, vec) in res.into_iter().enumerate() {
    ///     assert_eq!(vec.len(), 1);
    ///     for el in vec {
    ///         assert_eq!(el, (check, 385.00));
    ///     }
    /// }
    /// ```
    pub fn build_with_replicas<TInIter, TOutIter>(
        n_worker: usize,
        n_replicas: usize,
        f_map: FMap,
        f_reduce: FReduce,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TMapOut)>,
    {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::with_capacity(n_worker),
            replicas: n_replicas,
            f_map,
            f_reduce,
            phantom: PhantomData,
        }
    }
}
impl<TIn, TMapOut, TInIter, TKey, TOutIter, FMap, FReduce> InOut<TInIter, TOutIter>
    for OrderedMapReduce<TIn, TMapOut, TKey, FMap, FReduce>
where
    TIn: Send + Clone + 'static,
    TMapOut: Send + Clone + 'static,
    TInIter: IntoIterator<Item = TIn>,
    TKey: Send + Clone + 'static + Ord,
    TOutIter: FromIterator<(TKey, TMapOut)>,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TMapOut, TMapOut) -> TMapOut + Send + Copy + Sync,
{
    fn run(&mut self, input: TInIter) -> Option<TOutIter> {
        let res: TOutIter = self
            .threadpool
            .par_map_reduce(input, self.f_map, self.f_reduce)
            .collect();
        Some(res)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::{
        prelude::*,
        templates::{
            map::{Map, OrderedMap, Reduce, FlatMap, MapReduce, OrderedMapReduce, OrderedReduce},
            misc::{OrderedSinkVec, SinkVec, SourceIter},
        },
    };

    fn square(x: f64) -> f64 {
        x * x
    }

    #[test]
    #[serial]
    fn simple_map() {
        let mut counter = 1.0;
        let numbers: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let mut vector = Vec::new();

        for _i in 0..1000 {
            vector.push(numbers.clone());
        }

        let pipe = pipeline![
            SourceIter::build(vector.into_iter()),
            Map::build(4, |el: f64| square(el)),
            SinkVec::build()
        ];

        let res: Vec<Vec<f64>> = pipe.start_and_wait_end().unwrap();

        for vec in res {
            for el in vec {
                assert_eq!(el.sqrt(), counter);
                counter += 1.0;
            }
            counter = 1.0;
        }

        unsafe {
            Orchestrator::delete_global_orchestrator();
        }
    }

    #[test]
    #[serial]
    fn simple_map_replicated() {
        let mut counter = 1.0;
        let numbers: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let mut vector = Vec::new();

        for _i in 0..1000 {
            vector.push(numbers.clone());
        }

        let pipe = pipeline![
            SourceIter::build(vector.into_iter()),
            Map::build_with_replicas(4, 2, |el: f64| square(el)),
            SinkVec::build()
        ];

        let res: Vec<Vec<f64>> = pipe.start_and_wait_end().unwrap();

        for vec in res {
            for el in vec {
                assert_eq!(el.sqrt(), counter);
                counter += 1.0;
            }
            counter = 1.0;
        }

        unsafe {
            Orchestrator::delete_global_orchestrator();
        }
    }

    #[test]
    #[serial]
    fn simple_ordered_map() {
        let mut counter = 1.0;
        let mut vector = Vec::new();

        for _i in 0..1000 {
            let mut numbers = Vec::new();
            for _i in 0..10 {
                numbers.push(counter);
                counter += 1.0;
            }
            vector.push(numbers);
        }

        let pipe = pipeline![
            SourceIter::build(vector.into_iter()),
            OrderedMap::build(4, |el: f64| square(el)),
            OrderedSinkVec::build()
        ];

        let res: Vec<Vec<f64>> = pipe.start_and_wait_end().unwrap();

        counter = 1.0;
        for vec in res {
            for el in vec {
                assert_eq!(el.sqrt(), counter);
                counter += 1.0;
            }
        }

        unsafe {
            Orchestrator::delete_global_orchestrator();
        }
    }

    #[test]
    #[serial]
    fn simple_ordered_map_replicated() {
        let mut counter = 1.0;
        let mut vector = Vec::new();

        for _i in 0..1000 {
            let mut numbers = Vec::new();
            for _i in 0..10 {
                numbers.push(counter);
                counter += 1.0;
            }
            vector.push(numbers);
        }

        let pipe = pipeline![
            SourceIter::build(vector.into_iter()),
            OrderedMap::build_with_replicas(4, 2, |el: f64| square(el)),
            OrderedSinkVec::build()
        ];

        let res: Vec<Vec<f64>> = pipe.start_and_wait_end().unwrap();

        counter = 1.0;
        for vec in res {
            for el in vec {
                assert_eq!(el.sqrt(), counter);
                counter += 1.0;
            }
        }

        unsafe {
            Orchestrator::delete_global_orchestrator();
        }
    }

    #[test]
    #[serial]
    fn summation() {
        let mut set = Vec::new();

        for _i in 0..1000 {
            let mut vector = Vec::new();
            for _i in 0..10 {
                vector.push(1);
            }
            set.push(vector);
        }

        let pipe = pipeline![
            SourceIter::build(set.into_iter()),
            Reduce::build(4, |a, b| -> i32 { a + b }),
            SinkVec::build()
        ];

        let res: Vec<i32> = pipe.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 1000);

        for el in res {
            assert_eq!(el, 10)
        }

        unsafe {
            Orchestrator::delete_global_orchestrator();
        }
    }

    #[test]
    #[serial]
    fn summation_ordered() {
        let set = vec![vec![1; 10], vec![1; 100], vec![1; 1000]];
        let pipe = pipeline![
            SourceIter::build(set.into_iter()),
            OrderedReduce::build_with_replicas(2, 4, |a, b| -> i32 { a + b }),
            OrderedSinkVec::build()
        ];

        let res: Vec<i32> = pipe.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 3);
        assert_eq!(res[0], 10);
        assert_eq!(res[1], 100);
        assert_eq!(res[2], 1000);

        unsafe {
            Orchestrator::delete_global_orchestrator();
        }
    }

    #[test]
    #[serial]
    fn summation_of_squares() {
        let mut counter = 1.0;
        let mut set = Vec::new();

        for i in 0..100000 {
            let mut vector = Vec::new();
            for _i in 0..10 {
                vector.push((i, counter));
                counter += 1.0;
            }
            counter = 1.0;
            set.push(vector);
        }

        let pipe = pipeline![
            SourceIter::build(set.into_iter()),
            MapReduce::build(
                8,
                |el: (usize, f64)| -> (usize, f64) { (el.0, el.1 * el.1) },
                |a, b| { a + b }
            ),
            SinkVec::build()
        ];

        let res: Vec<Vec<(usize, f64)>> = pipe.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 100000);

        for vec in res {
            assert_eq!(vec.len(), 1);
            for el in vec {
                assert_eq!(el.1, 385.00);
            }
        }

        unsafe {
            Orchestrator::delete_global_orchestrator();
        }
    }

    #[test]
    #[serial]
    fn summation_of_squares_ordered() {
        let mut counter = 1.0;
        let mut set = Vec::new();

        for i in 0..100000 {
            let mut vector = Vec::new();
            for _i in 0..10 {
                vector.push((i, counter));
                counter += 1.0;
            }
            counter = 1.0;
            set.push(vector);
        }

        let pipe = pipeline![
            SourceIter::build(set.into_iter()),
            OrderedMapReduce::build_with_replicas(
                2,
                4,
                |el: (usize, f64)| -> (usize, f64) { (el.0, el.1 * el.1) },
                |a, b| { a + b }
            ),
            OrderedSinkVec::build()
        ];

        let res: Vec<Vec<(usize, f64)>> = pipe.start_and_wait_end().unwrap();

        assert_eq!(res.len(), 100000);

        for (check, vec) in res.into_iter().enumerate() {
            assert_eq!(vec.len(), 1);
            for el in vec {
                assert_eq!(el, (check, 385.00));
            }
        }

        unsafe {
            Orchestrator::delete_global_orchestrator();
        }
    }

    #[test]
    #[serial]
    fn flat_map() {
        let a: Vec<Vec<u64>> = vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]];
        let mut vector = Vec::new();
    // Create the vector of vectors.
        for _i in 0..1000 {
            vector.push(a.clone());
        }
    // Instantiate a new Pipeline with a FlatMap operator.
        let pipe = pipeline![
            SourceIter::build(vector.into_iter()),
            FlatMap::build(4, |x: Vec<u64>| x.into_iter().map(|i| i + 1)),
            SinkVec::build()
        ];
    // Start the pipeline and collect the results.
        let mut res: Vec<Vec<u64>> = pipe.start_and_wait_end().unwrap();
        let check = res.pop().unwrap();
        assert_eq!(&check, &[2, 3, 4, 5, 6, 7, 8, 9]);
    }
}
