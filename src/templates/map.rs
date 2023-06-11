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

/// Reduce
///
/// In this Reduce, the elements are grouped by key and then reduced.
#[derive(Clone)]
pub struct Reduce<TIn, TKey, TReduce, F>
where
    TIn: Send,
    TKey: Send,
    TReduce: Send,
    F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy,
{
    threadpool: ThreadPool,
    replicas: usize,
    f: F,
    phantom: PhantomData<(TIn, TKey, TReduce)>,
}
impl<TIn, TKey, TReduce, F> Reduce<TIn, TKey, TReduce, F>
where
    TIn: Send + Clone + 'static,
    TKey: Send + Clone + 'static + Ord,
    TReduce: Send + Clone + 'static,
    F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy,
{
    /// Create a new Reduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    pub fn build<TInIter, TOutIter>(n_worker: usize, f: F) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TOutIter: FromIterator<(TKey, TReduce)>,
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
    pub fn build_with_replicas<TInIter, TOutIter>(
        n_worker: usize,
        n_replicas: usize,
        f: F,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TOutIter: FromIterator<(TKey, TReduce)>,
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
impl<TIn, TInIter, TKey, TReduce, TOutIter, F> InOut<TInIter, TOutIter>
    for Reduce<TIn, TKey, TReduce, F>
where
    TIn: Send + Clone + 'static,
    TInIter: IntoIterator<Item = (TKey, TIn)>,
    TKey: Send + Clone + 'static + Ord,
    TReduce: Send + Clone + 'static,
    TOutIter: FromIterator<(TKey, TReduce)>,
    F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy,
{
    fn run(&mut self, input: TInIter) -> Option<TOutIter> {
        let res: TOutIter = self.threadpool.par_reduce(input, self.f).collect();
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
pub struct OrderedReduce<TIn, TKey, TReduce, F>
where
    TIn: Send,
    TKey: Send,
    TReduce: Send,
    F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy,
{
    threadpool: ThreadPool,
    replicas: usize,
    f: F,
    phantom: PhantomData<(TIn, TKey, TReduce)>,
}
impl<TIn, TKey, TReduce, F> OrderedReduce<TIn, TKey, TReduce, F>
where
    TIn: Send + Clone + 'static,
    TKey: Send + Clone + 'static + Ord,
    TReduce: Send + Clone + 'static,
    F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy,
{
    /// Create a new OrderedReduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    pub fn build<TInIter, TOutIter>(n_worker: usize, f: F) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TOutIter: FromIterator<(TKey, TReduce)>,
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
    pub fn build_with_replicas<TInIter, TOutIter>(
        n_worker: usize,
        n_replicas: usize,
        f: F,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TOutIter: FromIterator<(TKey, TReduce)>,
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
impl<TIn, TInIter, TKey, TReduce, TOutIter, F> InOut<TInIter, TOutIter>
    for OrderedReduce<TIn, TKey, TReduce, F>
where
    TIn: Send + Clone + 'static,
    TInIter: IntoIterator<Item = (TKey, TIn)>,
    TKey: Send + Clone + 'static + Ord,
    TReduce: Send + Clone + 'static,
    TOutIter: FromIterator<(TKey, TReduce)>,
    F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy,
{
    fn run(&mut self, input: TInIter) -> Option<TOutIter> {
        let res: TOutIter = self.threadpool.par_reduce(input, self.f).collect();
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
pub struct MapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
where
    TIn: Send,
    TMapOut: Send,
    TKey: Send,
    TReduce: Send,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy,
{
    threadpool: ThreadPool,
    replicas: usize,
    f_map: FMap,
    f_reduce: FReduce,
    phantom: PhantomData<(TIn, TMapOut, TKey, TReduce)>,
}
impl<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
    MapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
where
    TIn: Send + Clone + 'static,
    TMapOut: Send + Clone + 'static,
    TKey: Send + Clone + 'static + Ord,
    TReduce: Send + Clone + 'static,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy,
{
    /// Create a new MapReduce node.
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
        TOutIter: FromIterator<(TKey, TReduce)>,
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
    pub fn build_with_replicas<TInIter, TOutIter>(
        n_worker: usize,
        f_map: FMap,
        f_reduce: FReduce,
        n_replicas: usize,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TReduce)>,
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
impl<TIn, TMapOut, TInIter, TKey, TReduce, TOutIter, FMap, FReduce> InOut<TInIter, TOutIter>
    for MapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
where
    TIn: Send + Clone + 'static,
    TMapOut: Send + Clone + 'static,
    TInIter: IntoIterator<Item = TIn>,
    TKey: Send + Clone + 'static + Ord,
    TReduce: Send + Clone + 'static,
    TOutIter: FromIterator<(TKey, TReduce)>,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy,
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

/// Ordered Map Reduce
///
/// Nodes of this type are composed of a Map and a Reduce.
/// The Map is applied to each element of the input, and the Reduce is applied to the output of the Map.
/// The order of the input is preserved in the output.
/// This node is slower than MapReduce but preserves the order of the input.
/// This node is useful when the order of the input is important.
#[derive(Clone)]
pub struct OrderedMapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
where
    TIn: Send,
    TMapOut: Send,
    TKey: Send,
    TReduce: Send,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy,
{
    threadpool: ThreadPool,
    replicas: usize,
    f_map: FMap,
    f_reduce: FReduce,
    phantom: PhantomData<(TIn, TMapOut, TKey, TReduce)>,
}
impl<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
    OrderedMapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
where
    TIn: Send + Clone + 'static,
    TMapOut: Send + Clone + 'static,
    TKey: Send + Clone + 'static + Ord,
    TReduce: Send + Clone + 'static,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy,
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
        TOutIter: FromIterator<(TKey, TReduce)>,
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
    pub fn build_with_replicas<TInIter, TOutIter>(
        n_worker: usize,
        n_replicas: usize,
        f_map: FMap,
        f_reduce: FReduce,
    ) -> impl InOut<TInIter, TOutIter>
    where
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TReduce)>,
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
impl<TIn, TMapOut, TInIter, TKey, TReduce, TOutIter, FMap, FReduce> InOut<TInIter, TOutIter>
    for OrderedMapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
where
    TIn: Send + Clone + 'static,
    TMapOut: Send + Clone + 'static,
    TInIter: IntoIterator<Item = TIn>,
    TKey: Send + Clone + 'static + Ord,
    TReduce: Send + Clone + 'static,
    TOutIter: FromIterator<(TKey, TReduce)>,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy,
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
