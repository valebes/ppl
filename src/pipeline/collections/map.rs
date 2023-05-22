use std::marker::PhantomData;

use crate::{
    pipeline::{inout_node::InOut},
    thread_pool::ThreadPool,
};

/// Map
#[derive(Clone)]
pub struct Map<TIn: Send, TOut: Send, F: FnOnce(TIn) -> TOut + Send + Copy> {
    threadpool: ThreadPool,
    f: F,
    phantom: PhantomData<(TIn, TOut)>,
    replicas: usize,
}
impl<TIn: Send + Clone, TOut: Send + Clone + 'static, F: FnOnce(TIn) -> TOut + Send + Copy>
    Map<TIn, TOut, F>
{
    /// Create a new Map node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    pub fn build<TInIter: IntoIterator<Item = TIn>, TOutIter: FromIterator<TOut>>(
        n_worker: usize,
        f: F,
    ) -> impl InOut<TInIter, TOutIter> {
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f,
            phantom: PhantomData,
            replicas: 1,
        }
    }

    /// Create a new Map node with n_replicas replicas.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    /// * `n_replicas` - Number of replicas.
    /// # Panics
    /// Panics if n_replicas is 0.
    /// # Remarks
    /// The replicas are created by cloning the Map node.
    pub fn build_with_replicas<TInIter: IntoIterator<Item = TIn>, TOutIter: FromIterator<TOut>>(
        n_worker: usize,
        f: F,
        n_replicas: usize,
    ) -> impl InOut<TInIter, TOutIter> {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f,
            phantom: PhantomData,
            replicas: n_replicas,
        }
    }
}
impl<
        TIn: Send + Clone,
        TInIter: IntoIterator<Item = TIn>,
        TOut: Send + Clone + 'static,
        TOutIter: FromIterator<TOut>,
        F: FnOnce(TIn) -> TOut + Send + Copy,
    > InOut<TInIter, TOutIter> for Map<TIn, TOut, F>
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
/// In this Map, the elements are processed in the same order as they are received.
#[derive(Clone)]
pub struct OrderedMap<TIn: Send, TOut: Send, F: FnOnce(TIn) -> TOut + Send + Copy> {
    threadpool: ThreadPool,
    f: F,
    phantom: PhantomData<(TIn, TOut)>,
    replicas: usize,
}
impl<TIn: Send + Clone, TOut: Send + Clone + 'static, F: FnOnce(TIn) -> TOut + Send + Copy>
    OrderedMap<TIn, TOut, F>
{
    /// Create a new OrderedMap node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    pub fn build<TInIter: IntoIterator<Item = TIn>, TOutIter: FromIterator<TOut>>(
        n_worker: usize,
        f: F,
    ) -> impl InOut<TInIter, TOutIter> {
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f,
            phantom: PhantomData,
            replicas: 1,
        }
    }
    /// Create a new OrderedMap node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    /// * `n_replicas` - Number of replicas.
    /// # Panics
    /// Panics if n_replicas is 0.
    pub fn build_with_replicas<TInIter: IntoIterator<Item = TIn>, TOutIter: FromIterator<TOut>>(
        n_worker: usize,
        f: F,
        n_replicas: usize,
    ) -> impl InOut<TInIter, TOutIter> {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f,
            phantom: PhantomData,
            replicas: n_replicas,
        }
    }
}
impl<
        TIn: Send + Clone,
        TInIter: IntoIterator<Item = TIn>,
        TOut: Send + Clone + 'static,
        TOutIter: FromIterator<TOut>,
        F: FnOnce(TIn) -> TOut + Send + Copy,
    > InOut<TInIter, TOutIter> for OrderedMap<TIn, TOut, F>
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
/// In this Reduce, the elements are grouped by key and then reduced.
#[derive(Clone)]
pub struct Reduce<
    TIn: Send,
    TKey: Send,
    TReduce: Send,
    F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy,
> {
    threadpool: ThreadPool,
    f: F,
    phantom: PhantomData<(TIn, TKey, TReduce)>,
    replicas: usize,
}
impl<
        TIn: Send + Clone + Sync + 'static,
        TKey: Send + Clone + 'static + Sync + Ord,
        TReduce: Send + Clone + 'static + Sync,
        F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy + Sync,
    > Reduce<TIn, TKey, TReduce, F>
{   
    /// Create a new Reduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    pub fn build<
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TOutIter: FromIterator<(TKey, TReduce)>,
    >(
        n_worker: usize,
        f: F,
    ) -> impl InOut<TInIter, TOutIter> {
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f,
            phantom: PhantomData,
            replicas: 1,
        }
    }
    /// Create a new Reduce node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    /// * `n_replicas` - Number of replicas.
    /// # Panics
    /// Panics if n_replicas is 0.
    pub fn build_with_replicas<
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TOutIter: FromIterator<(TKey, TReduce)>,
    >(
        n_worker: usize,
        f: F,
        n_replicas: usize,
    ) -> impl InOut<TInIter, TOutIter> {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f,
            phantom: PhantomData,
            replicas: n_replicas,
        }
    }
}
impl<
        TIn: Send + Clone + Sync + 'static,
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TKey: Send + Clone + 'static + Sync + Ord,
        TReduce: Send + Clone + 'static + std::marker::Sync,
        TOutIter: FromIterator<(TKey, TReduce)>,
        F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy + Sync,
    > InOut<TInIter, TOutIter> for Reduce<TIn, TKey, TReduce, F>
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
/// In this Reduce, the elements are processed in the same order as they are received.
/// The order of the output is the same as the order of the input.
#[derive(Clone)]
pub struct OrderedReduce<
    TIn: Send,
    TKey: Send,
    TReduce: Send,
    F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy,
> {
    threadpool: ThreadPool,
    f: F,
    phantom: PhantomData<(TIn, TKey, TReduce)>,
    replicas: usize,
}
impl<
        TIn: Send + Clone + Sync + 'static,
        TKey: Send + Clone + 'static + Sync + Ord,
        TReduce: Send + Clone + 'static + Sync,
        F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy + Sync,
    > OrderedReduce<TIn, TKey, TReduce, F>
{
    /// Create a new OrderedReduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    pub fn build<
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TOutIter: FromIterator<(TKey, TReduce)>,
    >(
        n_worker: usize,
        f: F,
    ) -> impl InOut<TInIter, TOutIter> {
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f,
            phantom: PhantomData,
            replicas: 1,
        }
    }
    /// Create a new OrderedReduce node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f` - Function to apply to each element of the input.
    /// * `n_replicas` - Number of replicas.
    /// # Panics
    /// Panics if n_replicas is 0.
    pub fn build_with_replicas<
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TOutIter: FromIterator<(TKey, TReduce)>,
    >(
        n_worker: usize,
        f: F,
        n_replicas: usize,
    ) -> impl InOut<TInIter, TOutIter> {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f,
            phantom: PhantomData,
            replicas: n_replicas,
        }
    }
}
impl<
        TIn: Send + Clone + Sync + 'static,
        TInIter: IntoIterator<Item = (TKey, TIn)>,
        TKey: Send + Clone + 'static + Sync + Ord,
        TReduce: Send + Clone + 'static + std::marker::Sync,
        TOutIter: FromIterator<(TKey, TReduce)>,
        F: FnOnce(TKey, Vec<TIn>) -> (TKey, TReduce) + Send + Copy + Sync,
    > InOut<TInIter, TOutIter> for OrderedReduce<TIn, TKey, TReduce, F>
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
/// Nodes of this type are composed of a Map and a Reduce.
/// The Map is applied to each element of the input, and the Reduce is applied to the output of the Map.
#[derive(Clone)]
pub struct MapReduce<
    TIn: Send,
    TMapOut: Send,
    TKey: Send,
    TReduce: Send,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy,
> {
    threadpool: ThreadPool,
    f_map: FMap,
    f_reduce: FReduce,
    phantom: PhantomData<(TIn, TMapOut, TKey, TReduce)>,
    replicas: usize,
}
impl<
        TIn: Send + Clone + Sync + 'static,
        TMapOut: Send + Clone + Sync + 'static,
        TKey: Send + Clone + 'static + Sync + Ord,
        TReduce: Send + Clone + 'static + Sync,
        FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy + Sync,
        FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy + Sync,
    > MapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
{
    /// Create a new MapReduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f_map` - Function to apply to each element of the input.
    /// * `f_reduce` - Function to apply to the output of the Map.
    pub fn build<
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TReduce)>,
    >(
        n_worker: usize,
        f_map: FMap,
        f_reduce: FReduce,
    ) -> impl InOut<TInIter, TOutIter> {
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f_map,
            f_reduce,
            phantom: PhantomData,
            replicas: 1,
        }
    }
    /// Create a new MapReduce node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f_map` - Function to apply to each element of the input.
    /// * `f_reduce` - Function to apply to the output of the Map.
    /// * `n_replicas` - Number of replicas.
    /// # Panics
    /// Panics if n_replicas is 0.
    pub fn build_with_replicas<
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TReduce)>,
    >(
        n_worker: usize,
        f_map: FMap,
        f_reduce: FReduce,
        n_replicas: usize,
    ) -> impl InOut<TInIter, TOutIter> {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f_map,
            f_reduce,
            phantom: PhantomData,
            replicas: n_replicas,
        }
    }
}
impl<
        TIn: Send + Clone + Sync + 'static,
        TMapOut: Send + Clone + Sync + 'static,
        TInIter: IntoIterator<Item = TIn>,
        TKey: Send + Clone + 'static + Sync + Ord,
        TReduce: Send + Clone + 'static + Sync,
        TOutIter: FromIterator<(TKey, TReduce)>,
        FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy + Sync,
        FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy + Sync,
    > InOut<TInIter, TOutIter> for MapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
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
/// Nodes of this type are composed of a Map and a Reduce.
/// The Map is applied to each element of the input, and the Reduce is applied to the output of the Map.
/// The order of the input is preserved in the output.
/// This node is slower than MapReduce but preserves the order of the input.
/// This node is useful when the order of the input is important.
#[derive(Clone)]
pub struct OrderedMapReduce<
    TIn: Send,
    TMapOut: Send,
    TKey: Send,
    TReduce: Send,
    FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy,
    FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy,
> {
    threadpool: ThreadPool,
    f_map: FMap,
    f_reduce: FReduce,
    phantom: PhantomData<(TIn, TMapOut, TKey, TReduce)>,
    replicas: usize,
}
impl<
        TIn: Send + Clone + Sync + 'static,
        TMapOut: Send + Clone + Sync + 'static,
        TKey: Send + Clone + 'static + Sync + Ord,
        TReduce: Send + Clone + 'static + Sync,
        FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy + Sync,
        FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy + Sync,
    > OrderedMapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
{
    /// Create a new OrderedMapReduce node.
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f_map` - Function to apply to each element of the input.
    /// * `f_reduce` - Function to apply to the output of the Map.
    pub fn build<
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TReduce)>,
    >(
        n_worker: usize,
        f_map: FMap,
        f_reduce: FReduce,
    ) -> impl InOut<TInIter, TOutIter> {
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f_map,
            f_reduce,
            phantom: PhantomData,
            replicas: 1,
        }
    }
    /// Create a new OrderedMapReduce node with n_replicas replicas.
    ///
    /// # Arguments
    /// * `n_worker` - Number of worker threads.
    /// * `f_map` - Function to apply to each element of the input.
    /// * `f_reduce` - Function to apply to the output of the Map.
    /// * `n_replicas` - Number of replicas.
    /// # Panics
    /// Panics if n_replicas is 0.
    pub fn build_with_replicas<
        TInIter: IntoIterator<Item = TIn>,
        TOutIter: FromIterator<(TKey, TReduce)>,
    >(
        n_worker: usize,
        f_map: FMap,
        f_reduce: FReduce,
        n_replicas: usize,
    ) -> impl InOut<TInIter, TOutIter> {
        assert!(n_replicas > 0);
        Self {
            threadpool: ThreadPool::new_with_global_registry(n_worker),
            f_map,
            f_reduce,
            phantom: PhantomData,
            replicas: n_replicas,
        }
    }
}
impl<
        TIn: Send + Clone + Sync + 'static,
        TMapOut: Send + Clone + Sync + 'static,
        TInIter: IntoIterator<Item = TIn>,
        TKey: Send + Clone + 'static + Sync + Ord,
        TReduce: Send + Clone + 'static + Sync,
        TOutIter: FromIterator<(TKey, TReduce)>,
        FMap: FnOnce(TIn) -> (TKey, TMapOut) + Send + Copy + Sync,
        FReduce: FnOnce(TKey, Vec<TMapOut>) -> (TKey, TReduce) + Send + Copy + Sync,
    > InOut<TInIter, TOutIter> for OrderedMapReduce<TIn, TMapOut, TKey, TReduce, FMap, FReduce>
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