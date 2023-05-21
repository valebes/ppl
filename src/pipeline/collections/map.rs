use std::marker::PhantomData;

use crate::{
    pipeline::{self, inout_node::InOut},
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
