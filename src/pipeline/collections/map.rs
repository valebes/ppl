use std::marker::PhantomData;

use crate::{pipeline::inout_node::InOut, thread_pool::ThreadPool};

/// Map
/// 
#[derive(Clone)]
pub struct Map<TIn: Send, TOut: Send, F: FnOnce(TIn) -> TOut + Send + Copy> {
    threadpool: ThreadPool,
    f: F,
    phantom: PhantomData<(TIn, TOut)>,
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
}
