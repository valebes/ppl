use crate::{thread_pool::ThreadPool, node::inout_node::InOut};


#[derive(Clone)]
struct Map {
    threadpool: ThreadPool,
}
impl InOut<Vec<i32>, Vec<String>> for Map {
    fn run(&mut self, input: Vec<i32>) -> Option<Vec<String>> {
        let res: Vec<String> = self.threadpool.par_map(input, |el| -> String {String::from("Hello from: ".to_string() + &el.to_string())}).collect();
        Some(res)
    }

}