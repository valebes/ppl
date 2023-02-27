/*
   Fibonacci farm
   Is generated a sequence of i from 1 to 45.
   Each worker of the farm compute the i-th
   Fibonacci number.
*/

use std::sync::Arc;

use pspp::{
    node::{
        in_node::{In, InNode},
        inout_node::{InOut, InOutNode},
        out_node::{Out, OutNode},
    },
    parallel, propagate,
    pspp::Parallel,  thread_pool::ThreadPool,
};

struct Source {
    streamlen: usize,
    counter: usize,
}
impl Out<Vec<i32>> for Source {
    fn run(&mut self) -> Option<Vec<i32>> {
        if self.counter < self.streamlen {
            self.counter = self.counter + 1;
            Some(vec![2i32 ;100])
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct Map {
    threadpool: ThreadPool,
}
impl InOut<Vec<i32>, Vec<String>> for Map {
    fn run(&mut self, input: Vec<i32>) -> Option<Vec<String>> {
        let res: Vec<String> = self.threadpool.par_map(input, |el| -> String {String::from("Hello from: ".to_string() + &el.to_string())}).collect();
        Some(res)
    }
    fn number_of_replicas(&self) -> usize {
        4
    }

}

struct Sink {
    counter: usize,
}
impl In<Vec<String>, usize> for Sink {
    fn run(&mut self, input: Vec<String>) {
        self.counter = self.counter + 1;
    }

    fn finalize(self) -> Option<usize> {
        println!("End");
        Some(self.counter)
    }
}

#[test]
fn test_map() {
    env_logger::init();

    let mut p = parallel![
        Source {
            streamlen: 45,
            counter: 0
        },
       Map {threadpool: ThreadPool::new(2, false) },
       Sink { counter: 0 }
    ];

    p.start();
    let res = p.wait_and_collect();
    assert_eq!(res.unwrap(), 45);
}
