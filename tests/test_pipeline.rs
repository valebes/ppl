/*
   Fibonacci pipeline.
   Is generated a sequence of i from 1 to 45.
   Each worker of the farm compute the i-th
   Fibonacci number.
*/

use pspp::core::orchestrator::get_global_orchestrator;
use pspp::{
    pipeline::{
        in_node::{In, InNode},
        inout_node::{InOut, InOutNode},
        out_node::{Out, OutNode},
    },
    parallel, propagate,
    pspp::Parallel,
};

struct Source {
    streamlen: usize,
    counter: usize,
}
impl Out<i32> for Source {
    fn run(&mut self) -> Option<i32> {
        if self.counter < self.streamlen {
            self.counter += 1;
            Some((self.counter).try_into().unwrap())
        } else {
            None
        }
    }
}

pub fn fibonacci_reccursive(n: i32) -> u64 {
    if n < 0 {
        panic!("{} is negative!", n);
    }
    match n {
        0 => panic!("zero is not a right argument to fibonacci_reccursive()!"),
        1 | 2 => 1,
        3 => 2,
        /*
        50    => 12586269025,
        */
        _ => fibonacci_reccursive(n - 1) + fibonacci_reccursive(n - 2),
    }
}

#[derive(Clone)]
struct Worker {}
impl InOut<i32, u64> for Worker {
    fn run(&mut self, input: i32) -> Option<u64> {
        Some(fibonacci_reccursive(input))
    }
}

struct Sink {
    counter: usize,
}
impl In<u64, usize> for Sink {
    fn run(&mut self, input: u64) {
        println!("{}", input);
        self.counter += 1;
    }

    fn finalize(self) -> Option<usize> {
        println!("End");
        Some(self.counter)
    }
}

#[test]
fn fibonacci_pipe() {
    env_logger::init();

    let mut p = parallel![
        Source {
            streamlen: 20,
            counter: 0
        },
        Worker {},
        Sink { counter: 0 }
    ];

    p.start();
    let res = p.wait_and_collect();
    assert_eq!(res.unwrap(), 20);
}
