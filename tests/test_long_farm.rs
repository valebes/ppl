/*
    A pipeline of farm.
    There are generated multiple sequence of i from 1 to 20.
    The first stage is a farm that return simply the i-th number received.
    The second stage is a farm that compute the i-th Fibonacci number.
    The third stage is a farm that subtract 1 to the i-th Fibonacci number.
*/

use ppl::prelude::*;

// Fibonacci function
pub fn fibonacci_recursive(n: u64) -> u64 {
    match n {
        0 => panic!("zero is not a right argument to fibonacci_recursive()!"),
        1 | 2 => 1,
        3 => 2,
        /*
        50    => 12586269025,
        */
        _ => fibonacci_recursive(n - 1) + fibonacci_recursive(n - 2),
    }
}

// Source of the farm, it generates multiple sequence of i from 1 to 20.
struct Source {
    streamlen: usize,
}
impl Out<u64> for Source {
    fn run(&mut self) -> Option<u64> {
        let mut ret = None;
        if self.streamlen > 0 {
            ret = Some((self.streamlen as u64 % 20) + 1);
            self.streamlen -= 1;
        }
        ret
    }
}

// Stage that return simply the i-th number received.
#[derive(Clone)]
struct WorkerA {}
impl InOut<u64, u64> for WorkerA {
    fn run(&mut self, input: u64) -> Option<u64> {
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

// Stage that computes the i-th Fibonacci number.
#[derive(Clone)]
struct WorkerB {}
impl InOut<u64, u64> for WorkerB {
    fn run(&mut self, input: u64) -> Option<u64> {
        Some(fibonacci_recursive(input))
    }
    fn number_of_replicas(&self) -> usize {
        8
    }
}

// Stage that subtract 1 to the i-th Fibonacci number.
#[derive(Clone)]
struct WorkerC {}
impl InOut<u64, u64> for WorkerC {
    fn run(&mut self, input: u64) -> Option<u64> {
        Some(input - 1)
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

// Sink of the farm, it prints the i-th Fibonacci number.
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
fn test_long_farm() {
    env_logger::init();

    // Create a pipeline of farm.
    let mut p = parallel!(
        Source { streamlen: 500000 },
        WorkerA {},
        WorkerB {},
        WorkerC {},
        Sink { counter: 0 }
    );

    p.start();
    let res = p.wait_and_collect();
    assert_eq!(res.unwrap(), 500000);
}
