/*
    A pipeline of farm.
    There are generated multiple sequence of i from 1 to 20.
    The first stage is a farm that return simply the i-th number received.
    The second stage is a farm that compute the i-th Fibonacci number.
    The third stage is a farm that subtract 1 to the i-th Fibonacci number.
*/

use ppl::prelude::*;

// Fibonacci function
pub fn fib(n: usize) -> usize {
    match n {
        0 | 1 => 1,
        _ => fib(n - 2) + fib(n - 1),
    }
}

// Source of the farm, it generates multiple sequence of i from 1 to 20.
struct Source {
    streamlen: usize,
}
impl Out<usize> for Source {
    fn run(&mut self) -> Option<usize> {
        let mut ret = None;
        if self.streamlen > 0 {
            ret = Some((self.streamlen % 20) + 1);
            self.streamlen -= 1;
        }
        ret
    }
}

// Stage that return simply the i-th number received.
#[derive(Clone)]
struct WorkerA {}
impl InOut<usize, usize> for WorkerA {
    fn run(&mut self, input: usize) -> Option<usize> {
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

// Stage that computes the i-th Fibonacci number.
#[derive(Clone)]
struct WorkerB {}
impl InOut<usize, usize> for WorkerB {
    fn run(&mut self, input: usize) -> Option<usize> {
        Some(fib(input))
    }
    fn number_of_replicas(&self) -> usize {
        8
    }
}

// Stage that subtract 1 to the i-th Fibonacci number.
#[derive(Clone)]
struct WorkerC {}
impl InOut<usize, usize> for WorkerC {
    fn run(&mut self, input: usize) -> Option<usize> {
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
impl In<usize, usize> for Sink {
    fn run(&mut self, input: usize) {
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
    let mut p = pipeline!(
        Source { streamlen: 500000 },
        WorkerA {},
        WorkerB {},
        WorkerC {},
        Sink { counter: 0 }
    );

    p.start();
    let res = p.wait_end();
    assert_eq!(res.unwrap(), 500000);
}
