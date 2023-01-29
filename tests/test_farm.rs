/*
   Fibonacci farm
   Is generated a sequence of i from 1 to 45.
   Each worker of the farm compute the i-th
   Fibonacci number.
*/

use pspp::{
    node::{
        in_node::{In, InNode},
        inout_node::{InOut, InOutNode},
        out_node::{Out, OutNode},
    },
    pipeline,
    pipeline::Pipeline,
    pipeline_propagate,
};

struct Source {
    streamlen: usize,
    counter: usize,
}
impl Out<i32> for Source {
    fn run(&mut self) -> Option<i32> {
        if self.counter < self.streamlen {
            self.counter = self.counter + 1;
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
struct WorkerA {}
impl InOut<i32, u64> for WorkerA {
    fn run(&mut self, input: i32) -> Option<u64> {
        Some(fibonacci_reccursive(input))
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

#[derive(Clone)]
struct WorkerB {}
impl InOut<u64, u64> for WorkerB {
    fn run(&mut self, input: u64) -> Option<u64> {
        Some(input * 5)
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

#[derive(Clone)]
struct WorkerC {}
impl InOut<u64, u64> for WorkerC {
    fn run(&mut self, input: u64) -> Option<u64> {
        Some(input / 5)
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

struct Sink {
    counter: usize,
}
impl In<u64, usize> for Sink {
    fn run(&mut self, input: u64) {
        println!("{}", input);
        self.counter = self.counter + 1;
    }

    fn finalize(&mut self) -> Option<usize> {
        println!("End");
        Some(self.counter)
    }
}

#[test]
fn fibonacci_farm() {
    env_logger::init();

    let p = pipeline![
        Box::new(Source {
            streamlen: 45,
            counter: 0
        }),
        Box::new(WorkerA {}),
        Box::new(WorkerB {}),
        Box::new(WorkerC {}),
        Box::new(Sink { counter: 0 })
    ];

    let res = p.collect();
    assert_eq!(res.unwrap(), 45);
}
