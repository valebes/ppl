/*
   Fibonacci farm
   Is generated a sequence of i from 1 to 45.
   Each worker of the farm compute the i-th
   Fibonacci number.
*/

use ppl::prelude::*;

// Fibonacci function
pub fn fib(n: usize) -> usize {
    match n {
        0 | 1 => 1,
        _ => fib(n - 2) + fib(n - 1),
    }
}

// Source of the farm, it generates a sequence of i from 1 to 45.
// When the stream equals to 0, the source returns None and the farm ends.
struct Source {
    streamlen: usize,
}
impl Out<usize> for Source {
    fn run(&mut self) -> Option<usize> {
        let mut ret = None;
        if self.streamlen > 0 {
            ret = Some(self.streamlen);
            self.streamlen -= 1;
        }
        ret
    }
}

// Worker of the farm, it computes the i-th Fibonacci number.
#[derive(Clone)]
struct WorkerA {}
impl InOut<usize, usize> for WorkerA {
    fn run(&mut self, input: usize) -> Option<usize> {
        Some(fib(input))
    }
    fn number_of_replicas(&self) -> usize {
        8
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
fn test_farm() {
    env_logger::init();

    let mut p = pipeline![Source { streamlen: 45 }, WorkerA {}, Sink { counter: 0 }];
    p.start();
    let res = p.wait_end();
    assert_eq!(res.unwrap(), 45);
}
