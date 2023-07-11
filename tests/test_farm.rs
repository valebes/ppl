/*
   Fibonacci farm
   Is generated a sequence of i from 1 to 45.
   Each worker of the farm compute the i-th
   Fibonacci number.
*/

use ppl::prelude::*;

// Fibonacci function
pub fn fibonacci_recursive(n: u64) -> u64 {
    match n {
        0 => panic!("zero is not a right argument to fibonacci_recursive()!"),
        1 | 2 => 1,
        3 => 2,
        _ => fibonacci_recursive(n - 1) + fibonacci_recursive(n - 2),
    }
}

// Source of the farm, it generates a sequence of i from 1 to 45.
// When the stream equals to 0, the source returns None and the farm ends.
struct Source {
    streamlen: usize,
}
impl Out<u64> for Source {
    fn run(&mut self) -> Option<u64> {
        let mut ret = None;
        if self.streamlen > 0 {
            ret = Some(self.streamlen as u64);
            self.streamlen -= 1;
        }
        ret
    }
}

// Worker of the farm, it computes the i-th Fibonacci number.
#[derive(Clone)]
struct WorkerA {}
impl InOut<u64, u64> for WorkerA {
    fn run(&mut self, input: u64) -> Option<u64> {
        Some(fibonacci_recursive(input))
    }
    fn number_of_replicas(&self) -> usize {
        8
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
fn test_farm() {
    env_logger::init();

    let mut p = pipeline![Source { streamlen: 45 }, WorkerA {}, Sink { counter: 0 }];
    p.start();
    let res = p.wait_end();
    assert_eq!(res.unwrap(), 45);
}
