/*
   Fibonacci pipeline.
   Is generated a sequence of i from 1 to 45.
   Each worker of the farm compute the i-th
   Fibonacci number.
*/

use pspp::node::{out_node::{Out, OutNode}, inout_node::{InOut, InOutNode}, in_node::{In, InNode}};


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
        self.counter = self.counter + 1;
    }

    fn finalize(&mut self) -> Option<usize> {
        Some(self.counter)
    }
}

#[test]
fn fibonacci_pipe() {
    env_logger::init();

    let sink = InNode::new(3, Box::new(Sink { counter: 0 }), false).unwrap();
    let worker = InOutNode::new(2, Box::new(Worker {}), sink, false).unwrap();
    let source = OutNode::new(1, Box::new(Source {
        streamlen: 45,
        counter: 0,
    }), worker).unwrap();
    
}