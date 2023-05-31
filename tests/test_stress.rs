/*
    Stress test
*/

use log::error;
use ppl::{
    collections::misc::{Sequential, SinkVec, SourceIter},
    prelude::*,
};

struct Source {
    streamlen: usize,
    counter: usize,
}
impl Out<usize> for Source {
    fn run(&mut self) -> Option<usize> {
        if self.counter < self.streamlen {
            self.counter += 1;
            error!("[SOURCE] send: {}", self.counter);
            Some(self.counter)
        } else {
            None
        }
    }
}

pub fn fibonacci_recursive(n: usize) -> usize {
    match n {
        0 => panic!("zero is not a right argument to fibonacci_reccursive()!"),
        1 | 2 => 1,
        3 => 2,
        /*
        50    => 12586269025,
        */
        _ => fibonacci_recursive(n - 1) + fibonacci_recursive(n - 2),
    }
}

#[derive(Clone)]
struct Worker {}
impl InOut<usize, usize> for Worker {
    fn run(&mut self, input: usize) -> Option<usize> {
        error!("[WORKER] received: {}", input);
        Some(fibonacci_recursive(input))
    }
}

struct Sink {
    counter: usize,
}
impl In<usize, usize> for Sink {
    fn run(&mut self, input: usize) {
        error!("[SINK] received: {}", input);
        self.counter += 1;
    }

    fn finalize(self) -> Option<usize> {
        error!("End");
        Some(self.counter)
    }
}

#[test]
fn test_stress() {
    env_logger::init();

    for _i in 0..100000 {
        let mut p = parallel![
            Source {
                streamlen: 20,
                counter: 0
            },
            Worker {},
            Sink { counter: 0 }
        ];

        p.start();
        error!("Started new iteration");
        let res = p.wait_and_collect().unwrap();
        error!("Finished");
        assert_eq!(res, 20);
    }
}
