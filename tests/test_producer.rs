/*
  FlatMap example.
*/

use ppl::prelude::*;

// Source
struct Source {
    streamlen: usize,
    counter: usize,
}
impl Out<usize> for Source {
    fn run(&mut self) -> Option<usize> {
        if self.counter < self.streamlen {
            self.counter += 1;
            Some(self.counter)
        } else {
            None
        }
    }
}

// Given an input, it produces 5 copies of it.
#[derive(Clone)]
struct WorkerA {
    number_of_messages: usize,
    counter: usize,
}
impl InOut<usize, usize> for WorkerA {
    fn run(&mut self, _input: usize) -> Option<usize> {
        self.counter = 0;
        None
    }
    // Here we write the rts of the producer.
    fn produce(&mut self) -> Option<usize> {
        if self.counter < self.number_of_messages {
            self.counter += 1;
            Some(self.counter)
        } else {
            None
        }
    }
    // Here we state that this stage is a producer.
    fn is_producer(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

// Sink
struct Sink {
    counter: usize,
}
impl In<usize, usize> for Sink {
    fn run(&mut self, _input: usize) {
        self.counter += 1;
    }
    fn finalize(self) -> Option<usize> {
        Some(self.counter)
    }
}

#[test]
fn test_splitter() {
    env_logger::init();

    let mut p = parallel![
        Source {
            streamlen: 10000,
            counter: 0
        },
        WorkerA {
            number_of_messages: 5,
            counter: 0
        },
        Sink { counter: 0 }
    ];

    p.start();
    let res = p.wait_and_collect();
    assert_eq!(res.unwrap(), 50000);
}