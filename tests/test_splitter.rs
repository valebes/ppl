/*
  FlatMap example.
*/

use pspp::{
    node::{
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
impl Out<usize> for Source {
    fn run(&mut self) -> Option<usize> {
        if self.counter < self.streamlen {
            self.counter = self.counter + 1;
            Some((self.counter).try_into().unwrap())
        } else {
            None
        }
    }
}

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
    fn splitter(&mut self) -> Option<usize> {
        if self.counter < self.number_of_messages {
            self.counter = self.counter + 1;
            Some(self.counter)
        } else {
            None
        }
    }
    fn is_splitter(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

struct Sink {
    counter: usize,
}
impl In<usize, usize> for Sink {
    fn run(&mut self, _input: usize) {
        self.counter = self.counter + 1;
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
            streamlen: 100,
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
    assert_eq!(res.unwrap(), 500);
}
