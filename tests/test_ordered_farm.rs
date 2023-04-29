/*
  Farm of pipeline.
*/

use pspp::core::orchestrator::get_global_orchestrator;
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
            self.counter += 1;
            Some(self.counter)
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct WorkerA {}
impl InOut<usize, usize> for WorkerA {
    fn run(&mut self, input: usize) -> Option<usize> {
        Some(input)
    }
    fn is_ordered(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

#[derive(Clone)]
struct WorkerB {}
impl InOut<usize, usize> for WorkerB {
    fn run(&mut self, input: usize) -> Option<usize> {
        if input % 2 == 0 {
            Some(input)
        } else {
            None
        }
    }
    fn is_ordered(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

#[derive(Clone)]
struct WorkerC {}
impl InOut<usize, usize> for WorkerC {
    fn run(&mut self, input: usize) -> Option<usize> {
        Some(input / 2)
    }
    fn is_ordered(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

struct Sink {
    counter: usize,
    check: bool,
}
impl In<usize, bool> for Sink {
    fn run(&mut self, input: usize) {
        println!("{}", input);
        if input != self.counter {
            self.check = false;
        }
        self.counter += 1;
    }
    fn is_ordered(&self) -> bool {
        true
    }
    fn finalize(self) -> Option<bool> {
        println!("End");
        Some(self.check)
    }
}

#[test]
fn test_ordered_farm() {
    env_logger::init();

    let mut p = parallel![
        Source {
            streamlen: 100,
            counter: 0
        },
        WorkerA {},
        WorkerB {},
        WorkerC {},
        Sink {
            counter: 1,
            check: true
        }
    ];

    p.start();
    let res = p.wait_and_collect();
    assert!(res.unwrap());
}
