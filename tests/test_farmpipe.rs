/*
  Farm of pipeline.
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

#[derive(Clone)]
struct WorkerA {}
impl InOut<i32, i32> for WorkerA {
    fn run(&mut self, input: i32) -> Option<i32> {
        Some(input)
    }
    fn ordered(&self) -> bool {
        false
    }
    fn number_of_replicas(&self) -> usize {
        1
    }
}

#[derive(Clone)]
struct WorkerB {}
impl InOut<i32, i32> for WorkerB {
    fn run(&mut self, input: i32) -> Option<i32> {
        if input % 2 == 0 {
            Some(input)
        } else {
            None
        }
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

#[derive(Clone)]
struct WorkerC {}
impl InOut<i32, i32> for WorkerC {
    fn run(&mut self, input: i32) -> Option<i32> {
        Some(input / 2)
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
}

struct Sink {
    counter: usize,
}
impl In<i32, usize> for Sink {
    fn run(&mut self, input: i32) {
        println!("{}", input);
        self.counter = self.counter + 1;
    }
    fn ordered(&self) -> bool {
        true
    }
    fn finalize(&mut self) -> Option<usize> {
        println!("End");
        Some(self.counter)
    }
}

#[test]
fn farm() {
    env_logger::init();

    let p = pipeline![
        Box::new(Source {
            streamlen: 100,
            counter: 0
        }),
        Box::new(WorkerA {}),
        Box::new(WorkerB {}),
        Box::new(WorkerC {}),
        Box::new(Sink { counter: 0 })
    ];

    let res = p.collect();
    assert_eq!(res.unwrap(), 50);
}
