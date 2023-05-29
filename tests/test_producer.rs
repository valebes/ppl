/*
  FlatMap example.
*/
use ppl::{collections::misc::SinkVec, prelude::*};

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
    input: usize,
}
impl InOut<usize, usize> for WorkerA {
    fn run(&mut self, input: usize) -> Option<usize> {
        self.counter = 0;
        self.input = input;
        None
    }
    // Here we write the rts of the producer.
    fn produce(&mut self) -> Option<usize> {
        if self.counter < self.number_of_messages {
            self.counter += 1;
            Some(self.input)
        } else {
            None
        }
    }
    // Here we state that this stage is a producer.
    fn is_producer(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        8
    }
}

#[test]
fn test_producer() {
    env_logger::init();

    let mut tp = ThreadPool::new_with_global_registry(5);

    for _i in 0..100 {
        let mut p = parallel![
            Source {
                streamlen: 1000,
                counter: 0
            },
            WorkerA {
                number_of_messages: 5,
                counter: 0,
                input: 0
            },
            SinkVec::build()
        ];

        p.start();
        let res = p.wait_and_collect().unwrap();

        // Check that the number of messages is correct.
        assert_eq!(res.len(), 5000);

        // Count the occurrences of each number.
        let check = tp.par_map_reduce(
            res,
            |el| -> (usize, usize) { (el, 1) },
            |k, v| -> (usize, usize) { (k, v.iter().sum()) },
        );

        // Check that the number of occurrences is correct.
        for (_, v) in check {
            assert_eq!(v, 5);
        }
    }
}
