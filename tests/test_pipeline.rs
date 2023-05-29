/*
   Fibonacci pipeline.
   Is generated a sequence of i from 1 to 20.
   Each worker of the farm compute the i-th
   Fibonacci number.
*/

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
        Some(fibonacci_recursive(input))
    }
}

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
fn test_pipeline() {
    env_logger::init();

    for _i in 0..100 {
            // Fibonacci pipeline by using custom structs
    let mut p = parallel![
        Source {
            streamlen: 20,
            counter: 0
        },
        Worker {},
        Sink { counter: 0 }
    ];

    p.start();
    let res = p.wait_and_collect();
    assert_eq!(res.unwrap(), 20);


    // Another way to write the same pipeline, but here using templates instead
    let mut p = parallel![
        SourceIter::build(1..21),
        Sequential::build(fibonacci_recursive),
        SinkVec::build()
    ];
    p.start();
    let res = p.wait_and_collect().unwrap().len();
    assert_eq!(res, 20);

    // Also here another way to write the same pipeline, but here we use closures
    let mut p = parallel![
        {
            let mut counter = 0;
            move || {
                if counter < 20 {
                    counter += 1;
                    Some(counter)
                } else {
                    None
                }
            }
        },
        |input| Some(fibonacci_recursive(input)),
        {
            move |input| {
                println!("{}", input);
            }
        }
    ];
    p.start();
    p.wait_and_collect();
    }
}
