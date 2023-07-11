/*
    Pipeline with a filter node.
*/

use ppl::{
    prelude::*,
    templates::misc::{Filter, SinkVec},
};

// Source node.
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

// Filter function.
fn is_even(input: &usize) -> bool {
    input % 2 == 0
}

#[test]
fn test_filter() {
    env_logger::init();

    let mut p = pipeline![
        Source {
            streamlen: 100,
            counter: 0
        },
        // We can create a filter node with the filter template.
        Filter::build(|el: &usize| -> bool { is_even(el) }),
        // Also here we can use templates. In this case we use the SinkVec template.
        SinkVec::build()
    ];
    // Start the pipeline.
    p.start();
    // Wait for the pipeline to finish and collect the results.
    let res = p.wait_end().unwrap();
    assert_eq!(res.len(), 50);
}
