/*  An ordered pipeline with a map */

use pspp::core::orchestrator::get_global_orchestrator;
use pspp::pipeline::collections::map::OrderedMap;
use pspp::{
    parallel,
    pipeline::{
        in_node::{In, InNode},
        inout_node::{InOut, InOutNode},
        out_node::{Out, OutNode},
    },
    propagate,
    pspp::Parallel,
};

struct Source {
    streamlen: usize,
    counter: usize,
}
impl Out<Vec<i32>> for Source {
    fn run(&mut self) -> Option<Vec<i32>> {
        if self.counter < self.streamlen {
            self.counter += 1;
            Some((0..10000).collect())
        } else {
            None
        }
    }
}

struct Sink {
    res: Vec<Vec<String>>,
}
impl In<Vec<String>, Vec<Vec<String>>> for Sink {
    fn run(&mut self, input: Vec<String>) {
        self.res.push(input);
    }

    fn finalize(self) -> Option<Vec<Vec<String>>> {
        Some(self.res)
    }
}

#[test]
fn test_ordered_map() {
    env_logger::init();

    let mut p = parallel!(
        Source {
            streamlen: 100,
            counter: 0
        },
        OrderedMap::build_with_replicas(
            6,
            |el: i32| -> String { "Hello from: ".to_string() + &el.to_string() },
            6
        ),
        Sink { res: Vec::new() }
    );

    p.start();
    let res = p.wait_and_collect().unwrap();

    assert_eq!(res.len(), 100);
    (0..100).for_each(|i| {
        assert_eq!(res[i].len(), 10000);
        for j in 0..10000 {
            assert_eq!(res[i][j], "Hello from: ".to_string() + &j.to_string());
        }
    });
}
