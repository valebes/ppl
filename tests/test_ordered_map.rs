/*
    An ordered pipeline with a map
    It is similar to tests/test_map.rs, but it uses an ordered map.
*/

use ppl::{prelude::*, templates::map::OrderedMap};

// Source node.
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

// Sink node.
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

    let mut p = pipeline!(
        Source {
            streamlen: 100,
            counter: 0
        },
        // Create an OrderedMap node with the build_with_replicas method.
        OrderedMap::build_with_replicas(
            6, // Number of workers per stage
            3, // number of replicas of this stage
            |el: i32| -> String { "Hello from: ".to_string() + &el.to_string() },
        ),
        Sink { res: Vec::new() }
    );

    p.start();
    let res = p.wait_end().unwrap();

    assert_eq!(res.len(), 100);
    (0..100).for_each(|i| {
        assert_eq!(res[i].len(), 10000);
        for j in 0..10000 {
            assert_eq!(res[i][j], "Hello from: ".to_string() + &j.to_string());
        }
    });
}
