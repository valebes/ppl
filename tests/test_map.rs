/*
    Pipeline with a Map node.
 */

use ppl::{collections::map::Map, prelude::*};

// Source node.
// It generates a stream of 10000 elements each time.
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
// It collects the results from the Map node.
// Here it is possible to use the template SinkVec instead of this.
struct Sink {
    res: Vec<Vec<String>>,
}
impl In<Vec<String>, Vec<Vec<String>>> for Sink {
    fn run(&mut self, input: Vec<String>) {
        self.res.push(input);
    }
    // This function is called when the pipeline is finished.
    fn finalize(self) -> Option<Vec<Vec<String>>> {
        println!("End");
        Some(self.res)
    }
}

#[test]
fn test_map() {
    env_logger::init();

    // Create the pipeline.
    let mut p = parallel![
        Source {
            streamlen: 100,
            counter: 0
        },
        // Create a Map node with the build template.
        Map::build(6, |el: i32| -> String {
            "Hello from: ".to_string() + &el.to_string()
        }),
        Sink { res: Vec::new() }
    ];
    // Start the pipeline.
    p.start();
    let res = p.wait_and_collect().unwrap();

    // Check the results.
    let mut check = true;
    for sub_res in res {
        for (i, str) in sub_res.into_iter().enumerate() {
            if str != ("Hello from: ".to_string() + &i.to_string()) {
                check = false;
            }
        }
    }
    assert!(check)
}
