/*
    Pipeline with a filter node.
*/

use pspp::{prelude::*, collections::misc::{Filter, SinkVec}};

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

fn is_even(input: &usize) -> bool {
    input % 2 == 0
}

#[test]
fn test_filter() {
    env_logger::init();

    let mut p = parallel![
        Source {
            streamlen: 100,
            counter: 0
        },
        Filter::build(|el: &usize| -> bool { is_even(el) }),
        SinkVec::build()
    ];
    p.start();
    let res = p.wait_and_collect().unwrap();
    assert_eq!(res.len(), 50);
}
