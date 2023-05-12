use pspp::core::orchestrator::get_global_orchestrator;
use pspp::map::Map;
use pspp::{
    pipeline::{
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
impl Out<Vec<i32>> for Source {
    fn run(&mut self) -> Option<Vec<i32>> {
        if self.counter < self.streamlen {
            self.counter += 1;
            Some((0..99).collect())
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
        println!("End");
        Some(self.res)
    }
}

#[test]
fn test_map() {
    env_logger::init();

    let mut p = parallel![
        Source {
            streamlen: 50,
            counter: 0
        },
        Map::build(2, |el: i32| -> String {
            "Hello from: ".to_string() + &el.to_string()
        }),
        Sink { res: Vec::new() }
    ];

    p.start();
    let res = p.wait_and_collect().unwrap();

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
