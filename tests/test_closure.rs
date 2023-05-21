use pspp::core::orchestrator::get_global_orchestrator;
use pspp::{
    parallel,
    pipeline::{
        in_node::InNode,
        inout_node::{InOut, InOutNode},
        out_node::OutNode,
    },
    propagate,
    pspp::Parallel,
};

// Test that a closures can be used to build a pipeline
// When using a closure, the node created is sequential
#[test]
fn test_closure() {
    env_logger::init();

    let mut p = parallel!(
        {
            let mut counter = 0;
            move || -> Option<usize> {
                if counter < 100 {
                    counter += 1;
                    Some(counter)
                } else {
                    None
                }
            }
        },
        |el: usize| -> Option<usize> {
            if el % 2 == 0 {
                Some(el)
            } else {
                None
            }
        },
        |el: usize| { println!("Hello from: {}", el) }
    );

    p.start();
    assert_eq!(p.wait_and_collect(), None);
}
