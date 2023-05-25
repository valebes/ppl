use ppl::prelude::*;

// A closures can be used to build a pipeline.
// When using a closure, the node created is sequential.
#[test]
fn test_closure() {
    env_logger::init();

    let mut p = parallel!(
        {
            // This closure is the source node.
            // It is executed until it returns None.
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
        // This closure is the filter node.
        |el: usize| -> Option<usize> {
            if el % 2 == 0 {
                Some(el)
            } else {
                None
            }
        },
        // This closure is the sink node.
        |el: usize| { println!("Hello from: {}", el) }
    );

    p.start();
    assert_eq!(p.wait_and_collect(), None);
}
