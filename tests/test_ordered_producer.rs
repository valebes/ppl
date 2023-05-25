/*
  Ordered FlatMap example.
*/

use ppl::prelude::*;

// Source produces strings.
struct Source {
    strings: Vec<String>,
}
impl Out<String> for Source {
    fn run(&mut self) -> Option<String> {
        if !self.strings.is_empty() {
            Some(self.strings.remove(0))
        } else {
            None
        }
    }
}

// Stage that produces 5 replicas of each input.
#[derive(Clone)]
struct WorkerA {
    number_of_messages: usize,
    queue: Vec<String>,
}
impl InOut<String, String> for WorkerA {
    fn run(&mut self, input: String) -> Option<String> {
        for _i in 0..self.number_of_messages {
            self.queue.push(input.clone())
        }
        None
    }
    // This stage is a producer.
    // In this method we produce the messages that will be sent to the next stage.
    fn produce(&mut self) -> Option<String> {
        if !self.queue.is_empty() {
            Some(self.queue.pop().unwrap())
        } else {
            None
        }
    }
    // This stage is a producer.
    // Here we specify that, after run the run method,
    // the framework must also call the produce method.
    fn is_producer(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        2
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

// Sink receives strings.
struct Sink {
    queue: Vec<String>,
}
impl In<String, Vec<String>> for Sink {
    fn run(&mut self, input: String) {
        println!("{}", input);
        self.queue.push(input)
    }
    fn finalize(self) -> Option<Vec<String>> {
        Some(self.queue)
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

#[test]
fn test_ordered_producer() {
    env_logger::init();

    let mut p = parallel![
        Source {
            strings: vec![
                "pippo".to_string(),
                "pluto".to_string(),
                "paperino".to_string(),
                "topolino".to_string()
            ],
        },
        WorkerA {
            number_of_messages: 5,
            queue: Vec::new()
        },
        Sink { queue: Vec::new() }
    ];

    p.start();
    let res = p.wait_and_collect().unwrap();
    let a = vec!["pippo".to_string(); 5];
    let b = vec!["pluto".to_string(); 5];
    let c = vec!["paperino".to_string(); 5];
    let d = vec!["topolino".to_string(); 5];

    let check = [a, b, c, d].concat();
    assert_eq!(res, check)
}
