use std::{fs::File, collections::{VecDeque}, io::{BufReader, BufRead}, time::SystemTime, sync::{Arc}};

use dashmap::DashMap;
use pspp::{
    node::{
        in_node::{In, InNode},
        inout_node::{InOut, InOutNode},
        out_node::{Out, OutNode},
    },
    parallel, propagate,
    pspp::Parallel,
};

struct Source {
    buffer: VecDeque<String>,
}
impl Out<String> for Source {
    fn run(&mut self) -> Option<String> {
        if !self.buffer.is_empty() {
            Some(self.buffer.pop_front().unwrap())
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct Splitter {
    replicas: usize,
    tmp_buffer: VecDeque<String>
}
impl InOut<String, String> for Splitter {
    fn run(&mut self, input: String) -> Option<String> {
        self.tmp_buffer = input.split_whitespace().into_iter().map(|s| String::from(s).to_lowercase()).collect();
        None
    }
    fn splitter(&mut self) -> Option<String> {
        if !self.tmp_buffer.is_empty() {
            Some(self.tmp_buffer.pop_front().unwrap())
        } else {
            None
        }
    }
    fn is_splitter(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct Counter {
    hashmap:  Arc<DashMap<String, usize>>,
    replicas: usize,
}
impl InOut<String, (String, usize)> for Counter {
    fn run(&mut self, input: String) -> Option< (String, usize)> {
        if self.hashmap.contains_key(&input) {
            let res = *self.hashmap.get(&input).unwrap() + 1;
            self.hashmap.insert(input.clone(), res);
            Some((input, res))
        } else {
            self.hashmap.insert(input.clone(), 1);
            Some((input, 1))
        }
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
    fn is_ordered(&self) -> bool {
        true
    }
}


struct Sink {
    counter: usize
}
impl In<(String, usize), usize> for Sink {
    fn run(&mut self, input: (String, usize)) {
        //println!("Received word {} with counter {}", input.0, input.1 );
        self.counter = self.counter + 1;
    }
    fn finalize(self) -> Option<usize> {
        Some(self.counter)
    }
    fn is_ordered(&self) -> bool {
        true
    }
}

pub fn pspp(dataset: &str, threads: usize) {
    let file = File::open(dataset).expect("no such file");
    let reader = BufReader::new(file).lines();
    let mut buffer = VecDeque::new();
    for line in reader {
        match line {
            Ok(str) => buffer.push_back(str),
            Err(_) => panic!("Error reading the dataset!"),
        }
    }

    let hashmap = Arc::new(DashMap::new());
    let mut p = parallel![
        Source { buffer: buffer },
        Splitter {replicas: threads, tmp_buffer: VecDeque::new()},
        Counter { hashmap: Arc::clone(&hashmap) , replicas: threads},
        Sink { counter: 0 }
    ];

    let start = SystemTime::now();

    p.start();
    let res = p.wait_and_collect();
    println!("Total words: {}", res.unwrap());

    let system_duration = start.elapsed().expect("Failed to get render time?");
    let in_sec = system_duration.as_secs() as f64 + system_duration.subsec_nanos() as f64 * 1e-9;
    println!("Execution time: {} sec", in_sec);
}
