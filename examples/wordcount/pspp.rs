use pspp::{core::orchestrator::get_global_orchestrator, thread_pool::ThreadPool};
use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc, usize,
};

use dashmap::DashMap;
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
    reader: BufReader<File>,
}
impl Out<String> for Source {
    fn run(&mut self) -> Option<String> {
        let mut tmp = String::new();
        let res = self.reader.read_line(&mut tmp);
        match res {
            Ok(len) => {
                if len > 0 {
                    Some(tmp)
                } else {
                    None
                }
            }
            Err(e) => panic!("{}", e.to_string()),
        }
    }
}

#[derive(Clone)]
struct Splitter {
    replicas: usize,
    tmp_buffer: VecDeque<String>,
}
impl InOut<String, String> for Splitter {
    fn run(&mut self, input: String) -> Option<String> {
        self.tmp_buffer = input
            .split_whitespace()
            .map(|s| {
                s.to_lowercase()
                    .chars()
                    .filter(|c| c.is_alphabetic())
                    .collect::<String>()
            })
            .collect();
        None
    }
    fn produce(&mut self) -> Option<String> {
        if !self.tmp_buffer.is_empty() {
            Some(self.tmp_buffer.pop_front().unwrap())
        } else {
            None
        }
    }
    fn is_producer(&self) -> bool {
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
    hashmap: Arc<DashMap<String, usize>>,
    replicas: usize,
}
impl InOut<String, (String, usize)> for Counter {
    fn run(&mut self, input: String) -> Option<(String, usize)> {
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
    counter: usize,
}
impl In<(String, usize), usize> for Sink {
    fn run(&mut self, _input: (String, usize)) {
        //println!("Received word {} with counter {}", input.0, input.1 );
        self.counter += 1;
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
    let reader = BufReader::new(file);

    let hashmap = Arc::new(DashMap::with_shard_amount(256));
    let mut p = parallel![
        Source { reader },
        Splitter {
            replicas: threads,
            tmp_buffer: VecDeque::new()
        },
        Counter {
            hashmap: Arc::clone(&hashmap),
            replicas: threads
        },
        Sink { counter: 0 }
    ];

    p.start();
    let res = p.wait_and_collect();
    println!("[PIPELINE] Total words: {}", res.unwrap());
}

// Version that use par_map_reduce instead of the pipeline
pub fn pspp_map(dataset: &str, threads: usize) {
    let file = File::open(dataset).expect("no such file");
    let reader = BufReader::new(file);

    let mut tp = ThreadPool::new_with_global_registry(threads);

    let mut words = Vec::new();
    
        reader
            .lines()
            .map(|s| s.unwrap())
            .for_each(|s| words.push(s));




    let res = tp.par_map_reduce(
        words // Collect all the lines in a vector
            .iter()
            .flat_map(|s| s.split_whitespace())
            .map(|s| {
                s.to_lowercase()
                    .chars()
                    .filter(|c| c.is_alphabetic())
                    .collect::<String>()
            })
            .collect::<Vec<String>>(),
        |str| -> (String, usize) { (str, 1)},
        |str, count| {
            let mut sum = 0;
            for c in count {
                sum += c;
            }
            (str, sum)
        },
    );

    let mut total_words = 0;
    for (str, count) in res {
        //println!("{}: {}", str, count);
        total_words += count;
    }

    println!("[MAP] Total words: {}", total_words);
  
    

}
