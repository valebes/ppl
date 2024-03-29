/*
    WordCounter
*/
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    usize,
};

use ppl::{prelude::*, templates::map::MapReduce};

struct Source {
    reader: BufReader<File>,
}
impl Out<Vec<String>> for Source {
    fn run(&mut self) -> Option<Vec<String>> {
        let mut tmp = String::new();
        let res = self.reader.read_line(&mut tmp);
        match res {
            Ok(len) => {
                if len > 0 {
                    Some(
                        tmp.split_whitespace()
                            .map(|s| {
                                s.to_lowercase()
                                    .chars()
                                    .filter(|c| c.is_alphabetic())
                                    .collect::<String>()
                            })
                            .collect(),
                    )
                } else {
                    None
                }
            }
            Err(e) => panic!("{}", e.to_string()),
        }
    }
}

struct Sink {
    counter: HashMap<String, usize>,
}
impl In<Vec<(String, usize)>, Vec<(String, usize)>> for Sink {
    fn run(&mut self, input: Vec<(String, usize)>) {
        // Increment value for key in hashmap
        // If key does not exist, insert it with value 1
        for (key, value) in input {
            let counter = self.counter.entry(key).or_insert(0);
            *counter += value;
        }
    }
    fn finalize(self) -> Option<Vec<(String, usize)>> {
        Some(self.counter.into_iter().collect())
    }
}

// Version that use a node that combine map and reduce
pub fn ppl_combined_map_reduce(dataset: &str, threads: usize) {
    let file = File::open(dataset).expect("no such file");
    let reader = BufReader::new(file);

    let mut p = pipeline![
        Source { reader },
        MapReduce::build_with_replicas(
            threads / 2,
            |str| -> (String, usize) { (str, 1) }, // Map function
            |a, b| a + b,
            2
        ),
        Sink {
            counter: HashMap::new()
        }
    ];

    p.start();
    let res = p.wait_end();

    let mut total_words = 0;
    for (_key, value) in res.unwrap() {
        total_words += value;
    }
    println!(
        "[PIPELINE MAP REDUCE COMBINED] Total words: {}",
        total_words
    );
}
// Version that use par_map_reduce instead of the pipeline
pub fn ppl_map(dataset: &str, threads: usize) {
    let file = File::open(dataset).expect("no such file");
    let reader = BufReader::new(file);

    let mut tp = ThreadPool::with_capacity(threads);

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
        |str| -> (String, usize) { (str, 1) },
        |a, b| a + b,
    );

    let mut total_words = 0;
    for (_str, count) in res {
        //println!("{}: {}", str, count);
        total_words += count;
    }

    println!("[MAP] Total words: {}", total_words);
}
