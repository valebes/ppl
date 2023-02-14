extern crate raster;

use raster::filter;
use raster::Image;

use std::env;
use std::time::{SystemTime};
use pspp::{
    node::{
        in_node::{In, InNode},
        inout_node::{InOut, InOutNode},
        out_node::{Out, OutNode},
    },
    pspp::Parallel,
    propagate, parallel,
};

struct Source {
    all_images: Vec<Image>,
}
impl Out<Image> for Source {
    fn run(&mut self) -> Option<Image> {
        self.all_images.pop()
    }
}

#[derive(Clone)]
struct WorkerA {
    replicas: usize
}
impl InOut<Image, Image> for WorkerA {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::saturation(&mut input, 0.2).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
}

#[derive(Clone)]
struct WorkerB {
    replicas: usize
}
impl InOut<Image, Image> for WorkerB {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::emboss(&mut input).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
}

#[derive(Clone)]
struct WorkerC {
    replicas: usize
}
impl InOut<Image, Image> for WorkerC {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::gamma(&mut input, 2.0).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
}

#[derive(Clone)]
struct WorkerD {
    replicas: usize
}
impl InOut<Image, Image> for WorkerD {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::sharpen(&mut input).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
}

#[derive(Clone)]
struct WorkerE {
    replicas: usize
}
impl InOut<Image, Image> for WorkerE {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::grayscale(&mut input).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        self.replicas
    }
}

struct Sink {
    images: Vec<Image>
}
impl In<Image, Vec<Image>> for Sink {
    fn run(&mut self, input: Image) {
        self.images.push(input);
    }

    fn finalize(self) -> Option<Vec<Image>> {
        println!("End");
        Some(self.images)
    }
}

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!();
        panic!("Correct usage: $ ./{:?} <nthreads> <images dir>", args[0]);
    }
    let threads = args[1].parse::<usize>().unwrap();
    let dir_name = &args[2];

    let dir_entries = std::fs::read_dir(format!("{}", dir_name)).unwrap();
    let mut all_images: Vec<Image> = Vec::new();

    for entry in dir_entries {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.extension().is_none() {
            continue;
        }
        all_images.push(raster::open(path.to_str().unwrap()).unwrap());
    }


    let mut p = parallel![
        Source {
            all_images: all_images,
        },
        WorkerA { replicas: threads },
        WorkerB { replicas: threads },
        WorkerC { replicas: threads },
        WorkerD { replicas: threads },
        WorkerE { replicas: threads },
        Sink { images: vec![] }
    ];
   
    let start = SystemTime::now();

    p.start();
    let _res = p.wait_and_collect();

    let system_duration = start.elapsed().expect("Failed to get render time?");
    let in_sec = system_duration.as_secs() as f64 + system_duration.subsec_nanos() as f64 * 1e-9;
    println!("Execution time: {} sec", in_sec);
}