use raster::filter;
use raster::Image;

use std::time::{SystemTime};
use pspp::{
    node::{
        in_node::{In, InNode},
        inout_node::{InOut, InOutNode},
        out_node::{Out, OutNode},
    },
    pipeline::Pipeline,
    pipeline_propagate, parallel,
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
struct WorkerA {}
impl InOut<Image, Image> for WorkerA {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::saturation(&mut input, 0.2).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        8
    }
}

#[derive(Clone)]
struct WorkerB {}
impl InOut<Image, Image> for WorkerB {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::emboss(&mut input).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        8
    }
}

#[derive(Clone)]
struct WorkerC {}
impl InOut<Image, Image> for WorkerC {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::gamma(&mut input, 2.0).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        8
    }
}

#[derive(Clone)]
struct WorkerD {}
impl InOut<Image, Image> for WorkerD {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::sharpen(&mut input).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        8
    }
}

#[derive(Clone)]
struct WorkerE {}
impl InOut<Image, Image> for WorkerE {
    fn run(&mut self, mut input: Image) -> Option<Image> {
        filter::grayscale(&mut input).unwrap();
        Some(input)
    }
    fn number_of_replicas(&self) -> usize {
        8
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

#[test]
fn test_raster() {
    env_logger::init();

    let dir_entries = std::fs::read_dir(format!("{}", "/Users/valebes/Desktop/pspp/tests/input_big")).unwrap();
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
        Box::new(Source {
            all_images: all_images,
        }),
        Box::new(WorkerA {}),
        Box::new(WorkerB {}),
        Box::new(WorkerC {}),
        Box::new(WorkerD {}),
        Box::new(WorkerE {}),
        Box::new(Sink { images: vec![] })
    ];
   
    let start = SystemTime::now();

    p.start();
    let _res = p.collect();

    let system_duration = start.elapsed().expect("Failed to get render time?");
    let in_sec = system_duration.as_secs() as f64 + system_duration.subsec_nanos() as f64 * 1e-9;
    println!("Execution time: {} sec", in_sec);
}