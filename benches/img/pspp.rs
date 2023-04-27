use pspp::core::orchestrator::get_global_orchestrator;
use pspp::core::orchestrator::Orchestrator;
use raster::filter;
use raster::Image;

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
    all_images: Vec<Image>,
}
impl Out<Image> for Source {
    fn run(&mut self) -> Option<Image> {
        self.all_images.pop()
    }
}

#[derive(Clone)]
struct WorkerA {
    replicas: usize,
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
    replicas: usize,
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
    replicas: usize,
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
    replicas: usize,
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
    replicas: usize,
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
    images: Vec<Image>,
}
impl In<Image, Vec<Image>> for Sink {
    fn run(&mut self, input: Image) {
        self.images.push(input);
    }

    fn finalize(self) -> Option<Vec<Image>> {
        Some(self.images)
    }
}

pub fn pspp(images: Vec<Image>, threads: usize) {
    let mut p = parallel![
        Source { all_images: images },
        WorkerA { replicas: threads },
        WorkerB { replicas: threads },
        WorkerC { replicas: threads },
        WorkerD { replicas: threads },
        WorkerE { replicas: threads },
        Sink { images: vec![] }
    ];

    p.start();
    let _res = p.wait_and_collect();
    Orchestrator::delete_global_orchestrator();
}
