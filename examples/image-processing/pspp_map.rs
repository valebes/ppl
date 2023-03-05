use raster::filter;
use raster::Image;
use std::time::SystemTime;

use pspp::{
    map::Map,
    node::{
        in_node::{In, InNode},
        inout_node::{InOut, InOutNode},
        out_node::{Out, OutNode},
    },
    parallel, propagate,
    pspp::Parallel,
};

struct Source {
    window: usize,
    all_images: Vec<Image>,
}
impl Out<Vec<Image>> for Source {
    fn run(&mut self) -> Option<Vec<Image>> {
        let mut counter = 0;
        let mut tmp = Vec::with_capacity(self.window);
        while counter < self.window {
            match self.all_images.pop() {
                Some(el) =>{
                    tmp.push(el);
                    counter += 1;
                },
                None => {
                    if counter > 0 {
                        break;
                    } else {
                        return None
                    }
                },
            }
        }
        Some(tmp)
    }
}

struct Sink {
    images: Vec<Image>,
}
impl In<Vec<Image>, Vec<Image>> for Sink {
    fn run(&mut self, mut input: Vec<Image>) {
        self.images.append(&mut input);
    }

    fn finalize(self) -> Option<Vec<Image>> {
        Some(self.images)
    }
}

pub fn pspp_map(dir_name: &str, threads: usize) {
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
            window: (all_images.len() / 5) / threads,
            all_images: all_images,
        },
        Map::new(threads, |mut image: Image| -> Image {
            filter::saturation(&mut image, 0.2).unwrap();
            image
        }),
        Map::new::<Vec<Image>, Vec<Image>>(threads, |mut image: Image| -> Image {
            filter::emboss(&mut image).unwrap();
            image
        }),
        Map::new(threads, |mut image: Image| -> Image {
            filter::gamma(&mut image, 2.0).unwrap();
            image
        }),
        Map::new::<Vec<Image>, Vec<Image>>(threads, |mut image: Image| -> Image {
            filter::sharpen(&mut image).unwrap();
            image
        }),
        Map::new(threads, |mut image: Image| -> Image {
            filter::grayscale(&mut image).unwrap();
            image
        }),
        Sink { images: vec![] }
    ];

    let start = SystemTime::now();
    p.start();
    let _res = p.wait_and_collect();

    let system_duration = start.elapsed().expect("Failed to get render time?");
    let in_sec = system_duration.as_secs() as f64 + system_duration.subsec_nanos() as f64 * 1e-9;
    println!("Execution time: {} sec", in_sec);
}
