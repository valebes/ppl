use pspp::thread_pool::ThreadPool;
use raster::filter;
use raster::Image;
use std::time::SystemTime;

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

    let start = SystemTime::now();
    let mut pool = ThreadPool::new(threads * 5, true);
    let a = pool.par_map(all_images, |mut image: Image| -> Image {
        filter::saturation(&mut image, 0.2).unwrap();
        image
    });
    let b = pool.par_map(a, |mut image: Image| -> Image {
        filter::emboss(&mut image).unwrap();
        image
    });
    let c = pool.par_map(b,  |mut image: Image| -> Image {
        filter::gamma(&mut image, 2.0).unwrap();
        image
    });
    let d = pool.par_map(c,  |mut image: Image| {
        filter::sharpen(&mut image).unwrap();
        image
    });
    let e = pool.par_map(d,  |mut image: Image| {
        filter::grayscale(&mut image).unwrap();
        image
    });

    let system_duration = start.elapsed().expect("Failed to get render time?");
    let in_sec = system_duration.as_secs() as f64 + system_duration.subsec_nanos() as f64 * 1e-9;
    println!("Execution time: {} sec", in_sec);
}
