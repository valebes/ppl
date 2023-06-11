use ppl::core::orchestrator::Orchestrator;
use ppl::thread_pool::ThreadPool;
use raster::filter;
use raster::Image;

pub fn ppl_tp(images: Vec<Image>, threads: usize) {
    let mut pool = ThreadPool::with_capacity(threads * 5);

    let _res: Vec<Image> = pool
        .par_map(images, |mut image: Image| {
            filter::saturation(&mut image, 0.2).unwrap();
            filter::emboss(&mut image).unwrap();
            filter::gamma(&mut image, 2.0).unwrap();
            filter::sharpen(&mut image).unwrap();
            filter::grayscale(&mut image).unwrap();
            image
        })
        .collect();

        unsafe { Orchestrator::delete_global_orchestrator(); }
}
