use pspp::thread_pool::ThreadPool;
use raster::filter;
use raster::Image;

pub fn pspp_map(images: Vec<Image>, threads: usize) {
    let mut pool = ThreadPool::new(threads, true);
    let _a = pool.par_map(images, |mut image: Image| -> Image {
        filter::saturation(&mut image, 0.2).unwrap();
        filter::emboss(&mut image).unwrap();
        filter::gamma(&mut image, 2.0).unwrap();
        filter::sharpen(&mut image).unwrap();
        filter::grayscale(&mut image).unwrap();
        image
    });
}
