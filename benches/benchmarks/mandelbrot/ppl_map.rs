/* 
    Mandelbrot set
    https://rosettacode.org/wiki/Mandelbrot_set#Rust
*/
use ppl::prelude::*;
use num_complex::Complex;

pub fn ppl_map(threads: usize) {
    let mut pool = ThreadPool::new_with_global_registry(threads);

    let max_iterations = 256u16;
    let img_side = 800u32;
    let cxmin = -2f32;
    let cxmax = 1f32;
    let cymin = -1.5f32;
    let cymax = 1.5f32;
    let scalex = (cxmax - cxmin) / img_side as f32;
    let scaley = (cymax - cymin) / img_side as f32;

    // Create mew image buffer
    let mut imgbuf: image::ImageBuffer<image::Luma<u8>, Vec<u8>> = image::ImageBuffer::new(img_side, img_side);

    pool
        .par_for_each(imgbuf.enumerate_pixels_mut(), |(x, y, pixel)| {
            let cx = cxmin + x as f32 * scalex;
            let cy = cymin + y as f32 * scaley;
    
            let c = Complex::new(cx, cy);
            let mut z = Complex::new(0f32, 0f32);
    
            let mut i = 0;
            for t in 0..max_iterations {
                if z.norm() > 2.0 {
                    break;
                }
                z = z * z + c;
                i = t;
            }
    
            *pixel = image::Luma([i as u8]);
        });

    // Save image
    imgbuf.save("benches/benchmarks/mandelbrot/fractal_ppl_map.png").unwrap();
    Orchestrator::delete_global_orchestrator();
}