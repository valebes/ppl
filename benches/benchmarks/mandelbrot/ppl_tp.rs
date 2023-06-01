use image::{ImageBuffer, Luma};
/*
    Mandelbrot set
    https://rosettacode.org/wiki/Mandelbrot_set#Rust
*/
use num_complex::Complex;
use ppl::prelude::*;

pub fn ppl_tp(threads: usize) {
    let mut pool = ThreadPool::new_with_global_registry(threads);

    let max_iterations = 256u16;
    let img_side = 800u32;
    let cxmin = -2f32;
    let cxmax = 1f32;
    let cymin = -1.5f32;
    let cymax = 1.5f32;
    let scalex = (cxmax - cxmin) / img_side as f32;
    let scaley = (cymax - cymin) / img_side as f32;

    // Create the coordinates
    let mut buf = Vec::new();
    for y in 0..img_side {
        for x in 0..img_side {
            buf.push((x, y));
        }
    }

    /*
    It is possible to do more efficent as follows: 
    
    pool.par_for_each(imgbuf.enumerate_pixels_mut(), |(x, y, pixel)| {
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
    */

    let mut res: Vec<Luma<u8>> = pool
        .par_map(buf.into_iter(), |(x, y)| {
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

            image::Luma([i as u8])
        })
        .collect();

    // Save image
    let mut image: image::ImageBuffer<Luma<u8>, Vec<u8>> = ImageBuffer::new(img_side, img_side);
    for (_, _, pixel) in image.enumerate_pixels_mut() {
        *pixel = res.remove(0);
    }
    image
        .save("benches/benchmarks/mandelbrot/fractal_rayon.png")
        .unwrap();

    Orchestrator::delete_global_orchestrator();
}
