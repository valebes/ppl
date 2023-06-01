use image::{Luma, ImageBuffer};
/* 
    Mandelbrot set
    https://rosettacode.org/wiki/Mandelbrot_set#Rust
*/
use ppl::{prelude::*, collections::misc::{SourceIter, OrderedParallel, OrderedSinkVec}};
use num_complex::Complex;

pub fn ppl(threads: usize) {
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


    let mut pipeline = parallel![
        SourceIter::build(buf.into_iter()),
        OrderedParallel::build(threads, move |(x, y)| -> Luma<u8> {
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
        }),
        OrderedSinkVec::build()
    ];
    
    pipeline.start();
    let mut res = pipeline.wait_and_collect().unwrap();
    
    // Save image
    let mut image: image::ImageBuffer<Luma<u8>, Vec<u8>> = ImageBuffer::new(img_side, img_side);
    for (_, _, pixel) in image.enumerate_pixels_mut() {
        *pixel = res.remove(0);
    }
    image.save("benches/benchmarks/mandelbrot/fractal_ppl.png").unwrap();
    Orchestrator::delete_global_orchestrator();
}