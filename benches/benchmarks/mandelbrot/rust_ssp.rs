use image::Luma;
use num_complex::Complex;

use rust_spp::*;

pub fn rust_ssp(threads: usize) {
    let max_iterations = 10000u16;
    let img_side = 1000u32;
    let cxmin = -2f32;
    let cxmax = 1f32;
    let cymin = -1.5f32;
    let cymax = 1.5f32;
    let scalex = (cxmax - cxmin) / img_side as f32;
    let scaley = (cymax - cymin) / img_side as f32;

    // Create the coordinates
    let mut buf = Vec::new();
    for y in 0..1000u32 {
        for x in 0..1000u32 {
            buf.push((x, y));
        }
    }
    
    let pipeline = pipeline![
        parallel!(
            move |(x, y)| -> Option<Luma<u8>> {
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

                Some(image::Luma([i as u8]))
            },
            threads as i32
        ),
        collect_ordered!()
    ];

    for coord in buf.into_iter() {
        pipeline.post(coord).unwrap();
    }

    let mut _res = pipeline.collect();
}
