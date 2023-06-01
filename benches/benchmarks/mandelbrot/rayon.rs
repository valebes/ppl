use num_complex::Complex;
use rayon::prelude::*;

pub fn rayon(threads: usize) {
    let max_iterations = 10000u16;
    let img_side = 1000u32;
    let cxmin = -2f32;
    let cxmax = 1f32;
    let cymin = -1.5f32;
    let cymax = 1.5f32;
    let scalex = (cxmax - cxmin) / img_side as f32;
    let scaley = (cymax - cymin) / img_side as f32;

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap();

    // Create the coordinates
    let mut buf = Vec::new();
    for y in 0..img_side {
        for x in 0..img_side {
            buf.push((x, y));
        }
    }

    // Create a new ImgBuf
    let mut imgbuf = image::ImageBuffer::new(img_side, img_side);

    pool.install(|| {
        imgbuf
            .enumerate_pixels_mut()
            .par_bridge()
            .for_each(|(x, y, pixel)| {
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
    });
}
