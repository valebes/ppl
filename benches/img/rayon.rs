/*
MIT License

Copyright (c) 2021 Parallel Applications Modelling Group - GMAP

 GMAP website: https://gmap.pucrs.br

 Pontifical Catholic University of Rio Grande do Sul (PUCRS)

 Av. Ipiranga, 6681, Porto Alegre - Brazil, 90619-900

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

use raster::filter;
use raster::Image;

use rayon::prelude::*;

pub fn rayon(images: Vec<Image>, threads: usize) {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads * 5)
        .build()
        .unwrap();
        pool.install( || {
        let _collection: Vec<Image>  = images.into_iter()
            .par_bridge()
            .filter_map(|mut image: Image| { 
                filter::saturation(&mut image, 0.2).unwrap();
                Some(image)
            })
            .filter_map(|mut image: Image| { 
                filter::emboss(&mut image).unwrap();
                Some(image)
            })
            .filter_map(|mut image: Image| { 
                filter::gamma(&mut image, 2.0).unwrap();
                Some(image)
            })
            .filter_map(|mut image: Image| { 
                filter::sharpen(&mut image).unwrap();
                Some(image)
            })
            .filter_map(|mut image: Image| { 
                filter::grayscale(&mut image).unwrap();
                Some(image)
            })
            .collect();
});
}
