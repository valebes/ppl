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
use std::time::SystemTime;

use {
    crossbeam_channel::{bounded, TryRecvError},
    std::thread,
};

pub fn std_threads(dir_name: &str, threads: usize) {
    let dir_entries = std::fs::read_dir(format!("{}", dir_name));
    let mut all_images: Vec<Image> = Vec::new();

    for entry in dir_entries.unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.extension().is_none() {
            continue;
        }
        all_images.push(raster::open(path.to_str().unwrap()).unwrap());
    }

    let (queue1_send, queue1_recv) = bounded(512);
    let (queue2_send, queue2_recv) = bounded(512);
    let (queue3_send, queue3_recv) = bounded(512);
    let (queue4_send, queue4_recv) = bounded(512);
    let (queue5_send, queue5_recv) = bounded(512);
    let (queue6_send, queue6_recv) = bounded(512);

    let start = SystemTime::now();

    thread::spawn(move || {
        for image in all_images.into_iter() {
            queue1_send.send(image).unwrap();
        }

        drop(queue1_send);
    });

    for _i in 0..threads {
        let (send, recv) = (queue2_send.clone(), queue1_recv.clone());

        thread::spawn(move || loop {
            let image = recv.try_recv();
            let mut image = match image {
                Ok(image) => image,
                Err(e) if e == TryRecvError::Disconnected => break,
                Err(e) if e == TryRecvError::Empty => continue,
                Err(e) => panic!("Error during recv {}", e),
            };

            filter::saturation(&mut image, 0.2).unwrap();

            send.send(image).unwrap();
        });
    }
    drop(queue2_send);

    for _i in 0..threads {
        let (send, recv) = (queue3_send.clone(), queue2_recv.clone());

        thread::spawn(move || loop {
            let image = recv.try_recv();
            let mut image = match image {
                Ok(image) => image,
                Err(e) if e == TryRecvError::Disconnected => break,
                Err(e) if e == TryRecvError::Empty => continue,
                Err(e) => panic!("Error during recv {}", e),
            };

            filter::emboss(&mut image).unwrap();

            send.send(image).unwrap();
        });
    }
    drop(queue3_send);

    for _i in 0..threads {
        let (send, recv) = (queue4_send.clone(), queue3_recv.clone());

        thread::spawn(move || loop {
            let image = recv.try_recv();
            let mut image = match image {
                Ok(image) => image,
                Err(e) if e == TryRecvError::Disconnected => break,
                Err(e) if e == TryRecvError::Empty => continue,
                Err(e) => panic!("Error during recv {}", e),
            };

            filter::gamma(&mut image, 2.0).unwrap();

            send.send(image).unwrap();
        });
    }
    drop(queue4_send);

    for _i in 0..threads {
        let (send, recv) = (queue5_send.clone(), queue4_recv.clone());

        thread::spawn(move || loop {
            let image = recv.try_recv();
            let mut image = match image {
                Ok(image) => image,
                Err(e) if e == TryRecvError::Disconnected => break,
                Err(e) if e == TryRecvError::Empty => continue,
                Err(e) => panic!("Error during recv {}", e),
            };

            filter::sharpen(&mut image).unwrap();

            send.send(image).unwrap();
        });
    }
    drop(queue5_send);

    for _i in 0..threads {
        let (send, recv) = (queue6_send.clone(), queue5_recv.clone());

        thread::spawn(move || loop {
            let image = recv.try_recv();
            let mut image = match image {
                Ok(image) => image,
                Err(e) if e == TryRecvError::Disconnected => break,
                Err(e) if e == TryRecvError::Empty => continue,
                Err(e) => panic!("Error during recv {}", e),
            };

            filter::grayscale(&mut image).unwrap();

            send.send(image).unwrap();
        });
    }
    drop(queue6_send);

    let _collection: Vec<Image> = queue6_recv.iter().collect();

    let system_duration = start.elapsed().expect("Failed to get render time?");
    let in_sec = system_duration.as_secs() as f64 + system_duration.subsec_nanos() as f64 * 1e-9;
    println!("Execution time: {} sec", in_sec);
}
