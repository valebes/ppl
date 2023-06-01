use criterion::criterion_main;
mod benchmarks;

criterion_main!(
    benchmarks::mandelbrot_set::benches,
    benchmarks::image_processing::benches,
);
