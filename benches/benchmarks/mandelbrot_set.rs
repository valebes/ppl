/*
    Mandelbrot set
    https://rosettacode.org/wiki/Mandelbrot_set#Rust
*/
use criterion::{AxisScale, BenchmarkId, Criterion, PlotConfiguration, SamplingMode};

use super::mandelbrot;

pub fn mandelbrot_set(criterion: &mut Criterion) {
    // Sets up criterion.
    let plot_cfg = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = criterion.benchmark_group("Mandelbrot set");
    group
        .sampling_mode(SamplingMode::Auto)
        .plot_config(plot_cfg)
        .sample_size(10);

    let mut num = 1;
    let mut threads_range = Vec::new();
    while num < num_cpus::get() {
        threads_range.push(num);
        num *= 2;
    }
    threads_range.push(num_cpus::get());

    for threads in threads_range {
        group.bench_function(BenchmarkId::new("rayon", threads), |b| {
            b.iter(|| mandelbrot::rayon::rayon(threads))
        });

        group.bench_function(BenchmarkId::new("rust_ssp", threads), |b| {
            b.iter(|| mandelbrot::rust_ssp::rust_ssp(threads))
        });

        group.bench_function(BenchmarkId::new("ppl", threads), |b| {
            b.iter(|| mandelbrot::ppl::ppl(threads))
        });

        group.bench_function(BenchmarkId::new("ppl_tp", threads), |b| {
            b.iter(|| mandelbrot::ppl_tp::ppl_tp(threads))
        });
    }
}

criterion::criterion_group!(benches, mandelbrot_set);
