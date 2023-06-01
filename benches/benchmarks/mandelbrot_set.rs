use criterion::{AxisScale, BenchmarkId, Criterion, PlotConfiguration, SamplingMode};

use super::mandelbrot;

pub fn mandelbrot_set(criterion: &mut Criterion) {
    // Sets up criterion.
    let plot_cfg = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = criterion.benchmark_group("Mandelbrot set processing time");
    group
        .sampling_mode(SamplingMode::Auto)
        .plot_config(plot_cfg)
        .sample_size(10);


    // Create the coordinates
    let mut buf = Vec::new();
    for y in 0..1000u32 {
        for x in 0..800u32 {
            buf.push((x, y));
        }
    }

    let replicas_for_stage = 1..(num_cpus::get()) + 1;
    for replicas in replicas_for_stage {
        let threads = replicas;

        group.bench_function(BenchmarkId::new("rust_ssp", threads), |b| {
            b.iter(|| mandelbrot::rust_ssp::rust_ssp(buf.clone(), threads))
        });

        group.bench_function(BenchmarkId::new("ppl", threads), |b| {
            b.iter(|| mandelbrot::ppl::ppl(buf.clone(), threads))
        });

        group.bench_function(BenchmarkId::new("ppl_map", threads), |b| {
            b.iter(|| mandelbrot::ppl_tp::ppl_tp(threads))
        });

        group.bench_function(BenchmarkId::new("rayon", threads), |b| {
            b.iter(|| mandelbrot::rayon::rayon(threads))
        });
    }
}

criterion::criterion_group!(benches, mandelbrot_set);
