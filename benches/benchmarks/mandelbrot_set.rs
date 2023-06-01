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


    let replicas_for_stage = 1..(num_cpus::get() / 2) + 1;
    for replicas in replicas_for_stage {
        let mut threads = 1;
        if replicas > 1 {
            threads *= 2;
        }

        group.bench_function(BenchmarkId::new("rust_ssp", threads), |b| {
            b.iter(|| mandelbrot::rust_ssp::rust_ssp(threads))
        });

        group.bench_function(BenchmarkId::new("ppl", threads), |b| {
            b.iter(|| mandelbrot::ppl::ppl(threads))
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
