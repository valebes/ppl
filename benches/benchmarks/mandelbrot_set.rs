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

    let replicas_for_stage = 1..(num_cpus::get()) + 1;
    for replicas in replicas_for_stage {
        let threads = replicas;

        group.bench_function(BenchmarkId::new("ppl", threads), |b| {
            b.iter(|| mandelbrot::ppl::ppl(threads))
        });


        group.bench_function(BenchmarkId::new("ppl_map", threads), |b| {
            b.iter(|| mandelbrot::ppl_map::ppl_map(threads))
        });
        
    }
}

criterion::criterion_group!(benches, mandelbrot_set);
