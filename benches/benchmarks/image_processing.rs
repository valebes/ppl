/*
    Image Processing
    https://github.com/GMAP/RustStreamBench/tree/main/image-processing
*/
use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration, SamplingMode};

use super::img;

pub fn image_processing(criterion: &mut Criterion) {
    // Sets up criterion.
    let plot_cfg = PlotConfiguration::default().summary_scale(AxisScale::Linear);
    let mut group = criterion.benchmark_group("Image processing");
    group
        .sampling_mode(SamplingMode::Auto)
        .plot_config(plot_cfg)
        .sample_size(20);

    // Parses the images.
    let images = std::fs::read_dir("benches/benchmarks/img/images/")
        .map(|dir| {
            dir.into_iter()
                .flatten()
                .filter_map(|entry| {
                    let path = entry.path();
                    path.extension()
                        .is_some()
                        .then(|| raster::open(path.to_str().unwrap()).unwrap())
                })
                .collect::<Vec<_>>()
        })
        .expect("parsing error");

    let replicas_for_stage = 1..(num_cpus::get() / 5) + 1;
    for replicas in replicas_for_stage {
        let threads = replicas * 5;

        group.bench_function(BenchmarkId::new("rust-ssp", threads), |b| {
            b.iter_batched(
                || images.clone(),
                |images| img::rust_ssp::rust_ssp(images, replicas),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(BenchmarkId::new("std-thread", threads), |b| {
            b.iter_batched(
                || images.clone(),
                |images| img::std_threads::std_threads(images, replicas),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(BenchmarkId::new("ppl", threads), |b| {
            b.iter_batched(
                || images.clone(),
                |images| img::ppl::pspp(images, replicas),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(BenchmarkId::new("rayon", threads), |b| {
            b.iter_batched(
                || images.clone(),
                |images| img::rayon::rayon(images, replicas),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(BenchmarkId::new("ppl-tp", threads), |b| {
            b.iter_batched(
                || images.clone(),
                |images| img::ppl_tp::ppl_tp(images, replicas),
                BatchSize::LargeInput,
            )
        });
    }
}

criterion::criterion_group!(benches, image_processing);
