mod img;

use criterion::{BatchSize, BenchmarkId, Criterion, PlotConfiguration, SamplingMode};

const THREADS: usize = 8; // Can be an array also.

fn image_processing(criterion: &mut Criterion) {
    // Sets up criterion.
    let plot_cfg = PlotConfiguration::default();
    let mut group = criterion.benchmark_group("Processing time");
    group
        .sampling_mode(SamplingMode::Flat)
        .plot_config(plot_cfg)
        .sample_size(10);

    // Parses the images.
    let images = std::fs::read_dir("benches/img/images")
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

    group.bench_function(BenchmarkId::new("rust_ssp", THREADS), |b| {
        b.iter_batched(
            || images.clone(),
            |images| img::rust_ssp::rust_ssp(images, THREADS),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(BenchmarkId::new("std_threads", THREADS), |b| {
        b.iter_batched(
            || images.clone(),
            |images| img::std_threads::std_threads(images, THREADS),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(BenchmarkId::new("rayon", THREADS), |b| {
        b.iter_batched(
            || images.clone(),
            |images| img::rayon::rayon(images, THREADS),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(BenchmarkId::new("pspp", THREADS), |b| {
        b.iter_batched(
            || images.clone(),
            |images| img::pspp::pspp(images, THREADS),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(BenchmarkId::new("pspp_map", THREADS), |b| {
        b.iter_batched(
            || images.clone(),
            |images| img::pspp_map::pspp_map(images, THREADS),
            BatchSize::LargeInput,
        )
    });
}

criterion::criterion_group!(benches, image_processing);
criterion::criterion_main!(benches);
