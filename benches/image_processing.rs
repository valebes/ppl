mod img;

use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration, SamplingMode};

fn image_processing(criterion: &mut Criterion) {
    // Sets up criterion.
    let plot_cfg = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = criterion.benchmark_group("Processing time");
    group
        .sampling_mode(SamplingMode::Auto)
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

    let replicas_for_stage = 1..(num_cpus::get() / 5);
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

        group.bench_function(BenchmarkId::new("pspp", threads), |b| {
            b.iter_batched(
                || images.clone(),
                |images| img::pspp::pspp(images, replicas),
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

        group.bench_function(BenchmarkId::new("pspp-map", threads), |b| {
            b.iter_batched(
                || images.clone(),
                |images| img::pspp_map::pspp_map(images, replicas),
                BatchSize::LargeInput,
            )
        });
    }
}

criterion::criterion_group!(benches, image_processing);
criterion::criterion_main!(benches);
