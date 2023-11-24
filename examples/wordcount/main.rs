use std::env;
mod ppl;

// Take a function and calculate its execution time
fn timeit<F>(f: F)
where
    F: FnOnce(),
{
    let start = std::time::Instant::now();
    f();
    let end = std::time::Instant::now();
    let duration = end.duration_since(start);
    println!("Time: {}", duration.as_secs_f64());
}

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        println!();
        panic!(
            "Correct usage: $ ./{:?} <backend> <nthreads> <dataset.txt>",
            args[0]
        );
    }
    let backend = &args[1];
    let threads = args[2].parse::<usize>().unwrap();
    let dataset = &args[3];

    // TODO: add more backends? In that case this can be a benchmark instead than an example
    match backend.as_str() {
        //"sequential" => sequential::sequential(dir_name),
        //"rust-ssp" => rust_ssp::rust_ssp(dir_name, threads),
        //"rayon" => rayon::rayon(dir_name, threads),
        //"std-threads" => std_threads::std_threads(dir_name, threads),
        "ppl" => {
            timeit(|| ppl::ppl_combined_map_reduce(dataset, threads));
            timeit(|| ppl::ppl_map(dataset, threads));
        }
        _ => println!("Invalid run_mode, use: ppl "),
    }
}
