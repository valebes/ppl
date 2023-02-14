use std::env;
mod rayon;
mod pspp;
mod std_threads;
mod rust_ssp;

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        println!();
        panic!("Correct usage: $ ./{:?} <nthreads> <images dir>", args[0]);
    }
    let backend = &args[1];
    let threads = args[2].parse::<usize>().unwrap();
    let dir_name = &args[3];

	match backend.as_str() {
        //"sequential" => sequential::sequential(dir_name),
        "rust-ssp" => rust_ssp::rust_ssp(dir_name, threads),
        "rayon" => rayon::rayon(dir_name, threads),
        "std-threads" => std_threads::std_threads(dir_name, threads),
        "pspp" => pspp::pspp(dir_name, threads),
        _ => println!("Invalid run_mode, use: sequential | rust-ssp | std-threads | rayon | pspp "),
    
    }
}