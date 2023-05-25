<p align="center">
  <img width="325" height="104" src=".github/logo.png">
</p>

[![Rust](https://github.com/valebes/pspp/actions/workflows/rust.yml/badge.svg)](https://github.com/valebes/pspp/actions/workflows/rust.yml)
# Parallelo
Parallelo Parallel Library (PPL) is a small parallel framework written in rust.

## A simple example
Here a simple example that show how create a pipeline that computes the first 20 numbers of Fibonacci.

This pipeline is composed by:
* A Source, that emits number from 1 to 20.
* A Farm that computes the i-th number of Fibonacci.
* A Sink that accumulates and return a Vec with the results.

Here a snipped of code to show how build this pipeline:
```rust
use ppl::{prelude::*, collections::misc::{SourceIter, Sequential, SinkVec}};

fn main() {
    let mut p = parallel![
        SourceIter::build(1..21),
        Sequential::build(fib), // fib is a method that computes the i-th number of Fibonacci
        SinkVec::build()
    ];
    p.start();
    let res = p.wait_and_collect()
        .unwrap().len();
    assert_eq!(res, 20)
}
```

Parallelo also offers a powerful work-stealing threadpool. Using that instead, we can express the same parallel computation as following:

```rust
use ppl::thread_pool::ThreadPool;

fn main() {
    let tp = ThreadPool::new_with_global_registry(8);
    for i in 1..45 {
        tp.execute(move || {
            fib(i);
        });
    }
    tp.wait(); // Wait till al worker have finished
}
```
