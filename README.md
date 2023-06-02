<p align="center">
  <img width="325" height="104" src=".github/logo.png">
</p>

[![Rust](https://github.com/valebes/pspp/actions/workflows/rust.yml/badge.svg)](https://github.com/valebes/pspp/actions/workflows/rust.yml)
 
# Parallelo: Unlock the Power of Parallel Computing in Rust

🌟 Welcome to Parallelo Parallel Library (PPL) – a small, but powerful, parallel framework written in Rust. 🚀

PPL empowers your Rust programs by unlocking the immense potential of parallelism, making your computations faster and more efficient. Whether you're working on large-scale data processing, simulations, or any computationally intensive task, PPL has got you covered.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [A Simple (but Long) Example: Fibonacci pipeline](#a-simple-but-long-example-fibonacci-pipeline)
  - [A More Complex Example: Word Counter](#a-more-complex-example-word-counter)
  - [Advanced Example: Single-Input Multi-Output stage](#advanced-example-single-input-multi-output-stage)
- [Configuration](#configuration)
- [Channel Backend](#channel-backend)
  - [FastFlow Channel](#fastflow-channel)
- [Benchmarks](#benchmarks)
- [Contributing](#contributing)
  - [Code of Conduct](#code-of-conduct)
  - [How to Contribute](#how-to-contribute)
  - [Reporting Issues](#reporting-issues)
- [Warning](#warning)
- [License](#license)


## Features

- **Parallel Computing**: Unlock the power of parallelism in Rust with the Parallelo Parallel Library (PPL). Harness the potential of multiple cores to make your computations faster and more efficient.
- **Essential Tools**: PPL offers a variety of essential tools for parallel computing, including a work-stealing thread pool, pipelines, farms, and other parallel skeletons. These tools allow you to express complex parallel computations with ease.
- **NUMA Awareness**: Take advantage of PPL's NUMA awareness to optimize the utilization of your system's resources. Customize aspects such as the maximum number of cores to use, thread wait policies, and mapping of threads to physical cores for enhanced performance and efficiency.
- **Multiple Channel Backends**: Choose from a range of flexible channel implementations in PPL to enable seamless communication and coordination between parallel tasks. Select the channel backend that suits your application requirements, ensuring smooth data flow throughout your parallel computations.
- **Customization and Stateful Nodes**: With PPL, you have the flexibility to create custom stages and nodes, allowing you to add state and express more complex parallel computations. Tailor your pipeline to specific needs and create highly customizable parallel workflows.
- **Intuitive API**: Whether you're a seasoned parallel computing expert or new to parallelism, PPL simplifies parallel programming with its intuitive API. Developers of all levels of expertise can easily leverage the power of parallel computing in Rust.
- **Work-Stealing Thread Pool**: PPL includes a powerful work-stealing thread pool that further enhances parallel execution. Utilize the thread pool to distribute work across multiple threads, maximizing the efficiency of your parallel computations.

## Installation

Parallelo Parallel Library (PPL) is currently available on GitHub. To use PPL in your Rust project, you can clone the repository from GitHub.

Please make sure you have Rust and Cargo installed on your system before proceeding. If you don't have them installed, you can get them from the official Rust website: [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)

To clone the PPL repository, open your terminal and run the following command:

```shell
$ git clone https://github.com/valebes/ppl.git
```

## Usage
### A simple (but long) example: Fibonacci pipeline
Here a simple example that show how create a pipeline that computes the first 20 numbers of Fibonacci.

This pipeline is composed by:
- **Source**: emits number from 1 to 20.
- **Stage**: computes the i-th number of Fibonacci.
- **Sink**: accumulates and return a Vec with the results.

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

In this example we're building a simple pipeline, so why not use clousure instead than the library templates?
```rust
use ppl::prelude::*;

fn main() {
    let mut p = parallel![
        {
            let mut counter = 0;
            move || {
                if counter < 20 {
                    counter += 1;
                    Some(counter)
                } else {
                    None
                }
            }
        },
        |input| Some(fib(input)),
        {
            move |input| {
                println!("{}", input);
            }
        }
    ];
    p.start();
    p.wait_and_collect();
}
```

The RTS of the library will call the clousure representing the Source till it returns `None`, when the Source returns `None` then a termination message will be propagated to the other stages of the pipeline.

If we want to parallelize the computation we must find a part of this algorithm that can be parallelized.
In this case the stage in the middle is a good candidate, we replicate that stage introducing a Farm.

Now the code is as follows:
```rust
use ppl::{prelude::*, collections::misc::{SourceIter, Parallel, SinkVec}};

fn main() {
    let mut p = parallel![
        SourceIter::build(1..21),
        Parallel::build(8, fib), // We create 8 replicas of the stage seen in the previous code.
        SinkVec::build()
    ];
    p.start();
    let res = p.wait_and_collect()
        .unwrap().len();
    assert_eq!(res, 20)
}
```

If we don't want to use the templates offered by the library, there is the possibility to create custom stage.
By creating custom stages it is possible to build stateful and more complex nodes for our pipeline.

Here the same example but using custom defined stages:
```rust
use ppl::prelude::*;

struct Source {
    streamlen: usize,
    counter: usize,
}
impl Out<usize> for Source {
    // This method must be implemented in order to implement the logic of the node
    fn run(&mut self) -> Option<usize> {
        if self.counter < self.streamlen {
            self.counter += 1;
            Some(self.counter)
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct Worker {}
impl InOut<usize, usize> for Worker {
    // This method must be implemented in order to implement the logic of the node
    fn run(&mut self, input: usize) -> Option<usize> {
        Some(fib(input))
    }
    // We can override this method to specify the number of replicas of the stage
    fn number_of_replicas(&self) -> usize {
        8
    }
}

struct Sink {
    counter: usize,
}
impl In<usize, usize> for Sink {
    // This method must be implemented in order to implement the logic of the node
    fn run(&mut self, input: usize) {
        println!("{}", input);
        self.counter += 1;
    }
    // If at the end of the stream We want to return something, We can override the finalize method.
    fn finalize(self) -> Option<usize> {
        println!("End");
        Some(self.counter)
    }
}

fn main() {
    let mut p = parallel![
        Source {
            streamlen: 20,
            counter: 0
        },
        Worker {},
        Sink { counter: 0 }
    ];

    p.start();
    let res = p.wait_and_collect(); // Here We will get the counter returned by the Sink
    assert_eq!(res.unwrap(), 20);
}
```

Surely is more verbose, but allows to express a more variety of parallel computations in which each node can be stateful. Also, thanks to this approach, it is possible to build templates for stages that can be reused in different pipelines and/or projects.

Parallelo also offers a powerful work-stealing threadpool. Using that instead, we can express the same parallel computation as following:

```rust
use ppl::thread_pool::ThreadPool;

fn main() {
    let tp = ThreadPool::new_with_global_registry(8); // We create a new threadpool
    for i in 1..45 {
        tp.execute(move || {
            fib(i);
        });
    }
    tp.wait(); // Wait till al worker have finished
}
```
### A more complex example: Word Counter

To demonstrate the capabilities of Parallelo Parallel Library (PPL), let's consider a common problem: counting the occurrences of words in a text dataset. This example showcases how PPL can significantly speed up computations by leveraging parallelism.

#### Problem

The task is to count the occurrences of each word in a given text dataset. This involves reading the dataset, splitting it into individual words, normalizing the words (e.g., converting to lowercase and removing non-alphabetic characters), and finally, counting the occurrences of each word.

#### Approach 1: Pipeline

The pipeline approach in PPL provides an intuitive and flexible way to express complex parallel computations. In this approach, we construct a pipeline consisting of multiple stages, each performing a specific operation on the data. The data flows through the stages, with parallelism automatically applied at each stage.

In the word counter example, the pipeline involves the following stages:
- **Source**: Reads the dataset and emits lines of text.
- **Map**: Converts each line of text into a list of words, where each word is paired with a count of 1.
- **Reduce**: Aggregates the counts of words by summing them for each unique word.
- **Sink**: Stores the final word counts in a hashmap.

By breaking down the computation into stages and leveraging parallelism, PPL's pipeline approach allows for efficient distribution of work across multiple threads or cores, leading to faster execution.

Here's the Rust code for the word counter using the pipeline approach:

```rust
use ppl::{
    collections::map::{Map, MapReduce, Reduce},
    prelude::*,
};

struct Source {
    reader: BufReader<File>,
}

impl Out<Vec<String>> for Source {
    // Implementation details...
}

struct Sink {
    counter: HashMap<String, usize>,
}
impl In<Vec<(String, usize)>, Vec<(String, usize)>> for Sink {
    fn run(&mut self, input: Vec<(String, usize)>) {
        // Increment value for key in hashmap
        // If key does not exist, insert it with value 1
        for (key, value) in input {
            let counter = self.counter.entry(key).or_insert(0);
            *counter += value;
        }
    }
    fn finalize(self) -> Option<Vec<(String, usize)>> {
        Some(self.counter.into_iter().collect())
    }
}

pub fn ppl(dataset: &str, threads: usize) {
    // Initialization and configuration...

    let mut p = parallel![
        Source { reader },
        Map::build::<Vec<String>, Vec<(String, usize)>>(threads / 2, |str| -> (String, usize) {
            (str, 1)
        }),
        Reduce::build(threads / 2, |str, count| {
            let mut sum = 0;
            for c in count {
                sum += c;
            }
            (str, sum)
        }),
        Sink {
            counter: HashMap::new(),
        },
    ];

    p.start();
    let res = p.wait_and_collect();
}
```
#### Approach 2: Thread Pool

Alternatively, PPL also provides a thread pool for parallel computations. In this approach, we utilize the `par_map_reduce` function of the thread pool to perform the word counting task.

The thread pool approach in PPL involves the following steps:
- Create a thread pool with the desired number of threads.
- Read the dataset and collect all the lines into a vector.
- Use the `par_map_reduce` function to parallelize the mapping and reducing operations on the vector of words.
- The mapping operation converts each word into a tuple with a count of 1.
- The reducing operation aggregates the counts of words by summing them for each unique word.

Using the thread pool, PPL efficiently distributes the work across the available threads, automatically managing the parallel execution and resource allocation.

```rust
use ppl::prelude::*;

pub fn ppl_map(dataset: &str, threads: usize) {
    // Initialization and configuration...

    let file = File::open(dataset).expect("no such file");
    let reader = BufReader::new(file);

    let mut tp = ThreadPool::new_with_global_registry(threads);

    let mut words = Vec::new();

    reader.lines().map(|s| s.unwrap()).for_each(|s| words.push(s));

    let res = tp.par_map_reduce(
        words
            .iter()
            .flat_map(|s| s.split_whitespace())
            .map(|s| {
                s.to_lowercase()
                    .chars()
                    .filter(|c| c.is_alphabetic())
                    .collect::<String>()
            })
            .collect::<Vec<String>>(),
        |str| -> (String, usize) { (str, 1) },
        |str, count| {
            let mut sum = 0;
            for c in count {
                sum += c;
            }
            (str, sum)
        },
    );
}
```

### Advanced Example: Single-Input Multi-Output stage

In this example, we demonstrate how to model a data processing pipeline using PPL, where a stage produces multiple outputs for each input received.

This pipeline involves the following stages:
- **Source**: Generates a stream of 1000 numbers.
- **Worker**: Given a number, it produces multiple copies of that number.
- **Sink**: The `SinkVec` struct acts as a sink stage in the pipeline. It collects the output from the worker stage and stores the results in a vector.

The implementation of the `InOut<usize, usize>` trait overrides the `run` method to process the input received from the source and prepare the worker for producing multiple copies. The `produce` method generates the specified number of copies of the input. By implementing the `is_producer` method and returning `true`, this stage is marked as a producer. Additionally, the `number_of_replicas` method specifies the number of replicas of this worker to create for parallel processing.

```rust
use ppl::{collections::misc::SinkVec, prelude::*};

// Source
struct Source {
    streamlen: usize,
    counter: usize,
}
impl Out<usize> for Source {
    fn run(&mut self) -> Option<usize> {
        if self.counter < self.streamlen {
            self.counter += 1;
            Some(self.counter)
        } else {
            None
        }
    }
}

// Given an input, it produces 5 copies of it.
#[derive(Clone)]
struct Worker {
    number_of_messages: usize,
    counter: usize,
    input: usize,
}
impl InOut<usize, usize> for Worker {
    fn run(&mut self, input: usize) -> Option<usize> {
        self.counter = 0;
        self.input = input;
        None
    }
    fn produce(&mut self) -> Option<usize> {
        if self.counter < self.number_of_messages {
            self.counter += 1;
            Some(self.input)
        } else {
            None
        }
    }
    fn is_producer(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        8
    }
}

fn main() {
    // Create the pipeline using the parallel! macro
    let mut p = parallel![
        Source {
            streamlen: 1000,
            counter: 0
        },
        Worker {
            number_of_messages: 5,
            counter: 0,
            input: 0
        },
        SinkVec::build()
    ];

    // Start the processing
    p.start();

    // Wait for the processing to finish and collect the results
    let res = p.wait_and_collect().unwrap();
}
```

More complex examples are available in *benches/*, *examples/*, and in *tests/*.

## Configuration

The configuration of Parallelo Parallel Library (PPL) can be customized by setting environment variables. The following environment variables are available:

- **PPL_MAX_CORES**: Specifies the maximum number of cores to use. This configuration is only valid when pinning is active.

- **PPL_PINNING**: Enables or disables the threads pinning. By default, pinning is disabled (`false`). (Equivalent to `OMP_PROC_BIND` in OpenMP).

- **PPL_SCHEDULE**: Specifies the scheduling method used in the pipeline. The available options are:
  - `static`: Static scheduling (Round-robin).
  - `dynamic`: Dynamic scheduling (Work-stealing between replicas enabled).

- **PPL_WAIT_POLICY**: If set to `true`, the threads will try to give up their time to other threads when waiting for a communication or unused. This is particularly useful when we are using SMT and can improve performances. If is set to `false`, the threads wil do busy waiting. By default, this option is set to `false`.

- **PPL_THREADS_MAPPING**: Specifies the threads mapping. By default, the threads are mapped in the order in which the cores are found. This option is only valid when pinning is active. (Note that this environment variable is kinda similar to the `OMP_PLACES` environment variable in OpenMP). 
  - Example: `PPL_THREADS_MAPPING=0,2,1,3` will map the threads in the following order: `0 -> core 0`, `1 -> core 2`, `2 -> core 1`, `3 -> core 3`.

To customize the configuration, set the desired environment variables before running your Rust program that uses PPL. For example, you can set the environment variables in your shell script or use a tool to load them from a file.

Please note that changing these configuration options may have an impact on the performance and behavior of your parallel computations. Experiment with different settings to find the optimal configuration for your specific use case.

## Channel Backend

Parallelo Parallel Library (PPL) provides flexibility in choosing the channel backend used for multi-producer, single-consumer communication in the framework. Depending on your requirements and preferences, you can select the desired channel backend at compile time using feature flags.

The default backend in PPL is **crossbeam**.

Crossbeam provides a highly performant channel implementation that provides efficient and reliable communication differents threads. It is well-suited for a wide range of parallel computing scenarios.

Overall, PPL supports the following backends:

- **crossbeam**: Uses the [Crossbeam](https://github.com/crossbeam-rs/crossbeam) channel.
- **flume**: Uses the [Flume](https://github.com/zesterer/flume) channel.
- **kanal**: Uses the [Kanal](https://github.com/fereidani/kanal) channel.
- **ff**: Uses a channel based on [FastFlow](http://calvados.di.unipi.it/) queues (**Experimental**).

To select a specific channel backend, enable the corresponding feature flag during the build process. For example, to use the crossbeam channel, add the following to your Cargo.toml file:

```toml
[dependencies.ppl]
features = ["crossbeam"]
```

### FastFlow Channel
In addition to the others channel backends, PPL also provides an experimental mpsc channel implementation based on FastFlow queues. This backend is available using the `ff` feature flag.
This implementation can be found [here](https://github.com/valebes/ff_buffer) and is substantially based on the [FF Buffer](https://github.com/lucarin91/ff_buffer) by [Luca Rinaldi](https://github.com/lucarin91).

This backend is built around a wrapper for the FastFlow queues that provides a channel-like interface.

In order to use this implementation you must download and install the FastFlow library in the home directory. The library can be downloaded from [here](http://calvados.di.unipi.it/) or as follows:

```bash
cd ~
git clone https://github.com/fastflow/fastflow.git
```

## Benchmarks
The benchmarks are available in the *benches/* directory. To run the benchmarks, use the following command:

```bash
cargo bench
```

For now there are only two benchmarks.
The first, called *image_processing*, is based on the [RustStreamBench](https://github.com/GMAP/RustStreamBench/) Image Processing benchmark that can be found [here](https://github.com/GMAP/RustStreamBench/tree/main/image-processing).
The second is a basic parallel implementation of the well known iterative algorithm to compute the Mandelbrot Set, is called *mandelbrot_set*.

## Contributing

Thank you for considering contributing to Parallelo Parallel Library (PPL)!

If you would like to fix a bug, propose a new feature, or create a new template, you are welcome to do so! Please follow the steps below to contribute to PPL.

### Code of Conduct

Please review our [Code of Conduct](./CODE_OF_CONDUCT.md) before contributing.

### How to Contribute

1. **Create an Issue:** Before starting significant changes, [create an issue](https://github.com/valebes/ppl/issues) to discuss and coordinate efforts.

2. **Fork and Clone:** Fork the repository, then clone it to your local machine.

3. **Branching:** Create a new branch for your changes.

4. **Make Changes:** Implement your changes, write clear commit messages.

5. **Tests:** Add tests to ensure the correctness of your changes.

6. **Run Tests:** Verify that all tests pass successfully.

7. **Submit a Pull Request:** Push your branch and [create a pull request](https://github.com/valebes/ppl/pulls) referencing the relevant issue(s).

8. **Review and Iterate:** Address feedback and make necessary changes.

9. **Celebrate! 🎉:** Once approved and merged, your contributions will be part of PPL!

### Reporting Issues

To report bugs, ask questions, or suggest new ideas, [create an issue](https://github.com/valebes/ppl/issues).

## Warning

 This library is still in an early stage of development. The API is not stable and may change in the future.

## License

Licensed under either of
 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.




