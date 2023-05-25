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
- [Configuration](#configuration)
- [Channel Implementation](#channel-implementation)
- [Contributing](#contributing)
  - [Code of Conduct](#code-of-conduct)
  - [How to Contribute](#how-to-contribute)
  - [Reporting Issues](#reporting-issues)
- [License](#license)


## Features

- **Parallel Computing**: Unlock the power of parallelism in Rust with the Parallelo Parallel Library (PPL). Harness the potential of multiple cores to make your computations faster and more efficient.
- **Essential Tools**: PPL offers a variety of essential tools for parallel computing, including a work-stealing thread pool, pipelines, farms, and other parallel skeletons. These tools allow you to express complex parallel computations with ease.
- **NUMA Awareness**: Take advantage of PPL's NUMA awareness to optimize the utilization of your system's resources. Customize aspects such as maximum parallelism, thread wait policies, and mapping of threads to physical cores for enhanced performance and efficiency.
- **Flexible Channel Implementations**: Choose from a range of flexible channel implementations in PPL to enable seamless communication and coordination between parallel tasks. Select the channel type that suits your application requirements, ensuring smooth data flow throughout your parallel computations.
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
* A Source, that emits number from 1 to 20.
* A Stage that computes the i-th number of Fibonacci.
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

In this example We're building a simple pipeline, so why not use clousure instead than the library templates?
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

If We want to parallelize the computation We must find a part of this algorithm that can be parallelized.
In this case the stage in the middle is a good candidate, We replicate that stage introducing a Farm.

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

If We don't want to use the templates offered by the library, there is the possibility to create custom stage.
By creating custom stages It is possible to add state and create more complicate nodes for our pipeline.

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

Surely is more verbose, but allows to express a more variety of parallel computations in which each node can be stateful.

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

More complex examples are available in *benches/*, *examples/*, and in *tests/*.

## Configuration

The configuration of Parallelo Parallel Library (PPL) can be customized by setting environment variables. The following environment variables are available:

- **PPL_MAX_CORES**: Specifies the maximum number of cores allowed. This configuration is only valid when pinning is active.

- **PPL_PINNING**: Enables or disables the pinning of partitions to the cores. By default, pinning is disabled (`false`).

- **PPL_SCHEDULE**: Specifies the scheduling method used in the pipeline. The available options are:
  - `static`: Static scheduling (default).
  - `dynamic`: Dynamic scheduling.

- **PPL_WAIT_POLICY**: Enables or disables the blocking channel. By default, the blocking channel is disabled (`false`).

- **PPL_THREAD_MAPPING**: Specifies the core mapping. By default, the threads are mapped in the order in which the cores are found.

To customize the configuration, set the desired environment variables before running your Rust program that uses PPL. For example, you can set the environment variables in your shell script or use a tool like `env_file` to load them from a file.

Please note that changing these configuration options may have an impact on the performance and behavior of your parallel computations. Experiment with different settings to find the optimal configuration for your specific use case.

## Channel Implementation

Parallelo Parallel Library (PPL) provides flexibility in choosing the channel implementation for multi-producer, single-consumer communication. Depending on your requirements and preferences, you can select the desired channel implementation at compile time using feature flags.

The default channel implementation in PPL is **crossbeam**.

Crossbeam is a highly performant channel implementation that provides efficient and reliable communication differents threads. It is well-suited for a wide range of parallel computing scenarios.

The available channel implementations are:

- **crossbeam**: Uses the crossbeam channel.
- **flume**: Uses the flume channel.
- **kanal**: Uses the kanal channel.
- **ff**: Uses a channel based on fastflow spsc queues (**EXP**).

To select a specific channel implementation, enable the corresponding feature flag during the build process. For example, to use the crossbeam channel implementation, add the following to your Cargo.toml file:

```toml
[dependencies.ppl]
features = ["crossbeam"]
```

## Contributing

Thank you for considering contributing to Parallelo Parallel Library (PPL)!

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




