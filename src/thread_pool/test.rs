#![cfg(test)]

use std::sync::{Arc, Mutex};

use super::ThreadPool;
use crate::core::orchestrator::Orchestrator;
use serial_test::serial;

fn fib(n: i32) -> u64 {
    if n < 0 {
        panic!("{} is negative!", n);
    }
    match n {
        0 => panic!("zero is not a right argument to fib()!"),
        1 | 2 => 1,
        3 => 2,
        _ => fib(n - 1) + fib(n - 2),
    }
}

#[test]
#[serial]
fn test_threadpool() {
    let tp = ThreadPool::new_with_global_registry(8);
    for i in 1..45 {
        tp.execute(move || {
            fib(i);
        });
    }
    tp.wait();
    Orchestrator::delete_global_orchestrator();
}

#[test]
#[serial]
fn test_scoped_thread() {
    let mut vec = vec![0; 100];
    let mut tp = ThreadPool::new_with_global_registry(8);

    tp.scoped(|s| {
        for e in vec.iter_mut() {
            s.execute(move || {
                *e += 1;
            });
        }
    });
    Orchestrator::delete_global_orchestrator();
    assert_eq!(vec, vec![1i32; 100])
}

#[test]
#[serial]
fn test_par_for_each() {
    let mut vec = vec![0; 100];
    let mut tp = ThreadPool::new_with_global_registry(8);

    tp.par_for_each(&mut vec, |el: &mut i32| *el += 1);
    Orchestrator::delete_global_orchestrator();
    assert_eq!(vec, vec![1i32; 100])
}

#[test]
#[serial]
fn test_par_map() {
    let mut vec = Vec::new();
    let mut tp = ThreadPool::new_with_global_registry(16);

    for i in 0..10000 {
        vec.push(i);
    }
    let res: Vec<String> = tp
        .par_map(vec, |el| -> String {
            "Hello from: ".to_string() + &el.to_string()
        })
        .collect();

    let mut check = true;
    for (i, str) in res.into_iter().enumerate() {
        if str != "Hello from: ".to_string() + &i.to_string() {
            check = false;
        }
    }
    Orchestrator::delete_global_orchestrator();
    assert!(check)
}

#[test]
#[serial]
fn test_par_for() {
    let mut tp = ThreadPool::new_with_global_registry(8);

    let vec = {
        let mut v = Vec::with_capacity(100);
        (0..100).for_each(|_| v.push(Arc::new(Mutex::new(0))));
        v
    };

    tp.par_for(0..100, 2, |i| {
        let mut lock = vec[i].lock().unwrap();
        *lock += 1;
    });

    let mut check = true;

    (0..100).for_each(|i| {
        let lock = vec[i].lock().unwrap();
        if *lock != 1 {
            check = false;
        }
    });

    Orchestrator::delete_global_orchestrator();
    assert!(check)
}

// Test par_map_reduce
#[test]
#[serial]
fn test_par_map_reduce() {
    let mut vec = Vec::new();
    let mut tp = ThreadPool::new_with_global_registry(16);

    for _i in 0..100000 {
        for i in 0..10 {
            vec.push(i);
        }
    }

    let res = tp.par_map_reduce(
        vec,
        |el| -> (i32, i32) { (el, 1) },
        |k, v| (k, v.iter().sum::<i32>()),
    );

    let mut check = true;
    for (k, v) in res {
        if v != 100000 {
            check = false;
        }
        println!("Key: {} Total: {}", k, v)
    }

    Orchestrator::delete_global_orchestrator();
    assert!(check)
}

#[test]
#[serial]
fn test_par_map_reduce_seq() {
    let mut vec = Vec::new();
    let mut tp = ThreadPool::new_with_global_registry(16);

    for _i in 0..100000 {
        for i in 0..10 {
            vec.push(i);
        }
    }

    let res = tp.par_map(vec, |el| -> (i32, i32) { (el, 1) });
    let res = tp.par_reduce(res, |k, v| (k, v.iter().sum::<i32>()));

    let mut check = true;
    for (k, v) in res {
        if v != 100000 {
            check = false;
        }
        println!("Key: {} Total: {}", k, v)
    }

    Orchestrator::delete_global_orchestrator();
    assert!(check)
}

#[test]
#[serial]
fn test_multiple_threadpool() {
    let tp_1 = ThreadPool::new_with_global_registry(4);
    let tp_2 = ThreadPool::new_with_global_registry(4);
    ::scopeguard::defer! {
        tp_1.wait();
        tp_2.wait();

    }
    Orchestrator::delete_global_orchestrator();
}