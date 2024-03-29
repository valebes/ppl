/**
    Matrix * Vector multiplication
    A: n*n matrix
    b: n vector
*/
use ppl::prelude::*;
use rand::Rng;

// Taken a matrix, this source emit a row of the matrix at a time.
struct Source {
    matrix: Vec<Vec<i64>>,
}
impl Out<Vec<i64>> for Source {
    fn run(&mut self) -> Option<Vec<i64>> {
        if !self.matrix.is_empty() {
            return Some(self.matrix.remove(0));
        }
        None
    }
}

// Given a vector, this stage multiply it by the vector stored in its state.
#[derive(Clone)]
struct Multiplication {
    vec: Vec<i64>,
}
impl InOut<Vec<i64>, i64> for Multiplication {
    fn run(&mut self, input: Vec<i64>) -> Option<i64> {
        Some(mult(input, &self.vec))
    }
    // This stage is ordered because the multiplication is done in order.
    fn is_ordered(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        6
    }
}

// This sink collect the results of the multiplication.
// It is possible to use an OrderedSinkVec instead of this custom sink.
struct Sink {
    res: Vec<i64>,
}
impl In<i64, Vec<i64>> for Sink {
    fn run(&mut self, input: i64) {
        self.res.push(input);
    }
    fn is_ordered(&self) -> bool {
        true
    }
    fn finalize(self) -> Option<Vec<i64>> {
        Some(self.res)
    }
}

fn mult(m: Vec<i64>, v: &[i64]) -> i64 {
    let mut res = 0;
    for i in 0..m.len() {
        res += m[i] * v[i];
    }
    res
}

#[test]
fn test_mxv() {
    env_logger::init();

    let n = 1000; //rows
    let mut matrix = vec![vec![0; n]; n];
    let mut vec = vec![0; n];
    let mut correct_result = vec![0; n];

    for i in 0..n {
        vec[i] = rand::thread_rng().gen_range(0..10);
        for j in 0..n {
            matrix[i][j] = rand::thread_rng().gen_range(0..10);
        }
    }

    // Build and start the spp graph
    let mut p = pipeline![
        Source {
            matrix: matrix.clone()
        },
        Multiplication { vec: vec.clone() },
        Sink { res: vec![] }
    ];

    p.start();

    // Collect the results of the computation
    let res = p.wait_end().unwrap();

    // Compute the "reference" result
    for i in 0..matrix.len() {
        (0..vec.len()).for_each(|j| {
            correct_result[i] += matrix[i][j] * vec[j];
        });
    }

    assert_eq!(res, correct_result);
}
