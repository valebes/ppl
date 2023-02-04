use pspp::{
    node::{
        in_node::{In, InNode},
        inout_node::{InOut, InOutNode},
        out_node::{Out, OutNode},
    },
    pipeline,
    pipeline::Pipeline,
    pipeline_propagate,
};
/**
    Matrix * Vector multiplication
    A: n*n matrix
    b: n vector
*/
use rand::Rng;

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

#[derive(Clone)]
struct Multiplication {
    vec: Vec<i64>,
}
impl InOut<Vec<i64>, i64> for Multiplication {
    fn run(&mut self, input: Vec<i64>) -> Option<i64> {
        Some(mult(input, &self.vec))
    }
    fn ordered(&self) -> bool {
        true
    }
    fn number_of_replicas(&self) -> usize {
        6
    }
}

struct Sink {
    res: Vec<i64>,
}
impl In<i64, Vec<i64>> for Sink {
    fn run(&mut self, input: i64) {
        self.res.push(input);
    }
    fn ordered(&self) -> bool {
        true
    }
    fn finalize(&mut self) -> Option<Vec<i64>> {
        Some(self.res.clone())
    }
}

fn mult(m: Vec<i64>, v: &Vec<i64>) -> i64 {
    let mut res = 0;
    for i in 0..m.len() {
        res = res + m[i] * v[i];
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
    let p = pipeline![
        Box::new(Source {
            matrix: matrix.clone()
        }),
        Box::new(Multiplication { vec: vec.clone() }),
        Box::new(Sink { res: vec![] })
    ];

    // Collect the results of the computation
    let res = p.collect().unwrap();

    // Compute the "reference" result
    for i in 0..matrix.len() {
        for j in 0..vec.len() {
            correct_result[i] = correct_result[i] + matrix[i][j] * vec[j];
        }
    }

    assert_eq!(res, correct_result);
}
