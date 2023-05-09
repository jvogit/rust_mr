use std::env;

use rust_mr::worker::Worker;

fn main() {
    let args: Vec<String> = env::args().collect();
    let id = &args[1][..];

    Worker::run(id);
}
