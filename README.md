# MapReduce implementation
Implementation based on MapReduce paper in Rust for learning purposes.

1. Run coordinator 
```rust
cargo run --bin run_coordinator
```

2. Run worker (id must be usize and must start from 0 and be consecutive. Additionally there must be at least R workers)
```rust
cargo run --bin run_worker <id>
```

## Architecture