# MapReduce implementation
Implementation based on MapReduce paper in Rust for learning purposes.

1. Run coordinator 
```rust
cargo run --bin run_coordinator
```

2. Run worker (id must be usize and must start from 0 and be consecutive. Additionally there must be at least R workers)
```rust
cargo run --bin run_worker -- <id>
```

## Architecture
### Map Phase
1. Coordinator receives map tasks. A map task applies the mapper function to partition of the input data.
2. Worker periodically pings cooridnator for map tasks. The coordinator assigns an available map task to the worker
3. The worker takes the map task and applies the mapper functon. After, the worker partitions the intermediate key/values by the hash of the key (hash(key) % R where R is the amount of reducers). The worker produces R files each for a specific reducer. Each file/partition is sorted by key.
4. The worker submits a reduce task to the coordinator

### Reduce Phase
1. Coordinator receives a reduce task. A reduce task applies a reducer function to the intermediate key/value pair. (Example: accumulating the total word count where the intermediate key/value is the word and the count produced by the mapper function)
2. A worker assigned to a reducer task by hash value pings the coordinator. This happens only if there is no other mapper tasks. This means the mapping phase is complete. This means the reducer worker can process all available intermediate key/value. The coordinator assigns the reducer task to the worker.
3. The worker gathers all sorted partitions for reduction. The worker merges the sorted partition.
4. The worker applies the reduction function. The worker outputs the final output in a file.
5. The worker submits the file to the coordinator.

### Coordinator High Level Flow
0. Coordinator is specified the amount of workers needed, M (the amount of input partitions), R (the amount of reducers)
1. Sets up UNIX socket listener to listen for RPC calls for workers
2. Workers must register with the coordinator and periodically ping the coordinator to keep-alive
3. There must be at least R workers. Worker IDs must be labaelled starting from 0 and be consecutive. Worker IDs 0..R are reducer workers.
4. Reducer workers are the R reducers that will process one of the R partitions of the intermediate key/value pairs after the mapping phase.
5. Reducer workers only start after the mapping phase

### Worker High Level Flow
0. Worker must register with coordinator via UNIX socket RPC
1. Worker periodically tries to steal any available work from the coordinator
2. A worker can be in a map task, reduction task, or sit idle waiting for a task
3. In a map task, the worker receives one partition of the input data to map. THen outputs R sorted partitions of intermediate key/valye by hash of the intermediate key.
4. In a reduction task, the worker receives R sorted parittions. The worker must merge all sorted partitions and then apply the reduction function outputting one output file of the reduction phase.