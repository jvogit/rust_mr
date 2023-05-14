use std::{
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    io::{BufRead, BufReader, BufWriter, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

use crate::{coordinator, SOCKET};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Task {
    MAP(String, TaskStatus),
    REDUCE(String, TaskStatus),
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum TaskStatus {
    IDLE,
    RUNNING,
    DONE,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
struct Worker(usize);

pub struct Coordinator {
    M: usize,
    workers: HashSet<Worker>,
    workers_keep_alive: HashMap<Worker, Instant>,
    workers_task: HashMap<Worker, Task>,
    map_tasks: VecDeque<Task>,
    reduce_tasks: Vec<VecDeque<Task>>,
}

impl Coordinator {
    pub fn R(&self) -> usize {
        self.reduce_tasks.len()
    }

    pub fn run() {
        let M = 1;
        let R = 1;
        let socket_path = Path::new(SOCKET);
        let coordinator = Arc::new(Mutex::new(Coordinator {
            M,
            workers: HashSet::new(),
            workers_task: HashMap::new(),
            workers_keep_alive: HashMap::new(),
            // hard code a map task (test-wc.txt)
            map_tasks: VecDeque::from([Task::MAP("test-wc.txt".to_string(), TaskStatus::IDLE)]),
            // hardcode R = 1
            reduce_tasks: {
                let mut reduce_tasks = Vec::with_capacity(R);

                for _ in 0..R {
                    reduce_tasks.push(VecDeque::new());
                }

                reduce_tasks
            },
        }));

        if socket_path.exists() {
            println!("A socket is already present. Deleting...");
            std::fs::remove_file(socket_path).unwrap();
        }

        let unix_listener = UnixListener::bind(socket_path).expect("UnixListener failed to bind.");

        for incoming in unix_listener.incoming() {
            if let Ok(stream) = incoming {
                let coordinator = coordinator.clone();
                thread::spawn(move || Self::handle_rpc(coordinator, stream));
            } else {
                break;
            }
        }
    }

    fn handle_rpc(this: Arc<Mutex<Self>>, stream: UnixStream) {
        let mut out_stream = BufWriter::new(stream.try_clone().unwrap());
        let mut in_stream = BufReader::new(stream.try_clone().unwrap());

        // trim newline after
        let id = Self::read_line(&mut in_stream)
            .trim()
            .parse::<usize>()
            .expect("id is not usize");
        let worker = Worker(id);

        let rpc_call = Self::read_line(&mut in_stream);

        println!("RPC from worker {} : {}", id, rpc_call);

        match rpc_call.trim() {
            "register" => {
                let mut this = this.lock().unwrap();

                if !this.workers.insert(worker) {
                    panic!("Tried to register existing worker!")
                }
                this.workers_task.remove(&worker);
                this.workers_keep_alive.insert(worker, Instant::now());

                Self::write_res("register res", &mut out_stream);
            }
            "steal-work" => {
                let mut this = this.lock().unwrap();

                this.workers_keep_alive.insert(worker, Instant::now());

                if let Some(Task::MAP(map_at, _)) = this.map_tasks.pop_front() {
                    Self::write_res(format!("map\n{}\n{}", map_at, this.R()), &mut out_stream);
                    this.workers_task
                        .insert(worker, Task::MAP(map_at, TaskStatus::RUNNING));
                    this.workers_keep_alive.insert(worker, Instant::now());
                } else if id < this.R() && this.reduce_tasks[id].len() == this.M {
                    // mappers have finished mapping
                    // its reducing time
                    let mut reduce_at = String::new();

                    while let Some(Task::REDUCE(r_at, _)) = this.reduce_tasks[id].pop_front() {
                        reduce_at.push_str(&r_at);
                        reduce_at.push('\n');
                    }

                    Self::write_res(format!("reduce\n{}", reduce_at.trim()), &mut out_stream);
                    this.workers_task.insert(
                        worker,
                        Task::REDUCE(reduce_at.trim().to_string(), TaskStatus::RUNNING),
                    );
                    this.workers_keep_alive.insert(worker, Instant::now());
                } else {
                    Self::write_res("nowork", &mut out_stream);
                }
            }
            "finish" => {
                let mut this = this.lock().unwrap();

                this.workers_keep_alive.insert(worker, Instant::now());

                match this
                    .workers_task
                    .remove(&worker)
                    .expect("Worker to have task")
                {
                    Task::MAP(map_at, _) => {
                        this.workers_task
                            .insert(worker, Task::MAP(map_at, TaskStatus::DONE));

                        for r_i in 0..this.R() {
                            let at = Self::read_line(&mut in_stream);
                            let at = at.trim().to_string();
                            this.reduce_tasks[r_i].push_back(Task::REDUCE(at, TaskStatus::IDLE));
                        }
                    }
                    Task::REDUCE(map_at, _) => {
                        let at = Self::read_line(&mut in_stream);
                        let at = at.trim().to_string();
                        println!("Done: {}", at);
                        this.workers_task
                            .insert(worker, Task::REDUCE(map_at, TaskStatus::DONE));
                    }
                }

                Self::write_res("finish res", &mut out_stream);
            }
            "keep-alive" => {
                let mut this = this.lock().unwrap();
                this.workers_keep_alive.insert(Worker(id), Instant::now());

                Self::write_res("keep-alive res", &mut out_stream);
            }
            _ => {
                println!("Unknown RPC call {}", rpc_call);
            }
        }

        println!("{} Stream closed", id);
    }

    fn read_line(stream: &mut BufReader<UnixStream>) -> String {
        let mut res = String::new();

        stream.read_line(&mut res).expect("Read failed!");

        res
    }

    fn write_res<S>(res: S, stream: &mut BufWriter<UnixStream>)
    where
        S: Into<String>,
    {
        stream
            .write(res.into().as_bytes())
            .expect("Write res failed!");
    }
}
