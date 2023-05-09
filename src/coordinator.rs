use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    io::{BufRead, BufReader, BufWriter, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

use crate::SOCKET;

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
    R: usize,
    workers: HashSet<Worker>,
    workers_keep_alive: HashMap<Worker, Instant>,
    workers_task: HashMap<Worker, Task>,
    pending_tasks: BinaryHeap<Task>,
}

impl Coordinator {
    pub fn run() {
        let socket_path = Path::new(SOCKET);
        let coordinator = Arc::new(Mutex::new(Coordinator {
            M: 1,
            R: 1,
            workers: HashSet::new(),
            workers_task: HashMap::new(),
            workers_keep_alive: HashMap::new(),
            pending_tasks: BinaryHeap::new(),
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
        let mut id = String::new();

        in_stream.read_line(&mut id).expect("Reading ID failed");

        // trim newline after
        let id = id.trim().parse::<usize>().unwrap();
        let worker = Worker(id);

        let mut rpc_call = String::new();
        in_stream
            .read_line(&mut rpc_call)
            .expect("Reading RPC call failed");

        println!("RPC from worker {} : {}", id, rpc_call);

        match &rpc_call[..] {
            "register" => {
                let mut this = this.lock().unwrap();

                if !this.workers.insert(worker) {
                    panic!("Tried to register exisitng worker!")
                }
                this.workers_task.remove(&worker);
                this.workers_keep_alive.insert(worker, Instant::now());

                Self::write_res("register res", &mut out_stream);
            }
            "steal-work" => {
                let mut this = this.lock().unwrap();

                if let Some(task) = this.pending_tasks.pop() {
                    match task {
                        Task::MAP(at, _) => {
                            this.workers_task
                                .insert(worker, Task::MAP(at.clone(), TaskStatus::RUNNING));
                            this.workers_keep_alive.insert(worker, Instant::now());
                            Self::write_res(format!("map {}", at), &mut out_stream);
                        }
                        Task::REDUCE(at, _) => {
                            this.workers_task
                                .insert(worker, Task::REDUCE(at.clone(), TaskStatus::RUNNING));
                            this.workers_keep_alive.insert(worker, Instant::now());
                            Self::write_res(format!("reduce {}", at), &mut out_stream);
                        }
                    }
                } else {
                    Self::write_res("nowork", &mut out_stream);
                }
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

    fn write_res<S>(res: S, stream: &mut BufWriter<UnixStream>)
    where
        S: Into<String>,
    {
        stream
            .write(res.into().as_bytes())
            .expect("Write res failed!");
    }
}
