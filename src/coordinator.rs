use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::Path,
    thread,
    time::Duration,
};

use crate::SOCKET;

pub struct Coordinator {}

impl Coordinator {
    pub fn run() {
        let socket_path = Path::new(SOCKET);

        if socket_path.exists() {
            println!("A socket is already present. Deleting...");
            std::fs::remove_file(socket_path).unwrap();
        }

        let unix_listener = UnixListener::bind(socket_path).expect("UnixListener failed to bind.");

        for incoming in unix_listener.incoming() {
            if let Ok(stream) = incoming {
                thread::spawn(|| Self::handle_rpc(stream));
            } else {
                break;
            }
        }
    }

    fn handle_rpc(stream: UnixStream) {
        let mut out_stream = BufWriter::new(stream.try_clone().unwrap());
        let mut in_stream = BufReader::new(stream.try_clone().unwrap());
        let mut id = String::new();

        in_stream.read_line(&mut id).expect("Reading ID failed");

        // trim newline after
        let id = id.trim();

        println!("RPC from worker {}", id);

        let mut rpc_call = String::new();
        in_stream
            .read_line(&mut rpc_call)
            .expect("Reading RPC call failed");


        match &rpc_call[..] {
            "keep-alive" => {
                out_stream
                    .write(format!("{} keep-alive", id).as_bytes())
                    .expect("keep-alive res failed");
            }
            _ => {
                println!("Unknown RPC call {}", rpc_call);
            }
        }

        println!("{} Stream closed", id);
    }
}
