use std::{
    io::{BufRead, BufReader},
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
                thread::spawn(|| Self::handle_client(stream));
            } else {
                break;
            }
        }
    }

    fn handle_client(stream: UnixStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut stream = BufReader::new(stream);
        let mut id = String::new();

        stream.read_line(&mut id).expect("Reading ID failed");

        // trim newline after
        let id = id.trim();

        println!("Registered worker {}", id);

        for line in stream.lines() {
            if let Ok(line) = line {
                println!("Received: {} Hello", line);
            } else {
                println!("{} Timed out!", id);
                break;
            }
        }

        println!("{} Stream closed", id);
    }
}
