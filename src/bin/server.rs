use std::{io::Read, os::unix::net::UnixListener, path::Path};

fn main() {
    let socket_path = Path::new("mysocket");

    // copy-paste this and don't think about it anymore
    // it will be hidden from there on
    if socket_path.exists() {
        println!("A socket is already present. Deleting...");
        std::fs::remove_file(socket_path).unwrap();
    }

    let unix_listener = UnixListener::bind(socket_path).expect("UnixListener failed to bind.");

    for incoming in unix_listener.incoming() {
        if let Ok(mut stream) = incoming {
            let mut msg = String::new();
            stream.read_to_string(&mut msg).expect("Failed to read");
            println!("Received from client: {}", msg);
        } else {
            break;
        }
    }
}
