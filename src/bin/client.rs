use std::{io::Write, os::unix::net::UnixStream, path::Path};

fn main() {
    let socket_path = Path::new("mysocket");

    let mut unix_stream = UnixStream::connect(socket_path).expect("Unix Stream failed!");

    unix_stream.write(b"Hello World").unwrap();
}
