use std::{io::Write, os::unix::net::UnixStream, path::Path, thread, time::Duration};

use crate::SOCKET;

pub struct Worker<'a> {
    id: &'a str,
    stream: UnixStream,
}

impl<'a> Worker<'a> {
    pub fn run(id: &'a str) {
        let socket_path = Path::new(SOCKET);
        let stream = UnixStream::connect(socket_path).expect("Unix Stream failed!");
        let mut worker = Worker { id, stream };

        worker.write_str(worker.id);

        loop {
            worker.write_str("keep alive");
            thread::sleep(Duration::from_secs(1));
        }
    }

    pub fn write_str(&mut self, str: &str) {
        self.stream
            .write(format!("{}\n", str).as_bytes())
            .expect("Write failed!");
    }
}
