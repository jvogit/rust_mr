use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::SOCKET;

pub struct Worker<'a> {
    id: &'a str,
}

impl<'a> Worker<'a> {
    fn map(&mut self, path: &Path) {
        // READ FROM PARTITION
        // APPLY MAPPER FUNCTION FROM INITIAL KEY/VALUE TO INTERMEDIATE KEY/VALUE SET (SORTED BY KEY)
        // EXPORT MAPPING AND NOTIFY COORDINATOR WHEN DONE
    }

    fn reduce(&mut self, path: &Path) {
        // READ SORTED MAPPED PARTITION
        // APPLY REDUCTION
        // EXPORT REDUCTION AND NOTIFY COORDINATOR WHEN DONE
    }

    pub fn run(id: &'a str) {
        let worker = Worker { id };
        worker.do_rpc("register").expect("register failed");

        loop {
            let res = worker.do_rpc("keep-alive").expect("keep-alive rpc failed");

            thread::sleep(Duration::from_secs(10));
        }
    }

    pub fn do_rpc(&self, rpc_call: &str) -> std::io::Result<String> {
        let mut stream = UnixStream::connect(Path::new(SOCKET)).unwrap();
        
        println!("Sending {}", rpc_call);
        
        stream.write(format!("{}\n{}", self.id, rpc_call).as_bytes())?;
        
        stream.shutdown(std::net::Shutdown::Write).unwrap();

        let mut res = String::new();
        stream.read_to_string(&mut res)?;

        stream.shutdown(std::net::Shutdown::Both).expect("Shutdown failed");
        
        println!("Received {}", res);

        Ok(res)
    }
}
