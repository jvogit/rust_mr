use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::SOCKET;

enum TaskStatus {
    IDLE,
    MAP,
    REDUCE,
}

pub struct Worker<'a> {
    id: &'a str,
    status: TaskStatus,
}

impl<'a> Worker<'a> {
    fn do_map(&mut self, path: &Path, R: usize) {
        // READ FROM PARTITION
        let lines = Self::read_lines(path).expect("failed to read partition");

        // hardocded mapper function emits word with count of 1
        fn map(key: &str) -> (String, usize) {
            (key.to_string(), 1)
        }

        // APPLY MAPPER FUNCTION FROM INITIAL KEY/VALUE TO INTERMEDIATE KEY/VALUE SET (SORTED BY KEY)
        let lines = lines.map(|s| map(&s.unwrap()));
        // SCATTER OPERATION BY COMPUTING HASH INDEX AND PUSHING INTERMEDIATE KEY TO DESIGNATED BUCKET
        let mut chunks: Vec<Vec<(String, usize)>> = vec![vec![]; R];
        lines.for_each(|(k, v)| {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::Hasher;

            let mut hasher = DefaultHasher::new();
            hasher.write(k.as_bytes());

            let hash_index = hasher.finish() as usize % R;

            chunks[hash_index].push((k, v));
        });

        // EXPORT MAPPING AND NOTIFY COORDINATOR WHEN DONE
        let output_names = chunks
            .into_iter()
            .enumerate()
            .map(|(i, mut chunk)| {
                let output_name = format!("{}-{}-map.txt", self.id, i);
                let mut output = File::create(&output_name[..]).expect("failed to create output");

                chunk.sort();

                chunk
                    .into_iter()
                    .for_each(|(k, v)| writeln!(output, "{},{}", k, v).expect("write failed!"));

                output_name
            })
            .collect::<Vec<String>>()
            .join("\n");

        self.do_rpc(format!("finish\n{}", output_names.trim()))
            .expect("finish call failed!");
        self.status = TaskStatus::IDLE;
    }

    fn do_reduce(&mut self, partition_names: &[&str]) {
        // READ SORTED MAPPED PARTITIONS (merge individual partitions)
        let mut partitions = Vec::with_capacity(partition_names.len());
        for partition in partition_names {
            let path = Path::new(partition);
            let lines = Self::read_lines(path)
                .expect("failed to read partition")
                .map(|s| {
                    let s = s.unwrap();
                    let mut s = s.split(',');
                    let (k, v) = (
                        s.next().unwrap().to_string(),
                        s.next().unwrap().parse::<usize>().unwrap(),
                    );

                    (k, v)
                })
                .collect();

            partitions.push(lines);
        }
        let lines = Merge::merge_sorted_k(partitions);

        fn reduce(key: &String, values: &[(String, usize)]) -> String {
            // hardocded reduce function that simply totals the counts (which is the value) of the key
            let total_count: usize = values.iter().map(|(_, value)| value).sum();

            format!("{},{}", key, total_count)
        }

        // APPLY REDUCTION
        let output_name = format!("{}-reduce.txt", self.id);
        let mut file = File::create(&output_name).unwrap();
        let mut enumerator = lines.iter().enumerate().peekable();

        while let Some((start, (k, _))) = enumerator.next() {
            let mut end = start;

            while let Some((_, (k_o, _))) = enumerator.peek() {
                if k != k_o {
                    break;
                }

                end += 1;

                enumerator.next();
            }
            let reduced = reduce(k, &lines.as_slice()[start..=end]);

            writeln!(file, "{}", reduced).expect("Write failed");
        }
        // EXPORT REDUCTION AND NOTIFY COORDINATOR WHEN DONE

        self.do_rpc(format!("finish\n{}", output_name))
            .expect("finish rpc call failed!");
        self.status = TaskStatus::IDLE;
    }

    pub fn run(id: &'a str) {
        let mut worker = Worker {
            id,
            status: TaskStatus::IDLE,
        };
        worker.do_rpc("register").expect("register failed");

        loop {
            worker.do_work();
            worker.do_rpc("keep-alive").expect("keep-alive failed!");
            thread::sleep(Duration::from_secs(10));
        }
    }

    fn do_work(&mut self) {
        match self.status {
            TaskStatus::IDLE => {
                let res = self.do_rpc("steal-work").expect("steal-work failed");
                let res: Vec<&str> = res.split('\n').collect();

                match res[0] {
                    "nowork" => return,
                    "map" => {
                        let (at, R) = (res[1], res[2].parse::<usize>().unwrap());
                        self.status = TaskStatus::MAP;
                        // TODO: async mapping. For now it is synchronous for testing
                        self.do_map(&Path::new(at), R);
                    }
                    "reduce" => {
                        self.status = TaskStatus::REDUCE;

                        self.do_reduce(&res.as_slice()[1..])
                    }
                    _ => {
                        println!("Unknown res")
                    }
                }

                if res[0] == "nowork" {
                    return;
                }
            }
            _ => {}
        }
    }

    fn do_rpc<S>(&self, rpc_call: S) -> std::io::Result<String>
    where
        S: Into<String>,
    {
        let rpc_call = rpc_call.into();
        let mut stream = UnixStream::connect(Path::new(SOCKET)).unwrap();

        println!("Sending {}", rpc_call);

        stream.write(format!("{}\n{}", self.id, rpc_call).as_bytes())?;

        stream.shutdown(std::net::Shutdown::Write).unwrap();

        let mut res = String::new();
        stream.read_to_string(&mut res)?;

        stream
            .shutdown(std::net::Shutdown::Both)
            .expect("Shutdown failed");

        println!("Received {}", res);

        Ok(res)
    }

    fn read_lines(path: &Path) -> std::io::Result<std::io::Lines<BufReader<File>>> {
        let file = File::open(path)?;

        Ok(BufReader::new(file).lines())
    }
}

struct Merge {}

impl Merge {
    fn merge_sorted(a: Vec<(String, usize)>, b: Vec<(String, usize)>) -> Vec<(String, usize)> {
        let mut res = Vec::with_capacity(a.len() + b.len());
        let (mut a, mut b) = (a.into_iter(), b.into_iter());
        let (mut a_i, mut b_i) = (a.next(), b.next());

        while a_i.is_some() || b_i.is_some() {
            if b_i.is_none() || a_i.is_some() && a_i.as_ref().unwrap() <= b_i.as_ref().unwrap() {
                res.push(a_i.unwrap());
                a_i = a.next();
            } else {
                res.push(b_i.unwrap());
                b_i = b.next();
            }
        }

        res
    }

    fn merge_sorted_k(mut k: Vec<Vec<(String, usize)>>) -> Vec<(String, usize)> {
        match k.len() {
            0 => vec![],
            1 => k.remove(0),
            n => {
                let b = k.split_off(n / 2);
                Self::merge_sorted(Self::merge_sorted_k(k), Self::merge_sorted_k(b))
            }
        }
    }
}
