/*
 * The One Billion Row challenge: https://www.morling.dev/blog/one-billion-row-challenge/
 *
 * Task:
 *
 * Calculate the minimum, mean, and maximum temperature for each city in the list.
 * The complicating factor, as suggested, is that there are One Billion Rows.
 *
 * Each row is sensibly-formatted utf-8, so no issues there. We have further known constraints:
 * 12 GB file
 * 8 cores (prod), 6 cores (test)
 * 32 GB RAM (prob), 8 GB RAM (test).
 *   This means the file *cannot* be fully loaded into memory.
 * However, these are set ahead of time.
 *
 * Approach:
 *
 *  - Split into ~1 GB chunks (Actually split on next newline, line lengths are low).
 *  - Distribute each chunk to a thread. (i.e. read then move)
 *  - Threads create their own City -> (Min, Mean, Max, count) mapping.
 *  - Finally aggregate the results from each thread.
 */

use memmap2::Mmap;
use std::{
    cmp::min,
    collections::HashMap,
    env::args,
    fmt,
    fmt::{Display, Formatter},
    fs::File,
    // io::{Read, Seek, SeekFrom},
    str::FromStr,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
};

const CHUNK_SIZE: usize = 100_000_000;

struct Aggregate {
    result: HashMap<String, CityInfo>,
}

#[derive(Clone, Debug)]
struct CityInfo {
    min: f64,
    mean: f64,
    max: f64,
    count: usize,
}

struct Measurement {
    name: String,
    temp: f64,
}

struct SplitReader<'f> {
    f: &'f [u8],
    cursor: usize,
}

#[derive(Debug)]
struct MeasurementParseError;

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let printable = self.result.iter().map(|(key, value)| format!("{}={}\n", key, value))
            .collect::<Vec<_>>()
            .join("\n");
        write!(f, "{}", printable)
    }
}

impl Display for CityInfo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, self.mean, self.max)
    }
}

impl FromStr for Measurement {
    type Err = MeasurementParseError;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let mut l = line.split(';');
        let name = l.next().expect("bad line: no delimiter").to_string();
        let temp = l
            .next()
            .expect("bad line: no value")
            .parse::<f64>()
            .expect("bad line: invalid value");
        Ok(Self { name, temp })
    }
}

impl Aggregate {
    fn new(partials: Vec<HashMap<String, CityInfo>>) -> Self {
        let mut result = HashMap::<String, CityInfo>::new();
        for p in partials {
            for (k, v) in p {
                if let Some(curr) = result.get_mut(&k) {
                    curr.min = f64::min(curr.min, v.min);
                    curr.count += 1;
                    curr.mean = (curr.mean + v.mean) / (curr.count + v.count) as f64;
                    curr.max = f64::max(curr.max, v.max);
                } else {
                    result.insert(k, v);
                }
            }
        }
        Self { result }
    }
}

impl<'f> SplitReader<'f> {
    fn new(f: &'f [u8]) -> Self {
        Self { f, cursor: 0 }
    }

    fn next(&mut self) -> Option<Arc<[u8]>> {
        let l = self.f.len() - 1;
        let start = self.cursor;
        if start >= l {
            None
        } else {
            let mut end = min(self.cursor + CHUNK_SIZE, l);
            while end < l && self.f[end] != b'\n' {
                end += 1;
            }
            self.cursor = end + 1;
            Some(self.f[start..end].into())
        }
    }

    // fn next(&mut self) -> Option<String> {
    //     let mut n = String::with_capacity(CHUNK as usize + SLIP);
    //     let mut cand = vec![0u8];
    //     let mut i = 0i64;
    //     self.f.seek(SeekFrom::Current(CHUNK)).ok()?;
    //     while let Ok(_) = self.f.read(&mut cand[..]) {
    //         i += 1;
    //         if cand[0] as char == '\n' {
    //             self.f.seek(SeekFrom::Current( - (CHUNK + i))).expect("file read failed");
    //             self.f.read_to_string(&mut n).ok()?;
    //             break;
    //         } else if cand[0] == 0 {
    //             return None;
    //         }
    //     }
    //     Some(n)
    // }
}

fn process(r: Arc<Mutex<SplitReader>>, t: Sender<HashMap<String, CityInfo>>, _i: usize) {
    let mut results = HashMap::<String, CityInfo>::new();
    loop {
        let buf;
        {
            let mut reader = r.lock().expect("lost reader");
            buf = reader.next();
        }
        if let Some(buf) = buf {
            let buf = unsafe { std::str::from_utf8_unchecked(&buf) };
            for line in buf.lines() {
                let new = Measurement::from_str(line).expect("bad line");
                if let Some(curr) = results.get_mut(&new.name) {
                    curr.min = f64::min(curr.min, new.temp);
                    curr.count += 1;
                    curr.mean = curr.mean + (new.temp - curr.mean) / curr.count as f64;
                    curr.max = f64::max(curr.max, new.temp);
                } else {
                    results.insert(
                        new.name.to_owned(),
                        CityInfo {
                            min: new.temp,
                            mean: new.temp,
                            max: new.temp,
                            count: 1,
                        },
                    );
                }
            }
        } else {
            t.send(results).expect("lost writer");
            break;
        }
    }
}

fn main() {
    let fname = args().nth(1).unwrap_or("data/measurements.txt".to_string());
    let file = File::open(fname).expect("file not found");
    let map = unsafe { Mmap::map(&file) }.expect("error opening file");
    let reader = Arc::new(Mutex::new(SplitReader::new(&map)));
    let (tx, rx): (
        Sender<HashMap<String, CityInfo>>,
        Receiver<HashMap<String, CityInfo>>,
    ) = channel();
    let cores = thread::available_parallelism().unwrap().into();

    thread::scope(|s| {
        for i in 0..cores {
            let r = reader.clone();
            let t = tx.clone();
            s.spawn(move || process(r, t, i));
        }
    });

    let results = (0..cores)
        .map(|_| rx.recv().expect("received mal data"))
        .collect::<Vec<_>>();
    let agg = Aggregate::new(results);
    println!("{}", agg);
}
