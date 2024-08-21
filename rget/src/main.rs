use std::env;
use std::fs::File;
use std::os::unix::fs::FileExt;
use futures::StreamExt;
use std::time::{Instant, Duration};
use http::StatusCode;
use bytes::Bytes;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;
use size::Size;
use std::fmt;

fn as_megabits_per_sec(bytes: u64, elapsed: Duration) -> f64 {
    let bytes_per_sec = (bytes as f64) / elapsed.as_secs_f64();
    let megabytes_per_sec = bytes_per_sec / (1024.0*1024.0);
    let megabits_per_sec = megabytes_per_sec * 8.0;
    megabits_per_sec
}

struct WritePacket {
    offset: u64,
    buffer: Bytes
}

enum StatisticsLabel {
    Network,
    Disk
}

impl fmt::Display for StatisticsLabel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StatisticsLabel::Network => write!(f, "Network"),
            StatisticsLabel::Disk => write!(f, "Disk")
        }
    }
}

struct Statistics {
    label: StatisticsLabel,
    start_time: Instant,
    last_print_time: Instant,
    last_print_bytes: u64,
    next_print_bytes: u64,
    completed_bytes: u64,
    total_bytes: u64
}

impl Statistics {
    fn new(label: StatisticsLabel, total_bytes: u64) -> Statistics {
        let now = Instant::now();
        Statistics {
            label,
            start_time: now,
            last_print_time: now,
            last_print_bytes: 0,
            next_print_bytes: 0,
            completed_bytes: 0,
            total_bytes
        }
    }

    fn add(&mut self, bytes: u64) {
        self.completed_bytes += bytes;
        if self.last_print_time.elapsed().as_secs() >= 1 && self.completed_bytes >= self.next_print_bytes {
            let percent = self.print();
            self.last_print_time = Instant::now();
            self.last_print_bytes = self.completed_bytes;
            self.next_print_bytes = ((percent.floor() + 1.0) / 100.0 * self.total_bytes as f64) as u64;
        }
    }

    fn print(&self) -> f64 {
        let percent = self.completed_bytes as f64 / self.total_bytes as f64 * 100.0;
        let mbps = as_megabits_per_sec(self.completed_bytes - self.last_print_bytes, self.last_print_time.elapsed());
        println!("{0}: {1} {2:.1}% {3:.1}Mbps", self.label, Size::from_bytes(self.completed_bytes), percent, mbps);
        percent
    }
}

async fn get_content_length(url: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let response = client.head(url)
        .send()
        .await?;
    let content_length = response.headers().get(reqwest::header::CONTENT_LENGTH)
        .expect("Server must support Content-Length header")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();

    Ok(content_length)
}

fn fetch_writes(filename: String, content_length: u64, rx: Receiver<WritePacket>) -> Result<(), std::io::Error> {
    let f = File::create(filename)?;

    let mut write_stats = Statistics::new(StatisticsLabel::Disk, content_length);

    //println!("WRITES BEGIN");
    while let Ok(packet) = rx.recv() {
        let length = packet.buffer.len();
        f.write_all_at(&mut packet.buffer.as_ref(), packet.offset)?;
        write_stats.add(length as u64);
    }
    //println!("WRITES END");
    write_stats.print();

    Ok(())
}

async fn fetch_read(net_stats: Arc<Mutex<Statistics>>, url: String, tx: Sender<WritePacket>, start_offset: u64, end_offset: u64) -> Result<(), reqwest::Error> {
    let last_offset = end_offset - 1;

    let client = reqwest::Client::new();

    let response = client.get(url)
        .header(reqwest::header::RANGE, format!("bytes={start_offset}-{last_offset}"))
        .send()
        .await?;

    if response.status() != StatusCode::PARTIAL_CONTENT {
        panic!("Server does not support Range header");
    }

    let mut stream = response.bytes_stream();
    let mut offset = start_offset;
    let mut count_streams = 0;

    //println!("REQUEST BEGIN {start_offset}");
    while let Some(item) = stream.next().await {
        let buffer: Bytes = item.unwrap();
        let length = buffer.len() as u64;

        let packet = WritePacket { buffer, offset };
        tx.send(packet).unwrap();

        net_stats.lock().unwrap().add(length);

        offset += length;
        count_streams += 1;
    }
    //println!("REQUEST END {start_offset} count_streams={count_streams}");

    Ok(())
}

async fn fetch(url: &str, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    let content_length = get_content_length(url).await?;

    let net_stats = Arc::new(Mutex::new(Statistics::new(StatisticsLabel::Network, content_length)));

    let (tx, rx) = channel();

    let fw_filename = String::from(filename);
    let fw = tokio::task::spawn_blocking(move || { fetch_writes(fw_filename, content_length, rx) });

    let max_request_length = 1024 * 1024;
    let max_requests = 8;

    let mut offset: u64 = 0;

    let mut set = JoinSet::new();

    loop {
        if offset == content_length {
            break
        }

        let end_offset = std::cmp::min(offset + max_request_length, content_length);

        set.spawn(fetch_read(net_stats.clone(), String::from(url), tx.clone(), offset, end_offset));

        offset = end_offset;

        if set.len() >= max_requests {
            set.join_next().await.unwrap()??;
        }
    }

    drop(tx);
    //println!("QUEUE END");

    while let Some(_) = set.join_next().await { }

    net_stats.lock().unwrap().print();

    //println!("REQUESTS END");

    fw.await.unwrap().unwrap();

    let elapsed = start_time.elapsed();
    println!("Duration: {elapsed:?}");

    let megabits_per_sec = as_megabits_per_sec(content_length, elapsed);
    println!("Rate: {megabits_per_sec} Mbps");
    
    Ok(())
}

#[tokio::main()]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let url = &args[1];
    let filename = &args[2];

    fetch(url, filename).await?;

    Ok(())
}
