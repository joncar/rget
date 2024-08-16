use std::env;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use futures::StreamExt;
use std::time::Instant;
use http::StatusCode;
use bytes::Bytes;
use std::sync::mpsc::{Sender, Receiver, channel};
use tokio::task::JoinSet;

struct WritePacket {
    offset: u64,
    buffer: Bytes
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

async fn fetch_writes(filename: String, rx: Receiver<WritePacket>) -> Result<(), std::io::Error> {
    let mut f = File::create(filename).await?;

    while let Ok(packet) = rx.recv() {
        f.seek(std::io::SeekFrom::Start(packet.offset)).await?;
        tokio::io::copy(&mut packet.buffer.as_ref(), &mut f).await?;
    }

    Ok(())
}

async fn fetch_read(url: String, tx: Sender<WritePacket>, start_offset: u64, end_offset: u64) -> Result<(), reqwest::Error> {
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

    while let Some(item) = stream.next().await {
        let buffer: Bytes = item.unwrap();
        let length = buffer.len() as u64;

        let packet = WritePacket { buffer, offset };
        tx.send(packet).unwrap();

        offset += length;
    }

    Ok(())
}

async fn fetch(url: &str, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    let content_length = get_content_length(url).await?;

    let (tx, rx) = channel();

    let fw = tokio::spawn(fetch_writes(String::from(filename), rx));

    let max_request_length = 1024 * 1024;
    let max_requests = 8;

    let mut offset: u64 = 0;

    let mut set = JoinSet::new();

    loop {
        if offset == content_length {
            break
        }

        let end_offset = std::cmp::min(offset + max_request_length, content_length);

        set.spawn(fetch_read(String::from(url), tx.clone(), offset, end_offset));

        offset = end_offset;

        if set.len() >= max_requests {
            set.join_next().await.unwrap()??;
        }
    }

    drop(tx);

    while let Some(_) = set.join_next().await { }

    fw.await.unwrap().unwrap();

    let elapsed = start_time.elapsed();
    println!("Duration: {elapsed:?}");

    let bytes_per_sec = (content_length as f64) / elapsed.as_secs_f64();
    let megabytes_per_sec = bytes_per_sec / (1024.0*1024.0);
    let megabits_per_sec = megabytes_per_sec * 8.0;
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
