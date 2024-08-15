use std::env;
use tokio::fs::File;
use futures::StreamExt;
use std::time::Instant;

async fn fetch(url: &str, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut f = File::create(filename).await?;

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

    dbg!(content_length);

    let mut stream = client.get(url)
        .send()
        .await?
        .bytes_stream();

    let mut offset = 0;

    println!("Download started...");

    let start_time = Instant::now();

    while let Some(item) = stream.next().await {
        let buffer = item.unwrap();
        let length = buffer.len();

        //println!("Write to {offset}, {length} bytes");
        tokio::io::copy(&mut buffer.as_ref(), &mut f).await?;

        offset += length;
    }

    let elapsed = start_time.elapsed();
    println!("Duration: {elapsed:?}");

    let Bps = (content_length as f64) / elapsed.as_secs_f64();
    let MBps = Bps / (1024.0*1024.0);
    let Mbps = MBps * 8.0;
    println!("Rate: {Mbps} Mbps");
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let url = &args[1];
    let filename = &args[2];

    fetch(url, filename).await?;

    Ok(())
}
