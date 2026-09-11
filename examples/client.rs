use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

use nosql::connection::handshake::{HANDSHAKE_ACK, HANDSHAKE_INTRO};
use nosql::connection::Connection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = "127.0.0.1";
    let port = 8080;

    let server = Connection::new(host, port).await?;
    println!("[Server] Listening on {}:{}", host, port);

    tokio::spawn(async move {
        if let Err(e) = server.accept().await {
            eprintln!("[Server Error] {}", e);
        }
    });

    sleep(Duration::from_millis(100)).await;

    println!("[Client] Connecting to database server...");
    let mut stream = TcpStream::connect(format!("{}:{}", host, port)).await?;

    println!("[Client] Sending handshake request...");
    let handshake_len = (HANDSHAKE_INTRO.len() as u32).to_be_bytes();
    
    
    stream.write_all(&handshake_len).await?;
    stream.write_all(HANDSHAKE_INTRO).await?;
    stream.flush().await?;

    let mut resp_len_buf = [0u8; 4];
    stream.read_exact(&mut resp_len_buf).await?;
    let resp_len = u32::from_be_bytes(resp_len_buf) as usize;

    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).await?;

    if resp_buf == HANDSHAKE_ACK {
        println!("[Client] Handshake SUCCESS! Connected to database.");
    } else {
        eprintln!("[Client] Handshake REJECTED by server.");
        return Ok(());
    }

    let payload = b"{\"table\": \"users\", \"action\": \"INSERT\", \"data\": {\"id\": 1}}";
    println!("[Client] Sending query: {}", String::from_utf8_lossy(payload));

    let payload_len = (payload.len() as u32).to_be_bytes();
    stream.write_all(&payload_len).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;

    sleep(Duration::from_millis(200)).await;
    
    println!("[Client] Connection test finished.");

    Ok(())
}