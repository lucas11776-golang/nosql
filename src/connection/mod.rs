use std::net::SocketAddr;
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{connection::handshake::Handshake, Result};

pub mod handshake;

pub struct Connection {
    listener: TcpListener,
}

pub struct Payload {
    pub table: String,
    pub action: String,
    pub data: crate::Value,
}

pub struct Client {
    stream: TcpStream,
    addr: SocketAddr,
}

impl Client {
    pub fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self { stream, addr }
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        let len = data.len() as u32;

        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(data).await?;
        
        self
            .stream
            .flush()
            .await
            .map_err(Into::into)
    }

    pub async fn read(&mut self) -> Result<(usize, Bytes)> {
        let mut len_buf = [0u8; 4];

        self.stream.read_exact(&mut len_buf).await?;
        
        let payload_len = u32::from_be_bytes(len_buf) as usize;
        
        let mut payload_buf = vec![0u8; payload_len];
        self.stream.read_exact(&mut payload_buf).await?;

        let total_bytes_read = 4 + payload_len;

        Ok((total_bytes_read, Bytes::from(payload_buf)))
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Connection {
    pub async fn new(host: &str, port: u32) -> Result<Self> {
        let address = format!("{}:{}", host, port);
        let listener = TcpListener::bind(&address).await?;
        Ok(Self { listener })
    }

    pub async fn accept(&self) -> Result<()> {
    loop {
        let (stream, addr) = self.listener.accept().await?;

        tokio::spawn(async move {
            let mut client = Client::new(stream, addr);

            // 1. Handshake Phase
            let mut handshake = Handshake::new(&mut client);
            match handshake.agree().await {
                Ok(true) => {
                    println!("Handshake successful with {}", addr);
                }
                Ok(false) => {
                    eprintln!("Handshake rejected for {}", addr);
                    return;
                }
                Err(e) => {
                    eprintln!("Handshake error with {}: {:?}", addr, e);
                    return;
                }
            }

            loop {
                match client.read().await {
                    Ok((_bytes_read, payload_bytes)) => {
                        // Process requests...
                    }
                    Err(e) => {
                        eprintln!("Client {} disconnected: {:?}", addr, e);
                        break;
                    }
                }
            }
        });
    }
}
}