use anyhow::{Context, Result};
use crate::config::{Config, ScanMode};
use std::net::SocketAddr;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct Scanner {
    mode: ScanMode,
    clamd_host: String,
    clamd_port: u16,
    scan_chunk_size_mb: u64,
}

impl Scanner {
    pub fn new(config: &Config) -> Self {
        Self {
            mode: config.scan_mode.clone(),
            clamd_host: config.clamd_host.clone(),
            clamd_port: config.clamd_port,
            scan_chunk_size_mb: config.scan_chunk_size_mb,
        }
    }

    pub async fn scan_file(&self, path: &str) -> Result<bool> {

        match self.mode {
            ScanMode::Skip => Ok(true),
            ScanMode::Stream => self.scan_chunked(path).await, // Use chunked for 'stream' mode basically
            ScanMode::Full => self.scan_chunked(path).await,
        }
    }

    async fn scan_chunked(&self, path: &str) -> Result<bool> {
        let chunk_size = (self.scan_chunk_size_mb * 1024 * 1024) as usize;
        let overlap = 1 * 1024 * 1024; // 1MB overlap
        let chunk_size = chunk_size.max(overlap + 1024);
        
        // log::debug!("Starting chunked scan for {} (chunk: {} bytes)", path, chunk_size);

        let mut file = TokioFile::open(path).await.context("Failed to open file")?;
        let file_len = file.metadata().await?.len();
        
        let mut offset = 0;
        let mut buffer = vec![0u8; chunk_size];

        while offset < file_len {
            use std::io::SeekFrom;
            file.seek(SeekFrom::Start(offset)).await.context("Failed to seek")?;

            let mut bytes_read = 0;
            while bytes_read < chunk_size {
                let n = file.read(&mut buffer[bytes_read..]).await.context("Failed to read file")?;
                if n == 0 { break; }
                bytes_read += n;
            }

            if bytes_read == 0 { break; }

            let clean = self.scan_buffer(&buffer[..bytes_read]).await?;
            
            if !clean {
                return Ok(false);
            }

            offset += (chunk_size as u64) - (overlap as u64);
            if bytes_read < chunk_size { 
                break; 
            }
        }

        Ok(true)
    }

    async fn scan_buffer(&self, data: &[u8]) -> Result<bool> {
        let address = format!("{}:{}", self.clamd_host, self.clamd_port);
        let addr: SocketAddr = address.parse().context("Invalid clamd address")?;
        
        let mut stream = TcpStream::connect(addr).await.context("Failed to connect to clamd")?;
        stream.write_all(b"zINSTREAM\0").await.context("Failed to send zINSTREAM")?;
        
        let mut cursor = 0;
        while cursor < data.len() {
            let end = (cursor + 32768).min(data.len());
            let chunk = &data[cursor..end];
            let len_bytes = (chunk.len() as u32).to_be_bytes();
            
            stream.write_all(&len_bytes).await.context("Failed to write chunk len")?;
            stream.write_all(chunk).await.context("Failed to write chunk")?;
            
            cursor = end;
        }

        stream.write_all(&[0u8; 4]).await.context("Failed to write stream end")?;
        
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.context("Failed to read response")?;
        let response_str = String::from_utf8_lossy(&response);
        
        if response_str.contains("FOUND") {
            Ok(false)
        } else if response_str.contains("OK") {
            Ok(true)
        } else {
             Err(anyhow::anyhow!("ClamAV Error: {}", response_str.trim()))
        }
    }
}
