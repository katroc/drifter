use anyhow::{Context, Result};
use crate::core::config::{Config, ScanMode};
use std::net::SocketAddr;
use tokio::fs::File as TokioFile;
use tracing::{info, warn, error};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct Scanner {
    mode: ScanMode,
    clamd_host: String,
    clamd_port: u16,
    scan_chunk_size_mb: u64,
    concurrency: usize,
}

impl Scanner {
    pub fn new(config: &Config) -> Self {
        Self {
            mode: config.scan_mode.clone(),
            clamd_host: config.clamd_host.clone(),
            clamd_port: config.clamd_port,
            scan_chunk_size_mb: config.scan_chunk_size_mb,
            concurrency: config.concurrency_parts_per_file.max(1),
        }
    }

    pub async fn scan_file(&self, path: &str) -> Result<bool> {

        match self.mode {
            ScanMode::Skip => {
                info!("Scanning skipped for: {}", path);
                Ok(true)
            },
            ScanMode::Stream => self.scan_chunked(path).await,
            ScanMode::Full => self.scan_chunked(path).await,
        }
    }

    async fn scan_chunked(&self, path: &str) -> Result<bool> {
        use tokio::task::JoinSet;

        info!("Starting chunked scan for: {}", path);
        let chunk_size = (self.scan_chunk_size_mb * 1024 * 1024) as usize;
        let overlap = 1024 * 1024; // 1MB overlap
        let chunk_size = chunk_size.max(overlap + 1024);
        let concurrency = self.concurrency.max(1);
        
        let file = TokioFile::open(path).await.context("Failed to open file")?;
        let file_len = file.metadata().await?.len();
        
        // Calculate all chunk offsets
        let mut offsets = Vec::new();
        let mut offset = 0u64;
        while offset < file_len {
            offsets.push(offset);
            offset += (chunk_size as u64) - (overlap as u64);
        }
        
        let mut join_set = JoinSet::new();
        let mut offsets_iter = offsets.into_iter();
        
        // Fill initial queue
        for _ in 0..concurrency {
            if let Some(chunk_offset) = offsets_iter.next() {
                let path = path.to_string();
                let host = self.clamd_host.clone();
                let port = self.clamd_port;
                
                join_set.spawn(async move {
                    scan_chunk_at_offset(&path, chunk_offset, chunk_size, &host, port).await
                });
            }
        }
        
        // Process results and spawn remaining
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(clean)) => {
                    if !clean {
                        warn!("Infection detected in file: {}", path);
                        join_set.abort_all();
                        return Ok(false); // Infection found - stop scanning
                    }
                    
                    // Spawn next chunk if available
                    if let Some(chunk_offset) = offsets_iter.next() {
                        let path = path.to_string();
                        let host = self.clamd_host.clone();
                        let port = self.clamd_port;
                        
                        join_set.spawn(async move {
                            scan_chunk_at_offset(&path, chunk_offset, chunk_size, &host, port).await
                        });
                    }
                }
                Ok(Err(e)) => {
                    error!("Scan chunk error: {}", e);
                    join_set.abort_all();
                    return Err(e);
                }
                Err(e) => {
                     error!("Scan task failed: {}", e);
                     join_set.abort_all();
                     return Err(anyhow::anyhow!("Scan task failed: {}", e));
                }
            }
        }
        
        info!("Scan complete (clean) for: {}", path);
        Ok(true)
    }

    pub async fn check_connection(&self) -> Result<String> {
        let address = format!("{}:{}", self.clamd_host, self.clamd_port);
        let addr: SocketAddr = address.parse().context("Invalid clamd address")?;
        
        let mut stream = TcpStream::connect(addr).await.context("Failed to connect to clamd")?;
        stream.write_all(b"PING").await.context("Failed to send PING")?;
        
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.context("Failed to read response")?;
        let response_str = String::from_utf8_lossy(&response);
        
        if response_str.trim() == "PONG" {
            Ok("Connected to ClamAV successfully".to_string())
        } else {
             Err(anyhow::anyhow!("Unexpected response from ClamAV: '{}'", response_str.trim()))
        }
    }
}

/// Standalone function for parallel chunk scanning
async fn scan_chunk_at_offset(path: &str, offset: u64, chunk_size: usize, host: &str, port: u16) -> Result<bool> {
    use std::io::SeekFrom;
    
    let mut file = TokioFile::open(path).await.context("Failed to open file")?;
    file.seek(SeekFrom::Start(offset)).await.context("Failed to seek")?;
    
    let mut buffer = vec![0u8; chunk_size];
    let mut bytes_read = 0;
    
    while bytes_read < chunk_size {
        let n = file.read(&mut buffer[bytes_read..]).await.context("Failed to read file")?;
        if n == 0 { break; }
        bytes_read += n;
    }
    
    if bytes_read == 0 {
        return Ok(true); // Empty chunk, nothing to scan
    }
    
    // Send to ClamAV
    let address = format!("{}:{}", host, port);
    let addr: SocketAddr = address.parse().context("Invalid clamd address")?;
    
    let mut stream = TcpStream::connect(addr).await.context("Failed to connect to clamd")?;
    stream.write_all(b"zINSTREAM\0").await.context("Failed to send zINSTREAM")?;
    
    let mut cursor = 0;
    while cursor < bytes_read {
        let end = (cursor + 32768).min(bytes_read);
        let chunk = &buffer[cursor..end];
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
