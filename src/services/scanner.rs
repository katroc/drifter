use crate::core::config::{Config, ScanMode};
use anyhow::{Context, Result};
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info, warn};

#[derive(Debug, Clone, PartialEq)]
pub enum ScanResult {
    Clean,
    Infected(String),
}

#[derive(Clone)]
pub struct Scanner {
    mode: ScanMode,
    clamd_host: String,
    clamd_port: u16,
    clamd_socket: Option<String>,
    scan_chunk_size_mb: u64,
    max_scan_size_mb: Option<u64>,
    concurrency: usize,
}

impl Scanner {
    pub fn new(config: &Config) -> Self {
        Self {
            mode: config.scan_mode.clone(),
            clamd_host: config.clamd_host.clone(),
            clamd_port: config.clamd_port,
            clamd_socket: config.clamd_socket.clone().filter(|s| !s.trim().is_empty()),
            scan_chunk_size_mb: config.scan_chunk_size_mb,
            max_scan_size_mb: config.max_scan_size_mb,
            concurrency: config.concurrency_scan_parts.max(1),
        }
    }

    pub async fn scan_file(&self, path: &str) -> Result<ScanResult> {
        if let Some(max_mb) = self.max_scan_size_mb {
            let max_bytes = max_mb.saturating_mul(1024 * 1024);
            let file_size = tokio::fs::metadata(path)
                .await
                .with_context(|| format!("Failed to read metadata for {}", path))?
                .len();
            if file_size > max_bytes {
                warn!(
                    "Skipping scan for {} ({} bytes exceeds max_scan_size_mb={}MB)",
                    path, file_size, max_mb
                );
                return Ok(ScanResult::Clean);
            }
        }

        match self.mode {
            ScanMode::Skip => {
                info!("Scanning skipped for: {}", path);
                Ok(ScanResult::Clean)
            }
            ScanMode::Stream => self.scan_chunked(path).await,
            ScanMode::Full => self.scan_chunked(path).await,
        }
    }

    async fn scan_chunked(&self, path: &str) -> Result<ScanResult> {
        use tokio::task::JoinSet;

        info!("Starting chunked scan for: {}", path);
        let chunk_size = Self::calculate_effective_chunk_size(self.scan_chunk_size_mb);
        let concurrency = self.concurrency.max(1);

        let file = TokioFile::open(path).await.context("Failed to open file")?;
        let file_len = file.metadata().await?.len();

        // Calculate all chunk offsets
        let offsets = Self::calculate_chunk_offsets(file_len, self.scan_chunk_size_mb);

        let mut join_set = JoinSet::new();
        let mut offsets_iter = offsets.into_iter();

        // Fill initial queue
        for _ in 0..concurrency {
            if let Some(chunk_offset) = offsets_iter.next() {
                let path = path.to_string();
                let host = self.clamd_host.clone();
                let port = self.clamd_port;
                let clamd_socket = self.clamd_socket.clone();

                join_set.spawn(async move {
                    scan_chunk_at_offset(
                        &path,
                        chunk_offset,
                        chunk_size,
                        &host,
                        port,
                        clamd_socket.as_deref(),
                    )
                    .await
                });
            }
        }

        // Process results and spawn remaining
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(ScanResult::Infected(virus))) => {
                    warn!("Infection detected in file: {} ({})", path, virus);
                    join_set.abort_all();
                    return Ok(ScanResult::Infected(virus)); // Infection found - stop scanning
                }
                Ok(Ok(ScanResult::Clean)) => {
                    // Clean, continue

                    // Spawn next chunk if available
                    if let Some(chunk_offset) = offsets_iter.next() {
                        let path = path.to_string();
                        let host = self.clamd_host.clone();
                        let port = self.clamd_port;
                        let clamd_socket = self.clamd_socket.clone();

                        join_set.spawn(async move {
                            scan_chunk_at_offset(
                                &path,
                                chunk_offset,
                                chunk_size,
                                &host,
                                port,
                                clamd_socket.as_deref(),
                            )
                            .await
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
        Ok(ScanResult::Clean)
    }

    pub async fn check_connection(&self) -> Result<String> {
        let mut stream =
            connect_clamd(self.clamd_socket.as_deref(), &self.clamd_host, self.clamd_port).await?;
        stream
            .write_all(b"PING")
            .await
            .context("Failed to send PING")?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .context("Failed to read response")?;
        let response_str = String::from_utf8_lossy(&response);

        if response_str.trim() == "PONG" {
            Ok("Connected to ClamAV successfully".to_string())
        } else {
            Err(anyhow::anyhow!(
                "Unexpected response from ClamAV: '{}'",
                response_str.trim()
            ))
        }
    }
}

impl Scanner {
    /// Calculate chunk offsets for scanning a file
    /// Returns a vector of byte offsets where each chunk should start
    /// Chunks overlap by 1MB to ensure malware spanning chunk boundaries is detected
    pub fn calculate_chunk_offsets(file_size: u64, chunk_size_mb: u64) -> Vec<u64> {
        let chunk_size = (chunk_size_mb * 1024 * 1024) as usize;
        let overlap = 1024 * 1024; // 1MB overlap
        let chunk_size = chunk_size.max(overlap + 1024);

        let mut offsets = Vec::new();
        let mut offset = 0u64;
        while offset < file_size {
            offsets.push(offset);
            offset += (chunk_size as u64) - (overlap as u64);
        }

        offsets
    }

    /// Calculate the actual chunk size used for scanning
    /// Ensures minimum size to accommodate overlap
    pub fn calculate_effective_chunk_size(chunk_size_mb: u64) -> usize {
        let chunk_size = (chunk_size_mb * 1024 * 1024) as usize;
        let overlap = 1024 * 1024; // 1MB overlap
        chunk_size.max(overlap + 1024)
    }
}

/// Standalone function for parallel chunk scanning
async fn scan_chunk_at_offset(
    path: &str,
    offset: u64,
    chunk_size: usize,
    host: &str,
    port: u16,
    clamd_socket: Option<&str>,
) -> Result<ScanResult> {
    use std::io::SeekFrom;

    let mut file = TokioFile::open(path).await.context("Failed to open file")?;
    file.seek(SeekFrom::Start(offset))
        .await
        .context("Failed to seek")?;

    let mut buffer = vec![0u8; chunk_size];
    let mut bytes_read = 0;

    while bytes_read < chunk_size {
        let n = file
            .read(&mut buffer[bytes_read..])
            .await
            .context("Failed to read file")?;
        if n == 0 {
            break;
        }
        bytes_read += n;
    }

    if bytes_read == 0 {
        return Ok(ScanResult::Clean); // Empty chunk, nothing to scan
    }

    // Send to ClamAV
    let mut stream = connect_clamd(clamd_socket, host, port).await?;
    stream
        .write_all(b"zINSTREAM\0")
        .await
        .context("Failed to send zINSTREAM")?;

    let mut cursor = 0;
    while cursor < bytes_read {
        let end = (cursor + 32768).min(bytes_read);
        let chunk = &buffer[cursor..end];
        let len_bytes = (chunk.len() as u32).to_be_bytes();

        stream
            .write_all(&len_bytes)
            .await
            .context("Failed to write chunk len")?;
        stream
            .write_all(chunk)
            .await
            .context("Failed to write chunk")?;

        cursor = end;
    }

    stream
        .write_all(&[0u8; 4])
        .await
        .context("Failed to write stream end")?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .context("Failed to read response")?;
    let response_str = String::from_utf8_lossy(&response);

    if response_str.contains("FOUND") {
        // Parse virus name: "stream: Win.Test.EICAR_HDB-1 FOUND"
        let parts: Vec<&str> = response_str.split("FOUND").collect();
        let name_part = parts.first().unwrap_or(&"Unknown");
        let virus_name = name_part.trim().replace("stream: ", "").trim().to_string();
        Ok(ScanResult::Infected(virus_name))
    } else if response_str.contains("OK") {
        Ok(ScanResult::Clean)
    } else {
        Err(anyhow::anyhow!("ClamAV Error: {}", response_str.trim()))
    }
}

enum ClamdStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

impl ClamdStream {
    async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            ClamdStream::Tcp(stream) => stream.write_all(data).await,
            #[cfg(unix)]
            ClamdStream::Unix(stream) => stream.write_all(data).await,
        }
    }

    async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        match self {
            ClamdStream::Tcp(stream) => stream.read_to_end(buf).await,
            #[cfg(unix)]
            ClamdStream::Unix(stream) => stream.read_to_end(buf).await,
        }
    }
}

async fn connect_clamd(clamd_socket: Option<&str>, host: &str, port: u16) -> Result<ClamdStream> {
    if let Some(socket_path) = clamd_socket.filter(|s| !s.trim().is_empty()) {
        #[cfg(unix)]
        {
            let stream = tokio::net::UnixStream::connect(socket_path)
                .await
                .with_context(|| format!("Failed to connect to clamd socket {}", socket_path))?;
            return Ok(ClamdStream::Unix(stream));
        }
        #[cfg(not(unix))]
        {
            warn!(
                "clamd_socket '{}' is configured but unix sockets are unsupported on this platform; falling back to TCP",
                socket_path
            );
        }
    }

    let address = format!("{}:{}", host, port);
    let stream = TcpStream::connect(address.as_str())
        .await
        .with_context(|| format!("Failed to connect to clamd at {}", address))?;
    Ok(ClamdStream::Tcp(stream))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Chunk Size Calculation Tests ---

    #[test]
    fn test_calculate_effective_chunk_size_default() {
        // Default config is usually 10MB
        let chunk_size = Scanner::calculate_effective_chunk_size(10);
        assert_eq!(
            chunk_size,
            10 * 1024 * 1024,
            "10MB config should give 10MB chunks"
        );
    }

    #[test]
    fn test_calculate_effective_chunk_size_minimum_enforced() {
        // Very small config should be bumped up to minimum (1MB overlap + 1KB)
        let chunk_size = Scanner::calculate_effective_chunk_size(1);
        let min_size = 1024 * 1024 + 1024; // 1MB + 1KB

        // 1MB config = 1048576 bytes
        // Should use config size since 1048576 > 1049600
        // Actually, let me check the logic: chunk_size.max(overlap + 1024)
        // 1MB = 1048576, overlap + 1024 = 1049600
        // So max should be 1049600
        assert!(chunk_size >= min_size, "Should enforce minimum chunk size");
    }

    #[test]
    fn test_calculate_effective_chunk_size_large_value() {
        // Large config (100MB)
        let chunk_size = Scanner::calculate_effective_chunk_size(100);
        assert_eq!(chunk_size, 100 * 1024 * 1024);
    }

    // --- Chunk Offset Calculation Tests ---

    #[test]
    fn test_calculate_chunk_offsets_single_chunk() {
        // File smaller than chunk size -> single chunk at offset 0
        let file_size = 5 * 1024 * 1024; // 5MB
        let chunk_size_mb = 10; // 10MB chunks

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);

        assert_eq!(offsets.len(), 1, "Small file should have 1 chunk");
        assert_eq!(offsets[0], 0, "First chunk starts at offset 0");
    }

    #[test]
    fn test_calculate_chunk_offsets_exact_multiple() {
        // File that's exactly 2x chunk size (accounting for overlap)
        let chunk_size_mb = 10;
        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);
        let overlap = 1024 * 1024;
        let stride = effective_chunk_size - overlap;

        let file_size = (stride * 2) as u64;

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);

        // Should have 2 chunks
        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1], stride as u64);
    }

    #[test]
    fn test_calculate_chunk_offsets_with_overlap() {
        // Verify that chunks overlap by 1MB
        let chunk_size_mb = 10; // 10MB
        let file_size = 50 * 1024 * 1024; // 50MB file

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);

        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);
        let overlap = 1024 * 1024; // 1MB
        let stride = effective_chunk_size - overlap;

        // Verify stride between chunks
        for i in 1..offsets.len() {
            let diff = offsets[i] - offsets[i - 1];
            assert_eq!(
                diff, stride as u64,
                "Chunks should be spaced by stride (chunk_size - overlap)"
            );
        }
    }

    #[test]
    fn test_calculate_chunk_offsets_coverage() {
        // Verify all chunks together cover the entire file
        let chunk_size_mb = 10;
        let file_size = 35 * 1024 * 1024; // 35MB

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);
        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);

        // Last chunk should start before file_size
        let last_offset = offsets.last().unwrap();
        assert!(
            *last_offset < file_size,
            "Last chunk should start before EOF"
        );

        // Last chunk should cover end of file
        let last_chunk_end = last_offset + effective_chunk_size as u64;
        assert!(
            last_chunk_end >= file_size,
            "Last chunk should cover end of file"
        );
    }

    #[test]
    fn test_calculate_chunk_offsets_empty_file() {
        let offsets = Scanner::calculate_chunk_offsets(0, 10);
        assert_eq!(offsets.len(), 0, "Empty file should have no chunks");
    }

    #[test]
    fn test_calculate_chunk_offsets_tiny_file() {
        // File smaller than 1KB
        let file_size = 512; // 512 bytes
        let offsets = Scanner::calculate_chunk_offsets(file_size, 10);

        assert_eq!(offsets.len(), 1, "Tiny file should have 1 chunk");
        assert_eq!(offsets[0], 0);
    }

    #[test]
    fn test_calculate_chunk_offsets_large_file() {
        // 1GB file with 10MB chunks
        let file_size = 1024 * 1024 * 1024u64; // 1GB
        let chunk_size_mb = 10;

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);

        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);
        let overlap = 1024 * 1024;
        let stride = effective_chunk_size - overlap;

        // Calculate expected number of chunks
        // Formula: ceil(file_size / stride)
        let expected_chunks = ((file_size as f64) / (stride as f64)).ceil() as usize;

        assert_eq!(
            offsets.len(),
            expected_chunks,
            "Large file should have correct number of chunks"
        );
    }

    #[test]
    fn test_calculate_chunk_offsets_overlap_detection() {
        // Verify that a signature spanning chunk boundaries is covered
        let chunk_size_mb = 10;
        let file_size = 25 * 1024 * 1024; // 25MB

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);
        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);

        // Check that each pair of consecutive chunks overlaps
        for i in 1..offsets.len() {
            let prev_chunk_end = offsets[i - 1] + effective_chunk_size as u64;
            let current_chunk_start = offsets[i];

            assert!(
                prev_chunk_end > current_chunk_start,
                "Chunks {} and {} should overlap",
                i - 1,
                i
            );

            // Verify overlap is at least 1MB
            let overlap_size = prev_chunk_end - current_chunk_start;
            assert!(
                overlap_size >= 1024 * 1024,
                "Overlap should be at least 1MB, got {} bytes",
                overlap_size
            );
        }
    }

    // --- Chunk Boundary Tests ---

    #[test]
    fn test_chunk_boundaries_first_chunk() {
        let file_size = 50 * 1024 * 1024;
        let chunk_size_mb = 10;

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);
        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);

        // First chunk
        let start = offsets[0];
        let end = start + effective_chunk_size as u64;

        assert_eq!(start, 0);
        assert_eq!(end, 10 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_boundaries_middle_chunk() {
        let file_size = 50 * 1024 * 1024;
        let chunk_size_mb = 10;

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);
        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);

        if offsets.len() >= 3 {
            let middle_idx = offsets.len() / 2;
            let start = offsets[middle_idx];
            let end = start + effective_chunk_size as u64;

            // Middle chunk should be within file bounds
            assert!(start < file_size);
            assert!(end > start);
        }
    }

    #[test]
    fn test_chunk_boundaries_last_chunk() {
        let file_size = 25 * 1024 * 1024; // 25MB
        let chunk_size_mb = 10;

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);
        let last_offset = offsets.last().unwrap();

        // Last chunk should start before EOF
        assert!(*last_offset < file_size);

        // But should be positioned to cover the end
        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);
        let last_chunk_end = last_offset + effective_chunk_size as u64;
        assert!(
            last_chunk_end >= file_size,
            "Last chunk should cover EOF: chunk_end={}, file_size={}",
            last_chunk_end,
            file_size
        );
    }

    // --- Scan Mode Tests ---

    #[test]
    fn test_scan_mode_values() {
        // Verify ScanMode enum values exist
        let _skip = ScanMode::Skip;
        let _stream = ScanMode::Stream;
        let _full = ScanMode::Full;
    }

    #[test]
    fn test_scan_result_clean() {
        let result = ScanResult::Clean;
        assert_eq!(result, ScanResult::Clean);
    }

    #[test]
    fn test_scan_result_infected() {
        let result = ScanResult::Infected("TestVirus".to_string());

        match result {
            ScanResult::Infected(name) => assert_eq!(name, "TestVirus"),
            _ => panic!("Expected Infected result"),
        }
    }

    #[test]
    fn test_scan_result_equality() {
        assert_eq!(ScanResult::Clean, ScanResult::Clean);
        assert_eq!(
            ScanResult::Infected("Virus1".to_string()),
            ScanResult::Infected("Virus1".to_string())
        );
        assert_ne!(ScanResult::Clean, ScanResult::Infected("Virus".to_string()));
    }

    // --- Configuration Tests ---

    #[test]
    fn test_scanner_effective_chunk_size_consistency() {
        // Test various config values
        let test_cases = vec![
            (1, 1024 * 1024 + 1024),  // 1MB -> minimum enforced
            (5, 5 * 1024 * 1024),     // 5MB -> normal
            (10, 10 * 1024 * 1024),   // 10MB -> normal (default)
            (50, 50 * 1024 * 1024),   // 50MB -> normal
            (100, 100 * 1024 * 1024), // 100MB -> normal
        ];

        for (config_mb, expected_min) in test_cases {
            let chunk_size = Scanner::calculate_effective_chunk_size(config_mb);
            assert!(
                chunk_size >= expected_min,
                "Config {}MB should produce chunk size >= {} bytes, got {}",
                config_mb,
                expected_min,
                chunk_size
            );
        }
    }

    // --- Edge Cases ---

    #[test]
    fn test_chunk_offsets_exactly_one_chunk_size() {
        // File exactly equal to chunk size
        // Due to overlap logic, this will create 2 chunks (second chunk for overlap coverage)
        let chunk_size_mb = 10;
        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);
        let file_size = effective_chunk_size as u64;

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);

        // With overlap, we expect 2 chunks even for file size = chunk size
        // This ensures the entire file is scanned with proper overlap
        assert_eq!(
            offsets.len(),
            2,
            "File equal to chunk size creates 2 chunks (for overlap)"
        );
    }

    #[test]
    fn test_chunk_offsets_one_byte_over_chunk_size() {
        // File just barely larger than chunk size
        let chunk_size_mb = 10;
        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);
        let file_size = effective_chunk_size as u64 + 1;

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);

        assert!(
            offsets.len() >= 2,
            "File slightly larger than chunk size should have 2+ chunks"
        );
    }

    #[test]
    fn test_chunk_calculation_realistic_scenario() {
        // Realistic scenario: 500MB file with 50MB chunks
        let file_size = 500 * 1024 * 1024u64;
        let chunk_size_mb = 50;

        let offsets = Scanner::calculate_chunk_offsets(file_size, chunk_size_mb);
        let effective_chunk_size = Scanner::calculate_effective_chunk_size(chunk_size_mb);

        // Verify reasonable number of chunks (should be around 10-11)
        assert!(
            offsets.len() >= 10 && offsets.len() <= 15,
            "500MB file with 50MB chunks should have 10-15 chunks, got {}",
            offsets.len()
        );

        // Verify all chunks except last are evenly spaced
        let overlap = 1024 * 1024;
        let expected_stride = effective_chunk_size - overlap;

        for i in 1..offsets.len().min(3) {
            let actual_stride = offsets[i] - offsets[i - 1];
            assert_eq!(
                actual_stride, expected_stride as u64,
                "Chunks should be evenly spaced"
            );
        }
    }
}
