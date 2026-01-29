use anyhow::{Context, Result};
use crate::config::Config;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::error::ProvideErrorMetadata;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use crate::state::ProgressInfo;
use rusqlite::Connection;
use crate::db;

pub struct Uploader {}

impl Uploader {
    pub fn new(_config: &Config) -> Self {
        Self {}
    }

    pub async fn upload_file(
        &self,
        config: &Config,
        path: &str,
        job_id: i64,
        progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>,
        conn_mutex: Arc<Mutex<Connection>>,
        initial_upload_id: Option<String>,
    ) -> Result<bool> {
        let bucket = config.s3_bucket.as_deref().unwrap_or_default();
        let prefix = config.s3_prefix.as_deref().unwrap_or_default();
        let region = config.s3_region.as_deref().unwrap_or("us-east-1").trim();
        let endpoint = config.s3_endpoint.as_deref();
        let access_key = config.s3_access_key.as_deref().map(|s| s.trim());
        let secret_key = config.s3_secret_key.as_deref().map(|s| s.trim());
        let concurrency = config.concurrency_parts_per_file.max(1);

        // Notify UI of connection phase
        {
            let mut p = progress.lock().unwrap();
            p.insert(job_id, ProgressInfo { 
                percent: -1.0, 
                details: "Connecting to S3...".to_string(),
            });
        }

        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open("debug.log") {
            let ak_log = access_key.map(|s| format!("{}... (len={})", &s[..4.min(s.len())], s.len())).unwrap_or_else(|| "NONE".to_string());
            let sk_log = secret_key.map(|s| format!("{}... (len={})", &s[..4.min(s.len())], s.len())).unwrap_or_else(|| "NONE".to_string());
            let _ = std::io::Write::write_all(&mut f, format!("[S3_CONNECT] AK={} SK={} Bucket={} Region={}\n", ak_log, sk_log, bucket, region).as_bytes());
        }

        if bucket.is_empty() {
             return Err(anyhow::anyhow!("S3 bucket not configured"));
        }

        // Initialize Client
        #[allow(deprecated)]
        let mut config_loader = aws_config::from_env().region(aws_config::Region::new(region.to_string()));
        
        if let (Some(ak), Some(sk)) = (access_key, secret_key) {
             let creds = Credentials::new(
                 ak.to_string(),
                 sk.to_string(),
                 None,
                 None,
                 "static"
             );
             config_loader = config_loader.credentials_provider(SharedCredentialsProvider::new(creds));
        } else if access_key.is_some() || secret_key.is_some() {
             return Err(anyhow::anyhow!("S3 Credentials incomplete: Both Access Key and Secret Key must be provided. (Check if Secret Key is saved in Settings)"));
        }

        let sdk_config = config_loader.load().await;
        
        // Re-creating the client builder to properly set endpoint if needed
        let mut client_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if let Some(endpoint) = endpoint {
             client_builder = client_builder.endpoint_url(endpoint).force_path_style(true);
        }
        let client = Client::from_conf(client_builder.build());

        let file_path = Path::new(path);
        
        // Get the original source path or pre-calculated key from the job
        let s3_key_path = {
            let conn = conn_mutex.lock().unwrap();
            if let Ok(Some(job)) = db::get_job(&conn, job_id) {
                // If ingest calculated a relative path, use it
                if let Some(key) = job.s3_key {
                     key
                } else {
                    // Fallback to source path filename
                    let source = Path::new(&job.source_path);
                    source.file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| file_path.file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_else(|| "file".to_string()))
                }
            } else {
                file_path.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "file".to_string())
            }
        };
        let key = format!("{}{}", prefix, s3_key_path);

        let mut upload_id = initial_upload_id.clone();
        let mut completed_parts = Vec::new();
        let mut completed_indices = HashSet::new();

        // 1. Try to resume if upload_id exists
        if let Some(uid) = &upload_id {
            // Notify UI
            {
               let mut p = progress.lock().unwrap();
               p.insert(job_id, ProgressInfo { 
                   percent: -1.0, 
                   details: "Resuming: Listing parts...".to_string(),
               });
            }

            // List existing parts
            let list_parts_output = client.list_parts()
                .bucket(bucket)
                .key(&key)
                .upload_id(uid)
                .send()
                .await;

            match list_parts_output {
                Ok(output) => {
                    if let Some(parts) = output.parts {
                        for p in parts {
                             if let Some(pn) = p.part_number {
                                 if let Some(etag) = p.e_tag {
                                     completed_parts.push(CompletedPart::builder().part_number(pn).e_tag(etag).build());
                                     completed_indices.insert(pn);
                                 }
                             }
                        }
                    }
                }
                Err(_) => {
                    // Invalid upload ID (expired or aborted), start fresh
                    upload_id = None;
                }
            }
        }

        // 2. Create new upload if needed
        if upload_id.is_none() {
            let create_output = client
                .create_multipart_upload()
                .bucket(bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| {
                    let service_err = match e.as_service_error() {
                        Some(s) => format!("{}: {}", s.code().unwrap_or("Unknown"), s.message().unwrap_or("No message")),
                        None => e.to_string(),
                    };
                    anyhow::anyhow!("Failed to create multipart upload: {} (Bucket: {}, Key: {})", service_err, bucket, key)
                })?;
            
            let new_uid = create_output.upload_id().context("No upload ID")?.to_string();
            
            // Save to DB
            {
                let conn = conn_mutex.lock().unwrap();
                db::update_job_upload_id(&conn, job_id, &new_uid)?;
            }
            upload_id = Some(new_uid);
        }
        
        let upload_id = upload_id.unwrap(); // Safety: we just set it if none

        let mut file = File::open(path).await.context("Failed to open file")?;
        let file_size = file.metadata().await?.len();
        
        // Dynamic Part Size Calculation
        let min_part_size = 5 * 1024 * 1024; // S3 Min: 5MB
        let max_parts = 10000; // S3 Max parts
        
        // 1. Base config size (default 128MB)
        let config_part_size = (config.part_size_mb * 1024 * 1024) as usize;
        
        // 2. Adjust for parallelism: Attempt to create enough parts for all threads
        // If file_size=100MB, concurrency=4 -> target part size ~25MB
        let parallel_part_size = (file_size as usize).checked_div(concurrency).unwrap_or(file_size as usize);
        
        // 3. Select optimal size: 
        // Start with the SMALLER of config vs parallel (to encourage parallelism)
        // But clamp to min_part_size (5MB).
        let mut part_size = config_part_size.min(parallel_part_size).max(min_part_size);

        // 4. Critical: Adjust for S3 10,000 part limit (override everything else)
        // If file is 5TB, part_size MUST be > 500MB regardless of config
        let required_min_part = (file_size + max_parts - 1) / max_parts;
        part_size = part_size.max(required_min_part as usize);

        // helper to align to nearest MB is nice but not required 
        
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let uploaded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        
        // Initialize uploaded bytes with already completed parts size? 
        // We lack part sizes in list_parts usually (it returns Part struct). 
        // We can estimate or just track new uploads. For progress bar correctness on resume, 
        // it's tricky without summing existing parts. 
        // Ideally we sum existing parts.
        // But for time saving, let's start progress from what we upload now, or existing.
        // Actually, we can just calculate total = file_size.
        // Correct way:
        let initial_bytes: u64 = completed_indices.len() as u64 * part_size as u64; // Approx
        uploaded_bytes.store(initial_bytes, std::sync::atomic::Ordering::Relaxed);

        // Monitor State
        let net_bytes = uploaded_bytes.clone();
        let current_part = Arc::new(std::sync::atomic::AtomicU64::new(1));

        let monitor_progress = progress.clone();
        let monitor_net = net_bytes.clone();
        let m_job_id = job_id;
        let m_file_size = file_size;
        let m_part_size = part_size;
        let m_total_parts = (file_size as usize + part_size - 1) / part_size;

        let monitor_handle = tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
                
                let n = monitor_net.load(std::sync::atomic::Ordering::Relaxed);

                let pct = (n as f64 / m_file_size as f64) * 100.0;
                
                let part_size_display = if m_part_size >= 1024 * 1024 * 1024 {
                    format!("{:.2}GB", m_part_size as f64 / 1024.0 / 1024.0 / 1024.0)
                } else {
                    format!("{:.2}MB", m_part_size as f64 / 1024.0 / 1024.0)
                };

                // Calculate elapsed time
                let elapsed = start_time.elapsed();
                let elapsed_str = if elapsed.as_secs() >= 60 {
                    format!("{}m{}s", elapsed.as_secs() / 60, elapsed.as_secs() % 60)
                } else {
                    format!("{}s", elapsed.as_secs())
                };

                // Calculate parts completed
                let parts_done = n / m_part_size as u64;
                
                let details = format!("{}/{} parts, {} sized [{}]", 
                    parts_done, m_total_parts, part_size_display, elapsed_str);
                
                {
                    let mut p = monitor_progress.lock().unwrap();
                    p.insert(m_job_id, ProgressInfo { 
                        percent: pct, 
                        details, 
                    });
                }

                if n >= m_file_size as u64 {
                    // Don't break, keep updating (likely 0 speeds) until aborted by main thread.
                    // This ensures we don't freeze on stale 'high speed' metrics if finalization takes time.
                }
            }
        });

        // Initialize uploaded bytes 
        // Note: We already set disk_bytes above. uploaded_bytes is set around line 118.
        // We need to keep them effectively synced or logical.
        
        let mut handles = Vec::new();
        let mut part_number = 1;
        let mut buffer = vec![0u8; part_size];

        loop {
            // Check if part is already done
            if completed_indices.contains(&(part_number as i32)) {
                use std::io::SeekFrom;
                let current_pos = (part_number as u64 - 1) * part_size as u64;
                file.seek(SeekFrom::Start(current_pos + part_size as u64)).await.unwrap_or(current_pos); 
                
                part_number += 1;
                // Update tracker
                current_part.store(part_number as u64, std::sync::atomic::Ordering::Relaxed);
                continue;
            }

            // Acquire permit BEFORE reading to throttle memory usage
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            use std::io::SeekFrom;
            let offset = (part_number as u64 - 1) * (part_size as u64);
            file.seek(SeekFrom::Start(offset)).await?;

            let mut bytes_read = 0;
            while bytes_read < part_size {
                let n = file.read(&mut buffer[bytes_read..]).await.context("Failed to read file chunk")?;
                if n == 0 { break; }
                bytes_read += n;
            }

            if bytes_read == 0 {
                drop(permit);
                break;
            }

            let chunk = buffer[..bytes_read].to_vec();
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let uid_clone = upload_id.clone();
            let uploaded_bytes = uploaded_bytes.clone();
            // We do NOT pass 'progress' to task anymore, Monitor handles it.
            
            let handle = tokio::spawn(async move {
                let _permit = permit; // Hold permit until task completion
                let body = aws_sdk_s3::primitives::ByteStream::from(chunk);
                
                let upload_part_output = client
                    .upload_part()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(uid_clone)
                    .part_number(part_number)
                    .body(body)
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to upload part {}: {}", part_number, e))?;

                // Update Net Tracker
                let previous = uploaded_bytes.fetch_add(bytes_read as u64, std::sync::atomic::Ordering::Relaxed);
                
                // Debug to file
                use std::io::Write;
                if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open("debug.log") {
                    let _ = writeln!(f, "[UPLOAD_DONE] part={} bytes_read={} prev={} new={}", part_number, bytes_read, previous, previous + bytes_read as u64);
                }

                if let Some(etag) = upload_part_output.e_tag() {
                    Ok(CompletedPart::builder()
                        .e_tag(etag)
                        .part_number(part_number)
                        .build())
                } else {
                    Err(anyhow::anyhow!("No ETag"))
                }
            });
            
            handles.push(handle);
            part_number += 1;
            current_part.store(part_number as u64, std::sync::atomic::Ordering::Relaxed);
        }

        // Wait for all parts
        // `completed_parts` currently holds PRE-EXISTING parts.
        // We must append new ones.
        
        for handle in handles {
            let part = handle.await??;
            completed_parts.push(part);
        }
        
        // Monitor aborted AFTER complete to keep showing stats (likely 0)
        // Update details to "Finalizing"
        {
            let mut p = progress.lock().unwrap();
            if let Some(mut info) = p.get(&job_id).cloned() {
                info.details = "Finalizing S3...".to_string();
                p.insert(job_id, info);
            }
        }
        
        // Sort parts by number (important for S3)
        completed_parts.sort_by(|a, b| a.part_number().cmp(&b.part_number()));

        // Complete Multipart Upload
        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        client.complete_multipart_upload()
            .bucket(bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed complete: {}", e))?;

        // Cleanup DB
        {
             let mut p = progress.lock().unwrap();
             p.remove(&job_id);
             // Optionally clear s3_upload_id from DB now that it's done? 
             let conn = conn_mutex.lock().unwrap();
             // We can set it to NULL or keep it as record. 
             // Setting to NULL is good to indicate "done/no active upload".
             let _ = conn.execute("UPDATE jobs SET s3_upload_id = NULL WHERE id = ?", rusqlite::params![job_id]);
             let _ = conn.execute("UPDATE jobs SET s3_upload_id = NULL WHERE id = ?", rusqlite::params![job_id]);
        }
        
        monitor_handle.abort();

        Ok(true)
    }
}
