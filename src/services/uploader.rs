use anyhow::{Context, Result};
use crate::core::config::Config;
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
use crate::utils::lock_mutex;
use crate::coordinator::ProgressInfo;
use rusqlite::Connection;
use crate::db;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct TaskGuard(pub tokio::task::JoinHandle<()>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}



#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub size: i64,
    pub last_modified: String,
}

pub struct Uploader {}

impl Uploader {
    pub fn new(_config: &Config) -> Self {
        Self {}
    }

    async fn create_client(config: &Config) -> Result<(Client, String)> {
        let bucket = config.s3_bucket.as_deref().unwrap_or_default();
        let region = config.s3_region.as_deref().unwrap_or("us-east-1").trim();
        let endpoint = config.s3_endpoint.as_deref();
        let access_key = config.s3_access_key.as_deref().map(|s| s.trim());
        let secret_key = config.s3_secret_key.as_deref().map(|s| s.trim());

        if bucket.is_empty() {
             return Err(anyhow::anyhow!("S3 bucket not configured"));
        }

        #[allow(deprecated)]
        let mut config_loader = aws_config::from_env().region(aws_config::Region::new(region.to_string()));
        
        if let (Some(ak), Some(sk)) = (access_key, secret_key) {
             let creds = Credentials::new(ak.to_string(), sk.to_string(), None, None, "static");
             config_loader = config_loader.credentials_provider(SharedCredentialsProvider::new(creds));
        } else if access_key.is_some() || secret_key.is_some() {
             return Err(anyhow::anyhow!("S3 Credentials incomplete: Both Access Key and Secret Key must be provided."));
        }

        let sdk_config = config_loader.load().await;
        
        let mut client_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if let Some(endpoint) = endpoint {
             client_builder = client_builder.endpoint_url(endpoint).force_path_style(true);
        }
        let client = Client::from_conf(client_builder.build());
        Ok((client, bucket.to_string()))
    }

    pub async fn list_bucket_contents(config: &Config, prefix_filter: Option<&str>) -> Result<Vec<S3Object>> {
        let (client, bucket) = Self::create_client(config).await?;
        
        let prefix = config.s3_prefix.as_deref().unwrap_or("");
        // Combine config prefix with UI search filter if any? 
        // For now, let's just use the config prefix as the base.
        // User might want to browse different folders? 
        // We will list everything under the configured prefix.
        
        let mut objects = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut req = client.list_objects_v2().bucket(&bucket);
            if !prefix.is_empty() {
                req = req.prefix(prefix);
            }
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.context("Failed to list objects")?;
            
            let is_truncated = resp.is_truncated().unwrap_or(false);
            let next_token = resp.next_continuation_token.clone();

            if let Some(contents) = resp.contents {
                for obj in contents {
                    let key = obj.key().unwrap_or("").to_string();
                    let size = obj.size().unwrap_or(0);
                    let last_modified = obj.last_modified()
                        .map(|d| d.to_string()) // Simplified date
                        .unwrap_or_default();
                    
                    // Filter by user provided prefix/search if needed
                    if let Some(filter) = prefix_filter {
                        if !key.contains(filter) {
                            continue;
                        }
                    }

                    objects.push(S3Object {
                        key,
                        size,
                        last_modified,
                    });
                }
            }

            if is_truncated {
                continuation_token = next_token.map(|s| s.to_string());
            } else {
                break;
            }
        }
        
        Ok(objects)
    }

    pub async fn download_file(config: &Config, key: &str, dest: &Path) -> Result<()> {
        let (client, bucket) = Self::create_client(config).await?;
        
        let resp = client.get_object().bucket(&bucket).key(key).send().await
            .context("Failed to get object")?;
            
        let mut body = resp.body.into_async_read();
        let mut file = tokio::fs::File::create(dest).await.context("Failed to create local file")?;
        
        tokio::io::copy(&mut body, &mut file).await.context("Failed to write to file")?;
        Ok(())
    }

    pub async fn delete_file(config: &Config, key: &str) -> Result<()> {
        let (client, bucket) = Self::create_client(config).await?;
        
        client.delete_object().bucket(&bucket).key(key).send().await
            .context("Failed to delete object")?;
        Ok(())
    }

    pub async fn check_connection(config: &Config) -> Result<String> {
        // Use helper, but construct customized error messages?
        // create_client checks basic config/creds.
        let (client, bucket) = match Self::create_client(config).await {
            Ok(c) => c,
            Err(e) => return Err(e),
        };

        match client.list_buckets().send().await {
            Ok(_) => Ok(format!("Connected to S3 successfully (Bucket: {})", bucket)),
             Err(e) => {
                // Try HeadBucket
                 match client.head_bucket().bucket(&bucket).send().await {
                     Ok(_) => Ok(format!("Connected to S3 successfully (Access to '{}' confirmed)", bucket)),
                     Err(_) => Err(anyhow::anyhow!("S3 Connection Failed: {}", e))
                 }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upload_file(
        &self,
        config: &Config,
        path: &str,
        job_id: i64,
        progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>,
        conn_mutex: Arc<Mutex<Connection>>,
        initial_upload_id: Option<String>,
        cancellation_token: Arc<AtomicBool>,
    ) -> Result<bool> {
        let (client, bucket) = Self::create_client(config).await?;
        let prefix = config.s3_prefix.as_deref().unwrap_or_default();
        let concurrency = config.concurrency_parts_per_file.max(1);
        // create_client already checks bucket


        let file_path = Path::new(path);
        
        // Get the original source path or pre-calculated key from the job
        let s3_key_path = {
            let conn = lock_mutex(&conn_mutex)?;
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
               let mut p = lock_mutex(&progress)?;
               p.insert(job_id, ProgressInfo { 
                   percent: -1.0, 
                   details: "Resuming: Listing parts...".to_string(),
                   parts_done: 0,
                   parts_total: 0,
               });
            }

            // List existing parts
            let list_parts_output = client.list_parts()
                .bucket(&bucket)
                .key(&key)
                .upload_id(uid)
                .send()
                .await;

            match list_parts_output {
                Ok(output) => {
                    if let Some(parts) = output.parts {
                        for p in parts {
                             if let Some(pn) = p.part_number
                                 && let Some(etag) = p.e_tag
                             {
                                     completed_parts.push(CompletedPart::builder().part_number(pn).e_tag(etag).build());
                                     completed_indices.insert(pn);
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
                .bucket(&bucket)
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
                let conn = lock_mutex(&conn_mutex)?;
                db::update_job_upload_id(&conn, job_id, &new_uid)?;
            }
            upload_id = Some(new_uid);
        }
        
        let upload_id = upload_id.expect("Upload ID guaranteed by logic");

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
        let required_min_part = file_size.div_ceil(max_parts);
        part_size = part_size.max(required_min_part as usize);

        // helper to align to nearest MB is nice but not required 
        
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let uploaded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        
        // Initialize uploaded bytes with already completed parts size? 
        // We lack part sizes in list_parts usually (it returns Part struct). 
        // We can estimate or just track new uploads. For progress bar correctness on resume, 
        let initial_bytes: u64 = completed_indices.len() as u64 * part_size as u64;
        uploaded_bytes.store(initial_bytes, std::sync::atomic::Ordering::Relaxed);

        // Monitor State
        let net_bytes = uploaded_bytes.clone();
        let current_part = Arc::new(std::sync::atomic::AtomicU64::new(1));

        let monitor_progress = progress.clone();
        let monitor_net = net_bytes.clone();
        let m_job_id = job_id;
        let m_file_size = file_size;
        let m_part_size = part_size;
        let m_total_parts = (file_size as usize).div_ceil(part_size);

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
                    if let Ok(mut p) = lock_mutex(&monitor_progress) {
                        p.insert(m_job_id, ProgressInfo { 
                            percent: pct, 
                            details, 
                            parts_done: parts_done as usize, 
                            parts_total: m_total_parts
                        });
                    }
                }

                if n >= m_file_size as u64 {
                    // Don't break, keep updating (likely 0 speeds) until aborted by main thread.
                    // This ensures we don't freeze on stale 'high speed' metrics if finalization takes time.
                }
            }
        });
        let _monitor_guard = TaskGuard(monitor_handle);

        let mut handles: Vec<tokio::task::JoinHandle<Result<CompletedPart>>> = Vec::new();
        let mut part_number = 1;
        let mut buffer = vec![0u8; part_size];

        loop {
            // Check cancellation
            if cancellation_token.load(Ordering::Relaxed) {
                // Guards will abort monitor and pending handles (if we wrapped handles? No, handles are just handles)
                for h in handles { h.abort(); }
                return Ok(false);
            }

            // Check if part is already done
            if completed_indices.contains(&part_number) {
                use std::io::SeekFrom;
                let current_pos = (part_number as u64 - 1) * part_size as u64;
                file.seek(SeekFrom::Start(current_pos + part_size as u64)).await.unwrap_or(current_pos); 
                
                part_number += 1;
                // Update tracker
                current_part.store(part_number as u64, std::sync::atomic::Ordering::Relaxed);
                continue;
            }

            // Acquire permit BEFORE reading to throttle memory usage
            let permit = semaphore.clone().acquire_owned().await.context("Semaphore closed")?;

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
                uploaded_bytes.fetch_add(bytes_read as u64, std::sync::atomic::Ordering::Relaxed);

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
            let mut p = lock_mutex(&progress)?;
            if let Some(mut info) = p.get(&job_id).cloned() {
                info.details = "Finalizing S3...".to_string();
                p.insert(job_id, info);
            }
        }
        
        // Sort parts by number (important for S3)
        completed_parts.sort_by_key(|a| a.part_number());

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
            
        // monitor_handle aborted by TaskGuard drop

        // Cleanup DB
        {
             let mut p = lock_mutex(&progress)?;
             p.remove(&job_id);
             // Optionally clear s3_upload_id from DB now that it's done? 
             let conn = lock_mutex(&conn_mutex)?;
             // We can set it to NULL or keep it as record. 
             // Setting to NULL is good to indicate "done/no active upload".
             let _ = conn.execute("UPDATE jobs SET s3_upload_id = NULL WHERE id = ?", rusqlite::params![job_id]);
        }
        
        // monitor_handle aborted by TaskGuard

        Ok(true)
    }
}
