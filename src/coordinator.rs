use crate::core::config::Config;
use crate::db::{self, JobRow};
use crate::services::scanner::{Scanner, ScanResult};
use crate::services::uploader::Uploader;
use anyhow::Result;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::path::PathBuf;

use std::collections::HashMap;
use crate::utils::{lock_mutex, lock_async_mutex};
use tracing::{info, error};

#[derive(Debug, Clone)]
pub struct ProgressInfo {
    pub percent: f64, // < 0.0 means "Calculating/Indeterminate"
    pub details: String,
    pub parts_done: usize,
    pub parts_total: usize,
}

#[derive(Clone)]
pub struct Coordinator {
    conn: Arc<Mutex<Connection>>,
    config: Arc<AsyncMutex<Config>>,
    scanner: Scanner,
    uploader: Uploader,
    progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>>,
    cancellation_tokens: Arc<AsyncMutex<HashMap<i64, Arc<AtomicBool>>>>,
}

impl Coordinator {
    pub fn new(conn: Arc<Mutex<Connection>>, config: Arc<AsyncMutex<Config>>, progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>>, cancellation_tokens: Arc<AsyncMutex<HashMap<i64, Arc<AtomicBool>>>>) -> Result<Self> {
        // We need lock to init scanner/uploader but they are just helpers now or cheap to init
        // Note: Initializing services might need config, but for now we assume they don't deep copy config state 
        // OR we need to async lock here. But new() is sync.
        // Scanner/Uploader::new take &Config.
        // Block on the lock since we are in initialization phase (likely sync main) or create detached?
        // Actually Uploader::new takes &Config but just creates Self {}. It doesn't use it.
        // Scanner::new takes &Config and stores clamav host/port.
        // We need to peek at config.
        
        let cfg = futures::executor::block_on(config.lock());
        let scanner = Scanner::new(&cfg);
        let uploader = Uploader::new(&cfg);
        drop(cfg);
        
        Ok(Self { conn, config, scanner, uploader, progress, cancellation_tokens })
    }

    pub async fn run(&self) {
        loop {
            if let Err(e) = self.process_cycle().await {
                eprintln!("Coordinator error: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn check_and_report(&self, session_id: &str) -> Result<()> {
        let pending = {
            let conn = lock_mutex(&self.conn)?;
            db::count_pending_session_jobs(&conn, session_id)?
        };

        if pending == 0 {
            let config = lock_async_mutex(&self.config).await.clone();
            let conn = lock_mutex(&self.conn)?;
            info!("Session {} complete. Generating report.", session_id);
            use crate::services::reporter::Reporter;
            Reporter::generate_report(&conn, &config, session_id)?;
        }
        Ok(())
    }

    async fn process_cycle(&self) -> Result<()> {
        // 1. Check for retryable jobs
        {
            let conn = lock_mutex(&self.conn)?;
            if let Ok(retry_jobs) = db::list_retryable_jobs(&conn) {
                for job in retry_jobs {
                    let next_status = if job.scan_status.as_deref() == Some("clean") || job.scan_status.as_deref() == Some("scanned") {
                        "scanned"
                    } else {
                        "queued"
                    };
                    
                    info!("Retrying job {} (Attempt #{})", job.id, job.retry_count + 1);
                    db::update_job_retry_state(&conn, job.id, job.retry_count + 1, None, next_status, "Retrying...")?;
                    db::insert_event(&conn, job.id, "retry", &format!("Auto-retry attempt #{}", job.retry_count + 1))?;
                }
            }
        }

        // 2. Try starting scans (Limit: 2 concurrent file scans)
        let scanner_enabled = lock_async_mutex(&self.config).await.scanner_enabled;
        let active_scans = {
            let conn = lock_mutex(&self.conn)?;
            db::count_jobs_with_status(&conn, "scanning")?
        };

        if active_scans < 2 {
            let queued_job = {
                let conn = lock_mutex(&self.conn)?;
                if let Some(job) = db::get_next_job(&conn, "queued")? {
                    if scanner_enabled {
                        db::update_scan_status(&conn, job.id, "scanning", "scanning")?;
                        Some(job)
                    } else {
                        db::update_scan_status(&conn, job.id, "skipped", "scanned")?;
                        db::insert_event(&conn, job.id, "scan", "scan skipped by policy")?;
                        None
                    }
                } else {
                    None
                }
            };

            if let Some(job) = queued_job {
                let coord = self.clone();
                tokio::spawn(async move {
                    let _ = coord.process_scan(&job).await;
                });
            }
        }

        // 3. Try starting uploads
        let (max_uploads, active_uploads) = {
            let cfg = lock_async_mutex(&self.config).await;
            let conn = lock_mutex(&self.conn)?;
            (cfg.concurrency_upload_global, db::count_jobs_with_status(&conn, "uploading")?)
        };

        if active_uploads < max_uploads as i64 {
            let scanned_job = {
                let conn = lock_mutex(&self.conn)?;
                if let Some(job) = db::get_next_job(&conn, "scanned")? {
                    db::update_upload_status(&conn, job.id, "uploading", "uploading")?;
                    Some(job)
                } else {
                    None
                }
            };

            if let Some(job) = scanned_job {
                let coord = self.clone();
                tokio::spawn(async move {
                    let _ = coord.process_upload(&job).await;
                });
            }
        }

        Ok(())
    }

    async fn process_scan(&self, job: &JobRow) -> Result<()> {
        let path = match &job.staged_path {
            Some(p) => p,
            None => {
                 let conn = lock_mutex(&self.conn)?;
                 db::update_job_error(&conn, job.id, "failed", "no staged path")?;
                 return Ok(());
            }
        };

        let start_time = std::time::Instant::now();
        match self.scanner.scan_file(path).await {
            Ok(ScanResult::Clean) => {
                 let duration = start_time.elapsed().as_millis() as i64;
                 let conn = lock_mutex(&self.conn)?;
                 db::update_scan_status(&conn, job.id, "clean", "scanned")?;
                 db::update_scan_duration(&conn, job.id, duration)?;
                 db::insert_event(&conn, job.id, "scan", &format!("scan completed in {}ms", duration))?;
            }
            Ok(ScanResult::Infected(virus_name)) => {
                 let (quarantine_dir, _delete_source) = {
                    let cfg = lock_async_mutex(&self.config).await;
                    (PathBuf::from(&cfg.quarantine_dir), cfg.delete_source_after_upload)
                 };
                 
                 if !quarantine_dir.exists() {
                     let _ = std::fs::create_dir_all(&quarantine_dir);
                 }
                 
                 let file_name = std::path::Path::new(path).file_name();
                 let mut quarantine_path_str = String::new();
                 
                 if let Some(fname) = file_name {
                     let dest = quarantine_dir.join(fname);
                     if let Err(e) = std::fs::rename(path, &dest) {
                         eprintln!("Failed to quarantine file: {}", e);
                     } else {
                         quarantine_path_str = dest.to_string_lossy().to_string();
                     }
                 }

                 {
                     let conn = lock_mutex(&self.conn)?;
                     db::update_scan_status(&conn, job.id, "infected", "quarantined")?;
                     
                     // Store virus name in error column so it appears in details
                     db::update_job_error(&conn, job.id, "quarantined", &format!("Infected: {}", virus_name))?;
                     
                     if !quarantine_path_str.is_empty() {
                         let _ = db::update_job_staged(&conn, job.id, &quarantine_path_str, "quarantined");
                     }
                     db::insert_event(&conn, job.id, "scan", &format!("scan failed: infected with {}", virus_name))?;
                 }
                 self.check_and_report(&job.session_id).await?;
            }
            Err(e) => {
                 {
                     let conn = lock_mutex(&self.conn)?;
                     db::update_job_error(&conn, job.id, "failed", &format!("scan error: {}", e))?;
                 }
                 self.check_and_report(&job.session_id).await?;
            }
        }
        Ok(())
    }

    async fn process_upload(&self, job: &JobRow) -> Result<()> {
        let path = match &job.staged_path {
             Some(p) => p.clone(),
             None => return Ok(()),
        };
        
        let config = {
            let config_guard = lock_async_mutex(&self.config).await;
            config_guard.clone()
        };

        // Set status to "uploading" BEFORE starting upload
        {
            let conn = lock_mutex(&self.conn)?;
            db::update_upload_status(&conn, job.id, "starting", "uploading")?;
        }
        
        let cancel_token = Arc::new(AtomicBool::new(false));
        {
            lock_async_mutex(&self.cancellation_tokens).await.insert(job.id, cancel_token.clone());
        }

        let start_time = std::time::Instant::now();
        let res = self.uploader.upload_file(
            &config, 
            &path, 
            job.id, 
            self.progress.clone(),
            self.conn.clone(),
            job.s3_upload_id.clone(),
            cancel_token
        ).await;
        
        // Remove token
        {
            lock_async_mutex(&self.cancellation_tokens).await.remove(&job.id);
        }

        match res {
            Ok(true) => {
                {
                    let duration = start_time.elapsed().as_millis() as i64;
                    let conn = lock_mutex(&self.conn)?;
                    db::update_upload_status(&conn, job.id, "completed", "complete")?;
                    db::update_upload_duration(&conn, job.id, duration)?;
                    db::insert_event(&conn, job.id, "upload", &format!("upload completed in {}ms", duration))?;
                }
                
                // Cleanup based on staging mode
                use crate::core::config::StagingMode;
                let staged_path = std::path::Path::new(&path);
                match config.staging_mode {
                    StagingMode::Copy => {
                        let _ = std::fs::remove_file(staged_path);
                        if let Some(parent) = staged_path.parent() {
                            let _ = std::fs::remove_dir(parent); 
                        }
                    }
                    StagingMode::Direct => {
                        if config.delete_source_after_upload {
                            let _ = std::fs::remove_file(staged_path);
                        }
                    }
                }
                
                self.check_and_report(&job.session_id).await?;
            }
            Ok(false) => {
                // Cancelled or Paused
                {
                    let conn = lock_mutex(&self.conn)?;
                    let current_status = db::get_job(&conn, job.id)?
                        .map(|j| j.status)
                        .unwrap_or_else(|| "unknown".to_string());

                    if current_status == "paused" {
                         db::insert_event(&conn, job.id, "upload", "upload paused")?;
                    } else {
                         db::insert_event(&conn, job.id, "upload", "upload cancelled")?;
                    }
                }
                self.check_and_report(&job.session_id).await?;
            }
            Err(e) => {
                let max_retries = 5;
                let should_report = {
                    let conn = lock_mutex(&self.conn)?;
                    if job.retry_count < max_retries {
                        // Exponential backoff: 5s, 10s, 20s, 40s, 80s
                        let backoff_secs = 5 * (2_u64.pow(job.retry_count as u32));
                        let next_retry = chrono::Utc::now() + chrono::Duration::seconds(backoff_secs as i64);
                        let next_retry_str = next_retry.to_rfc3339();
                        
                        error!("Upload failed for job {}: {}. Retrying in {}s...", job.id, e, backoff_secs);
                        
                        db::update_job_retry_state(
                            &conn, 
                            job.id, 
                            job.retry_count, 
                            Some(&next_retry_str), 
                            "retry_pending", 
                            &format!("Failed: {}. Retry pending.", e)
                        )?;
                        
                        db::insert_event(&conn, job.id, "retry_scheduled", &format!("Scheduled retry in {}s", backoff_secs))?;
                        false
                    } else {
                        error!("Upload failed for job {} after {} retries: {}", job.id, job.retry_count, e);
                        db::update_job_error(&conn, job.id, "failed", &format!("Max retries exceeded. Error: {}", e))?;
                        true
                    }
                };

                if should_report {
                    self.check_and_report(&job.session_id).await?;
                }
            }
        }
        
        // Check for report if we failed permanently (max retries)
        // We can't do it easily inside the match arm above without refactoring the if/else or using drop.
        // But wait, I need to check report only on failure terminal state.
        
        Ok(())
    }
}
