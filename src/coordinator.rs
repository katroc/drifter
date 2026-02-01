use crate::core::config::Config;
use crate::db::{self, JobRow};
use crate::services::scanner::Scanner;
use crate::services::uploader::Uploader;
use anyhow::Result;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::path::PathBuf;

use std::collections::HashMap;
use crate::utils::lock_mutex;

#[derive(Debug, Clone)]
pub struct ProgressInfo {
    pub percent: f64, // < 0.0 means "Calculating/Indeterminate"
    pub details: String,
    pub parts_done: usize,
    pub parts_total: usize,
}

pub struct Coordinator {
    conn: Arc<Mutex<Connection>>,
    config: Arc<Mutex<Config>>,
    scanner: Scanner,
    uploader: Uploader,
    progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>,
    cancellation_tokens: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>,
}

impl Coordinator {
    pub fn new(conn: Arc<Mutex<Connection>>, config: Arc<Mutex<Config>>, progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>, cancellation_tokens: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>) -> Result<Self> {
        // We need lock to init scanner/uploader but they are just helpers now or cheap to init
        let cfg = lock_mutex(&config)?;
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

    async fn process_cycle(&self) -> Result<()> {
        let (job, scanner_enabled) = {
            let conn = lock_mutex(&self.conn)?;
            
            if let Some(job) = db::get_next_job(&conn, "queued")? {
                let scanner_enabled = lock_mutex(&self.config)?.scanner_enabled;
                (Some(job), scanner_enabled)
            } else if let Some(job) = db::get_next_job(&conn, "scanned")? {
                (Some(job), false) // scanner false means move to upload
            } else {
                (None, false)
            }
        };

        if let Some(job) = job {
            if job.status == "queued" {
                if scanner_enabled {
                    {
                        let conn = lock_mutex(&self.conn)?;
                        db::update_scan_status(&conn, job.id, "scanning", "scanning")?;
                    }
                    self.process_scan(&job).await?;
                } else {
                    {
                        let conn = lock_mutex(&self.conn)?;
                        db::update_upload_status(&conn, job.id, "uploading", "uploading")?;
                        db::insert_event(&conn, job.id, "scan", "scan skipped by policy")?;
                    }
                    self.process_upload(&job).await?;
                }
            } else if job.status == "scanned" {
                {
                    let conn = lock_mutex(&self.conn)?;
                    db::update_upload_status(&conn, job.id, "uploading", "uploading")?;
                }
                self.process_upload(&job).await?;
            }
            return Ok(());
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

        match self.scanner.scan_file(path).await {
            Ok(true) => {
                 let conn = lock_mutex(&self.conn)?;
                 db::update_scan_status(&conn, job.id, "clean", "scanned")?;
                 db::insert_event(&conn, job.id, "scan", "scan completed")?;
            }
            Ok(false) => {
                 let (quarantine_dir, _delete_source) = {
                    let cfg = lock_mutex(&self.config)?;
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

                 let conn = lock_mutex(&self.conn)?;
                 db::update_scan_status(&conn, job.id, "infected", "quarantined")?;
                 if !quarantine_path_str.is_empty() {
                     let _ = db::update_job_staged(&conn, job.id, &quarantine_path_str, "quarantined");
                 }
                 db::insert_event(&conn, job.id, "scan", "scan failed: infected (quarantined)")?;
            }
            Err(e) => {
                 let conn = lock_mutex(&self.conn)?;
                 db::update_job_error(&conn, job.id, "failed", &format!("scan error: {}", e))?;
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
            let config_guard = lock_mutex(&self.config)?;
            config_guard.clone()
        };

        // Set status to "uploading" BEFORE starting upload
        {
            let conn = lock_mutex(&self.conn)?;
            db::update_upload_status(&conn, job.id, "starting", "uploading")?;
        }

        // Register cancellation token
        let cancel_token = Arc::new(AtomicBool::new(false));
        {
            lock_mutex(&self.cancellation_tokens)?.insert(job.id, cancel_token.clone());
        }

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
            lock_mutex(&self.cancellation_tokens)?.remove(&job.id);
        }

        match res {
            Ok(true) => {
                {
                    let conn = lock_mutex(&self.conn)?;
                    db::update_upload_status(&conn, job.id, "completed", "complete")?;
                    db::insert_event(&conn, job.id, "upload", "upload completed")?;
                }
                
                // Cleanup based on staging mode
                use crate::config::StagingMode;
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
            }
            Ok(false) => {
                // Cancelled or Paused
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
            Err(e) => {
                let conn = lock_mutex(&self.conn)?;
                db::update_job_error(&conn, job.id, "failed", &format!("upload error: {}", e))?;
            }
        }
        Ok(())
    }
}
