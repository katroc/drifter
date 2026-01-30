use crate::config::Config;
use crate::db::{self, JobRow};
use crate::scanner::Scanner;
use crate::uploader::Uploader;
use anyhow::Result;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

use std::collections::HashMap;

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
    runtime: Arc<Runtime>,
    scanner: Scanner,
    uploader: Uploader,
    progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>,
    cancellation_tokens: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>,
}

impl Coordinator {
    pub fn new(conn: Arc<Mutex<Connection>>, config: Arc<Mutex<Config>>, progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>, cancellation_tokens: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>) -> Result<Self> {
        let runtime = Arc::new(Runtime::new()?);
        // We need lock to init scanner/uploader but they are just helpers now or cheap to init
        let cfg = config.lock().unwrap();
        let scanner = Scanner::new(&cfg);
        let uploader = Uploader::new(&cfg);
        drop(cfg);
        
        Ok(Self { conn, config, runtime, scanner, uploader, progress, cancellation_tokens })
    }

    pub fn run(&self) {
        loop {
            if let Err(e) = self.process_cycle() {
                eprintln!("Coordinator error: {}", e);
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    fn process_cycle(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        // Find jobs in "queued" state
        if let Some(job) = db::get_next_job(&conn, "queued")? {
            let scanner_enabled = self.config.lock().unwrap().scanner_enabled;
            if scanner_enabled {
                // Move to scanning
                db::update_scan_status(&conn, job.id, "scanning", "scanning")?;
                // Process scan
                drop(conn); 
                self.process_scan(&job)?;
            } else {
                // Skip scan, move directly to uploading (or ready for upload)
                // We fake a "clean" scan result but go straight to uploading to avoid UI flicker
                db::update_upload_status(&conn, job.id, "uploading", "uploading")?;
                db::insert_event(&conn, job.id, "scan", "scan skipped by policy")?;
                
                drop(conn);
                self.process_upload(&job)?;
            }
            return Ok(()); // Loop again
        }

        // Find jobs in "scanned" state -> move to "uploading"
        if let Some(job) = db::get_next_job(&conn, "scanned")? {
             db::update_upload_status(&conn, job.id, "uploading", "uploading")?;
             drop(conn);
             self.process_upload(&job)?;
             return Ok(());
        }

        Ok(())
    }

    fn process_scan(&self, job: &JobRow) -> Result<()> {
        let path = match &job.staged_path {
            Some(p) => p,
            None => {
                // Should not happen if queued
                 let conn = self.conn.lock().unwrap();
                 db::update_job_error(&conn, job.id, "failed", "no staged path")?;
                 return Ok(());
            }
        };

        match self.runtime.block_on(self.scanner.scan_file(path)) {
            Ok(true) => {
                 let conn = self.conn.lock().unwrap();
                 db::update_scan_status(&conn, job.id, "clean", "scanned")?;
                 db::insert_event(&conn, job.id, "scan", "scan completed")?;
            }
            Ok(false) => {
                 let cfg = self.config.lock().unwrap();
                 let quarantine_dir = std::path::Path::new(&cfg.quarantine_dir);
                 if !quarantine_dir.exists() {
                     let _ = std::fs::create_dir_all(quarantine_dir);
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
                 drop(cfg);

                 let conn = self.conn.lock().unwrap();
                 db::update_scan_status(&conn, job.id, "infected", "quarantined")?;
                 if !quarantine_path_str.is_empty() {
                     let _ = db::update_job_staged(&conn, job.id, &quarantine_path_str, "quarantined");
                 }
                 db::insert_event(&conn, job.id, "scan", "scan failed: infected (quarantined)")?;
            }
            Err(e) => {
                 let conn = self.conn.lock().unwrap();
                 db::update_job_error(&conn, job.id, "failed", &format!("scan error: {}", e))?;
            }
        }
        Ok(())
    }

    fn process_upload(&self, job: &JobRow) -> Result<()> {
        let path = match &job.staged_path {
             Some(p) => p,
             None => return Ok(()),
        };
        
        let config_guard = self.config.lock().unwrap();
        // Clone config to pass into async block or ...? 
        // uploader.upload_file takes &Config. We can't hold mutex across await. 
        // So we should clone the Config. Config is cloneable.
        let config = config_guard.clone();
        drop(config_guard);

        // Set status to "uploading" BEFORE starting upload
        {
            let conn = self.conn.lock().unwrap();
            db::update_upload_status(&conn, job.id, "starting", "uploading")?;
        }

        // Register cancellation token
        let cancel_token = Arc::new(AtomicBool::new(false));
        self.cancellation_tokens.lock().unwrap().insert(job.id, cancel_token.clone());

        let res = self.runtime.block_on(self.uploader.upload_file(
            &config, 
            path, 
            job.id, 
            self.progress.clone(),
            self.conn.clone(),
            job.s3_upload_id.clone(),
            cancel_token
        ));
        
        // Remove token
        self.cancellation_tokens.lock().unwrap().remove(&job.id);
        match res {
            Ok(true) => {
                let conn = self.conn.lock().unwrap();
                db::update_upload_status(&conn, job.id, "completed", "complete")?;
                db::insert_event(&conn, job.id, "upload", "upload completed")?;
                
                // Cleanup based on staging mode
                use crate::config::StagingMode;
                if let Some(staged) = &job.staged_path {
                    let staged_path = std::path::Path::new(staged);
                    match config.staging_mode {
                        StagingMode::Copy => {
                            // Copy mode: delete staged file and its parent directory
                            let _ = std::fs::remove_file(staged_path);
                            if let Some(parent) = staged_path.parent() {
                                let _ = std::fs::remove_dir(parent); // Only removes if empty
                            }
                        }
                        StagingMode::Direct => {
                            // Direct mode: only delete source if configured to do so
                            if config.delete_source_after_upload {
                                let _ = std::fs::remove_file(staged_path);
                            }
                        }
                    }
                }
            }
            Ok(false) => {
                // Cancelled
                let conn = self.conn.lock().unwrap();
                // We use update_job_error for now or a specific status?
                // Actually cancel_job_to_history in db would set it to cancelled.
                // But if we returned false, it means we detected cancellation flag.
                // The TUI likely already updated the DB status to 'cancelled'.
                // But we should ensure we log an event.
                 db::insert_event(&conn, job.id, "upload", "upload cancelled")?;
            }
            Err(e) => {
                let conn = self.conn.lock().unwrap();
                db::update_job_error(&conn, job.id, "failed", &format!("upload error: {}", e))?;
            }
        }
        Ok(())
    }
}
