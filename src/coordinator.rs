use crate::app::state::AppEvent;
use crate::core::config::Config;
use crate::db::{self, JobRow};
use crate::services::scanner::{ScanResult, Scanner};
use crate::services::uploader::Uploader;
use anyhow::Result;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

use crate::utils::{lock_async_mutex, lock_mutex};
use std::collections::HashMap;
use tracing::{error, info, warn};

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
    progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>>,
    cancellation_tokens: Arc<AsyncMutex<HashMap<i64, Arc<AtomicBool>>>>,
    app_tx: mpsc::Sender<AppEvent>,
}

impl Coordinator {
    pub fn new(
        conn: Arc<Mutex<Connection>>,
        config: Arc<AsyncMutex<Config>>,
        progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>>,
        cancellation_tokens: Arc<AsyncMutex<HashMap<i64, Arc<AtomicBool>>>>,
        app_tx: mpsc::Sender<AppEvent>,
    ) -> Result<Self> {
        Ok(Self {
            conn,
            config,
            progress,
            cancellation_tokens,
            app_tx,
        })
    }

    pub async fn run(&self) {
        loop {
            if let Err(e) = self.process_cycle().await {
                error!("Coordinator error: {}", e);
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

    pub async fn process_cycle(&self) -> Result<()> {
        // 1. Check for retryable jobs
        {
            let conn = lock_mutex(&self.conn)?;
            if let Ok(retry_jobs) = db::list_retryable_jobs(&conn) {
                for job in retry_jobs {
                    let next_status = if job.scan_status.as_deref() == Some("clean")
                        || job.scan_status.as_deref() == Some("scanned")
                    {
                        "scanned"
                    } else {
                        "queued"
                    };

                    info!("Retrying job {} (Attempt #{})", job.id, job.retry_count + 1);
                    db::update_job_retry_state(
                        &conn,
                        job.id,
                        job.retry_count + 1,
                        None,
                        next_status,
                        "Retrying...",
                    )?;
                    db::insert_event(
                        &conn,
                        job.id,
                        "retry",
                        &format!("Auto-retry attempt #{}", job.retry_count + 1),
                    )?;
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
                    if let Err(e) = coord.process_scan(&job).await {
                        error!("Scan orchestration failed for job {}: {}", job.id, e);
                    }
                });
            }
        }

        // 3. Try starting uploads
        let (max_uploads, active_uploads) = {
            let cfg = lock_async_mutex(&self.config).await;
            let conn = lock_mutex(&self.conn)?;
            (
                cfg.concurrency_upload_global,
                db::count_jobs_with_status(&conn, "uploading")?,
            )
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
                    if let Err(e) = coord.process_upload(&job).await {
                        error!("Upload orchestration failed for job {}: {}", job.id, e);
                    }
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

        let scanner = {
            let cfg = lock_async_mutex(&self.config).await.clone();
            Scanner::new(&cfg)
        };

        let start_time = std::time::Instant::now();
        match scanner.scan_file(path).await {
            Ok(ScanResult::Clean) => {
                let duration = start_time.elapsed().as_millis() as i64;
                let conn = lock_mutex(&self.conn)?;
                db::update_scan_status(&conn, job.id, "clean", "scanned")?;
                db::update_scan_duration(&conn, job.id, duration)?;
                db::insert_event(
                    &conn,
                    job.id,
                    "scan",
                    &format!("scan completed in {}ms", duration),
                )?;
            }
            Ok(ScanResult::Infected(virus_name)) => {
                let (quarantine_dir, _delete_source) = {
                    let cfg = lock_async_mutex(&self.config).await;
                    (
                        PathBuf::from(&cfg.quarantine_dir),
                        cfg.delete_source_after_upload,
                    )
                };

                if !quarantine_dir.exists()
                    && let Err(e) = std::fs::create_dir_all(&quarantine_dir)
                {
                    error!(
                        "Failed to create quarantine directory {:?}: {}",
                        quarantine_dir, e
                    );
                }

                let file_name = std::path::Path::new(path).file_name();
                let mut quarantine_path_str = String::new();

                if let Some(fname) = file_name {
                    let dest = quarantine_dir.join(fname);
                    if let Err(e) = std::fs::rename(path, &dest) {
                        error!("Failed to quarantine file: {}", e);
                    } else {
                        quarantine_path_str = dest.to_string_lossy().to_string();
                    }
                }

                {
                    let conn = lock_mutex(&self.conn)?;
                    db::update_scan_status(&conn, job.id, "infected", "quarantined")?;

                    // Store virus name in error column so it appears in details
                    db::update_job_error(
                        &conn,
                        job.id,
                        "quarantined",
                        &format!("Infected: {}", virus_name),
                    )?;

                    if !quarantine_path_str.is_empty()
                        && let Err(e) = db::update_job_staged(
                            &conn,
                            job.id,
                            &quarantine_path_str,
                            "quarantined",
                        )
                    {
                        warn!(
                            "Failed to update quarantined staged path for job {}: {}",
                            job.id, e
                        );
                    }
                    db::insert_event(
                        &conn,
                        job.id,
                        "scan",
                        &format!("scan failed: infected with {}", virus_name),
                    )?;
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
        let uploader = Uploader::new(&config);

        // Set status to "uploading" BEFORE starting upload
        {
            let conn = lock_mutex(&self.conn)?;
            db::update_upload_status(&conn, job.id, "starting", "uploading")?;
        }

        let cancel_token = Arc::new(AtomicBool::new(false));
        {
            lock_async_mutex(&self.cancellation_tokens)
                .await
                .insert(job.id, cancel_token.clone());
        }

        let start_time = std::time::Instant::now();
        let res = uploader
            .upload_file(
                &config,
                &path,
                job.id,
                self.progress.clone(),
                self.conn.clone(),
                job.s3_upload_id.clone(),
                cancel_token,
            )
            .await;

        // Remove token
        {
            lock_async_mutex(&self.cancellation_tokens)
                .await
                .remove(&job.id);
        }

        match res {
            Ok(true) => {
                {
                    let duration = start_time.elapsed().as_millis() as i64;
                    let conn = lock_mutex(&self.conn)?;
                    db::update_upload_status(&conn, job.id, "completed", "complete")?;
                    db::update_upload_duration(&conn, job.id, duration)?;
                    db::insert_event(
                        &conn,
                        job.id,
                        "upload",
                        &format!("upload completed in {}ms", duration),
                    )?;
                }

                let staged_path = std::path::Path::new(&path);
                if job.source_path != path {
                    Self::remove_file_if_exists(staged_path, "staged file");
                    if let Some(parent) = staged_path.parent() {
                        Self::remove_dir_if_exists(parent, "staging directory");
                    }
                } else if config.delete_source_after_upload {
                    Self::remove_file_if_exists(staged_path, "source file");
                }

                // Signal TUI to refresh remote panel
                if let Err(e) = self.app_tx.send(AppEvent::RefreshRemote) {
                    warn!("Failed to send RefreshRemote event: {}", e);
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
                        let backoff_secs = Self::calculate_backoff_seconds(job.retry_count);
                        let next_retry =
                            chrono::Utc::now() + chrono::Duration::seconds(backoff_secs as i64);
                        let next_retry_str = next_retry.to_rfc3339();

                        error!(
                            "Upload failed for job {}: {}. Retrying in {}s...",
                            job.id, e, backoff_secs
                        );

                        db::update_job_retry_state(
                            &conn,
                            job.id,
                            job.retry_count,
                            Some(&next_retry_str),
                            "retry_pending",
                            &format!("Failed: {}. Retry pending.", e),
                        )?;

                        db::insert_event(
                            &conn,
                            job.id,
                            "retry_scheduled",
                            &format!("Scheduled retry in {}s", backoff_secs),
                        )?;
                        false
                    } else {
                        error!(
                            "Upload failed for job {} after {} retries: {}",
                            job.id, job.retry_count, e
                        );
                        db::update_job_error(
                            &conn,
                            job.id,
                            "failed",
                            &format!("Max retries exceeded. Error: {}", e),
                        )?;
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

    /// Calculate exponential backoff delay in seconds
    /// Formula: 5 * (2^retry_count)
    /// Results: 5s, 10s, 20s, 40s, 80s...
    pub fn calculate_backoff_seconds(retry_count: i64) -> u64 {
        5 * (2_u64.pow(retry_count as u32))
    }

    fn remove_file_if_exists(path: &std::path::Path, label: &str) {
        if let Err(e) = std::fs::remove_file(path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!("Failed to remove {} {:?}: {}", label, path, e);
        }
    }

    fn remove_dir_if_exists(path: &std::path::Path, label: &str) {
        if let Err(e) = std::fs::remove_dir(path)
            && e.kind() != std::io::ErrorKind::NotFound
            && e.kind() != std::io::ErrorKind::DirectoryNotEmpty
        {
            warn!("Failed to remove {} {:?}: {}", label, path, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Exponential Backoff Tests ---

    #[test]
    fn test_calculate_backoff_first_retry() {
        let backoff = Coordinator::calculate_backoff_seconds(0);
        assert_eq!(backoff, 5, "First retry should wait 5 seconds");
    }

    #[test]
    fn test_calculate_backoff_second_retry() {
        let backoff = Coordinator::calculate_backoff_seconds(1);
        assert_eq!(backoff, 10, "Second retry should wait 10 seconds");
    }

    #[test]
    fn test_calculate_backoff_third_retry() {
        let backoff = Coordinator::calculate_backoff_seconds(2);
        assert_eq!(backoff, 20, "Third retry should wait 20 seconds");
    }

    #[test]
    fn test_calculate_backoff_fourth_retry() {
        let backoff = Coordinator::calculate_backoff_seconds(3);
        assert_eq!(backoff, 40, "Fourth retry should wait 40 seconds");
    }

    #[test]
    fn test_calculate_backoff_fifth_retry() {
        let backoff = Coordinator::calculate_backoff_seconds(4);
        assert_eq!(backoff, 80, "Fifth retry should wait 80 seconds");
    }

    #[test]
    fn test_calculate_backoff_sequence() {
        // Verify the complete retry sequence: 5s, 10s, 20s, 40s, 80s
        let expected = [5, 10, 20, 40, 80];

        for (retry_count, expected_delay) in expected.iter().enumerate() {
            let backoff = Coordinator::calculate_backoff_seconds(retry_count as i64);
            assert_eq!(
                backoff, *expected_delay,
                "Retry {} should have backoff of {}s, got {}s",
                retry_count, expected_delay, backoff
            );
        }
    }

    #[test]
    fn test_calculate_backoff_doubles_each_time() {
        // Verify exponential growth property
        for retry_count in 0..5 {
            let current = Coordinator::calculate_backoff_seconds(retry_count);
            let next = Coordinator::calculate_backoff_seconds(retry_count + 1);

            assert_eq!(
                next,
                current * 2,
                "Backoff should double: retry {} = {}s, retry {} = {}s",
                retry_count,
                current,
                retry_count + 1,
                next
            );
        }
    }

    #[test]
    fn test_calculate_backoff_large_retry_count() {
        // Test with larger retry count (though app limits to 5)
        let backoff = Coordinator::calculate_backoff_seconds(10);
        assert_eq!(backoff, 5 * 1024); // 5 * 2^10 = 5120 seconds
    }

    #[test]
    fn test_calculate_backoff_max_retries_boundary() {
        // Test at the max retry boundary (5 retries = retry_count 0-4)
        let max_retries = 5;
        let last_backoff = Coordinator::calculate_backoff_seconds((max_retries - 1) as i64);

        // After 5th retry (retry_count=4), backoff should be 80s
        assert_eq!(last_backoff, 80);

        // Total wait time across all retries: 5 + 10 + 20 + 40 + 80 = 155 seconds
        let total_wait: u64 = (0..max_retries)
            .map(|i| Coordinator::calculate_backoff_seconds(i as i64))
            .sum();
        assert_eq!(total_wait, 155);
    }

    // --- Orchestration Tests ---

    fn setup_test_db() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL DEFAULT 'legacy',
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                source_path TEXT NOT NULL,
                staged_path TEXT,
                size_bytes INTEGER NOT NULL,
                scan_status TEXT,
                upload_status TEXT,
                s3_bucket TEXT,
                s3_key TEXT,
                s3_upload_id TEXT,
                checksum TEXT,
                remote_checksum TEXT,
                error TEXT,
                priority INTEGER DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                next_retry_at TEXT,
                scan_duration_ms INTEGER,
                upload_duration_ms INTEGER
            );
            CREATE TABLE uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                upload_id TEXT,
                part_size INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(id)
            );
            CREATE TABLE parts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                upload_id INTEGER NOT NULL,
                part_number INTEGER NOT NULL,
                etag TEXT,
                checksum_sha256 TEXT,
                size_bytes INTEGER NOT NULL,
                status TEXT NOT NULL,
                retries INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(upload_id) REFERENCES uploads(id)
            );
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(id)
            );
            CREATE TABLE secrets (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            ",
        )?;
        Ok(conn)
    }

    // Helper to setup a test coordinator
    async fn setup_coordinator(
        scanner_enabled: bool,
    ) -> Result<(Coordinator, Arc<Mutex<Connection>>)> {
        let conn = Arc::new(Mutex::new(setup_test_db()?));
        let config = Config {
            scanner_enabled,
            s3_bucket: Some("test-bucket".to_string()),
            ..Default::default()
        };

        let config = Arc::new(AsyncMutex::new(config));
        let progress = Arc::new(AsyncMutex::new(HashMap::new()));
        let cancel = Arc::new(AsyncMutex::new(HashMap::new()));
        let (app_tx, _app_rx) = mpsc::channel();

        let coord = Coordinator::new(conn.clone(), config, progress, cancel, app_tx)?;
        Ok((coord, conn))
    }

    #[tokio::test]
    async fn test_coordinator_skips_scan_when_disabled() -> Result<()> {
        let (coord, conn) = setup_coordinator(false).await?;

        // Create a queued job
        let job_id = {
            let c = lock_mutex(&conn)?;
            let id = db::create_job(&c, "session1", "/tmp/file.txt", 100, None)?;
            db::update_job_staged(&c, id, "/tmp/staged.txt", "queued")?;
            id
        };

        // Run cycle
        coord.process_cycle().await?;

        // Verify state changed. It might reach 'uploading' in one cycle if uploader is also ready
        let c = lock_mutex(&conn)?;
        let job = db::get_job(&c, job_id)?.expect("Job not found");

        // It goes queued -> scanned (skipped) -> uploading
        assert_eq!(job.status, "uploading");
        assert_eq!(job.scan_status, Some("skipped".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_coordinator_picks_up_upload() -> Result<()> {
        let (coord, conn) = setup_coordinator(true).await?;

        // Create a scanned job ready for upload
        let job_id = {
            let c = lock_mutex(&conn)?;
            let id = db::create_job(&c, "session1", "/tmp/file.txt", 100, None)?;
            db::update_job_staged(&c, id, "/tmp/staged.txt", "queued")?;
            db::update_scan_status(&c, id, "clean", "scanned")?;
            id
        };

        // Run cycle
        // This will spawn the upload task, but we only care about the synchronous state update
        // that happens BEFORE the spawn.
        coord.process_cycle().await?;

        // Verify state changed to 'uploading'
        let c = lock_mutex(&conn)?;
        let job = db::get_job(&c, job_id)?.expect("Job not found");

        assert_eq!(job.status, "uploading");
        assert_eq!(job.upload_status, Some("uploading".to_string()));

        Ok(())
    }
}
