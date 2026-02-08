use crate::app::state::AppEvent;
use crate::core::config::Config;
use crate::core::transfer::EndpointKind;
use crate::db::{self, JobRow, JobStatus};
use crate::services::scanner::{ScanResult, Scanner};
use crate::services::uploader::{S3EndpointConfig, Uploader};
use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::utils::{lock_async_mutex, lock_mutex};
use std::collections::HashMap;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct ProgressInfo {
    pub percent: f64, // < 0.0 means "Calculating/Indeterminate"
    pub details: String,
    pub parts_done: usize,
    pub parts_total: usize,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub elapsed_secs: u64,
}

#[derive(Clone)]
pub struct Coordinator {
    conn: Arc<Mutex<Connection>>,
    config: Arc<AsyncMutex<Config>>,
    progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>>,
    cancellation_tokens: Arc<AsyncMutex<HashMap<i64, Arc<AtomicBool>>>>,
    app_tx: mpsc::Sender<AppEvent>,
    work_notify: Arc<Notify>,
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
            work_notify: Arc::new(Notify::new()),
        })
    }

    pub async fn run(&self) {
        loop {
            let mut iteration_guard = 0usize;
            loop {
                match self.process_cycle().await {
                    Ok(true) => {
                        iteration_guard += 1;
                        if iteration_guard >= 64 {
                            break;
                        }
                    }
                    Ok(false) => break,
                    Err(e) => {
                        error!("Coordinator error: {}", e);
                        break;
                    }
                }
            }

            let wait_for = self
                .next_wake_delay()
                .await
                .unwrap_or(Duration::from_secs(5));
            tokio::select! {
                _ = self.work_notify.notified() => {}
                _ = tokio::time::sleep(wait_for) => {}
            }
        }
    }

    pub fn notify_work(&self) {
        self.work_notify.notify_one();
    }

    async fn next_wake_delay(&self) -> Result<Duration> {
        let next_retry_at = {
            let conn = lock_mutex(&self.conn)?;
            db::next_retry_due_at(&conn)?
        };

        let max_idle = Duration::from_secs(2);
        let min_delay = Duration::from_millis(200);

        if let Some(next_retry_at) = next_retry_at {
            let now = chrono::Utc::now();
            if next_retry_at <= now {
                return Ok(min_delay);
            }

            let delay = (next_retry_at - now).to_std().unwrap_or(min_delay);
            return Ok(delay.clamp(min_delay, max_idle));
        }

        Ok(max_idle)
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

    pub async fn process_cycle(&self) -> Result<bool> {
        let mut did_work = false;

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
                    did_work = true;
                }
            }
        }

        // 2. Try starting scans
        let (scanner_enabled, scan_limit) = {
            let cfg = lock_async_mutex(&self.config).await;
            (cfg.scanner_enabled, cfg.concurrency_scan_global.max(1))
        };

        if scanner_enabled {
            let active_scans = {
                let conn = lock_mutex(&self.conn)?;
                db::count_jobs_with_job_status(&conn, JobStatus::Scanning)?
            };

            let available_scan_slots = scan_limit.saturating_sub(active_scans as usize);
            for _ in 0..available_scan_slots {
                enum ScanDispatch {
                    Spawn(Box<JobRow>),
                    Skipped,
                    Empty,
                }

                let dispatch = {
                    let conn = lock_mutex(&self.conn)?;
                    if let Some(job) = db::get_next_job_with_status(&conn, JobStatus::Queued)? {
                        let scan_policy = db::get_job_transfer_metadata(&conn, job.id)?
                            .and_then(|m| m.scan_policy);
                        if scan_policy.as_deref() == Some("never") {
                            db::update_scan_status_with_job_status(
                                &conn,
                                job.id,
                                "skipped",
                                JobStatus::Scanned,
                            )?;
                            db::insert_event(
                                &conn,
                                job.id,
                                "scan",
                                "scan skipped by transfer policy",
                            )?;
                            ScanDispatch::Skipped
                        } else {
                            db::update_scan_status_with_job_status(
                                &conn,
                                job.id,
                                "scanning",
                                JobStatus::Scanning,
                            )?;
                            ScanDispatch::Spawn(Box::new(job))
                        }
                    } else {
                        ScanDispatch::Empty
                    }
                };

                match dispatch {
                    ScanDispatch::Spawn(job) => {
                        did_work = true;
                        let coord = self.clone();
                        tokio::spawn(async move {
                            if let Err(e) = coord.process_scan(&job).await {
                                error!("Scan orchestration failed for job {}: {}", job.id, e);
                            }
                        });
                    }
                    ScanDispatch::Skipped => {
                        did_work = true;
                    }
                    ScanDispatch::Empty => break,
                }
            }
        } else {
            // Scanner disabled: mark a bounded batch as scanned so uploads can proceed.
            for _ in 0..32 {
                let skipped_job = {
                    let conn = lock_mutex(&self.conn)?;
                    if let Some(job) = db::get_next_job_with_status(&conn, JobStatus::Queued)? {
                        db::update_scan_status_with_job_status(
                            &conn,
                            job.id,
                            "skipped",
                            JobStatus::Scanned,
                        )?;
                        db::insert_event(&conn, job.id, "scan", "scan skipped by policy")?;
                        Some(job)
                    } else {
                        None
                    }
                };

                if skipped_job.is_some() {
                    did_work = true;
                } else {
                    break;
                }
            }
        }

        // 3. Try starting uploads
        let (max_uploads, active_uploads) = {
            let cfg = lock_async_mutex(&self.config).await;
            let conn = lock_mutex(&self.conn)?;
            (
                cfg.concurrency_upload_global,
                db::count_jobs_with_job_status(&conn, JobStatus::Uploading)?
                    + db::count_jobs_with_job_status(&conn, JobStatus::Transferring)?,
            )
        };

        let available_upload_slots = max_uploads.saturating_sub(active_uploads as usize);
        for _ in 0..available_upload_slots {
            let scanned_job = {
                let conn = lock_mutex(&self.conn)?;
                if let Some(job) = db::get_next_job_with_status(&conn, JobStatus::Scanned)? {
                    let transfer_direction = match db::get_job_transfer_metadata(&conn, job.id) {
                        Ok(metadata) => metadata.and_then(|value| value.transfer_direction),
                        Err(e)
                            if e.to_string().contains("no such column")
                                || e.to_string().contains("no such table") =>
                        {
                            None
                        }
                        Err(e) => return Err(e),
                    };
                    let is_s3_to_s3 = transfer_direction.as_deref() == Some("s3_to_s3");
                    let initial_status = if is_s3_to_s3 {
                        JobStatus::Transferring
                    } else {
                        JobStatus::Uploading
                    };
                    let initial_phase = if is_s3_to_s3 { "copying" } else { "uploading" };
                    db::update_upload_status_with_job_status(
                        &conn,
                        job.id,
                        initial_phase,
                        initial_status,
                    )?;
                    Some(job)
                } else {
                    None
                }
            };

            if let Some(job) = scanned_job {
                did_work = true;
                let coord = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = coord.process_transfer(&job).await {
                        error!("Transfer orchestration failed for job {}: {}", job.id, e);
                    }
                });
            } else {
                break;
            }
        }

        Ok(did_work)
    }

    async fn process_scan(&self, job: &JobRow) -> Result<()> {
        let path = match &job.staged_path {
            Some(p) => p,
            None => {
                let conn = lock_mutex(&self.conn)?;
                db::update_job_error_with_status(
                    &conn,
                    job.id,
                    JobStatus::Failed,
                    "no staged path",
                )?;
                self.notify_work();
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
                db::update_scan_status_with_job_status(&conn, job.id, "clean", JobStatus::Scanned)?;
                db::update_scan_duration(&conn, job.id, duration)?;
                db::insert_event(
                    &conn,
                    job.id,
                    "scan",
                    &format!("scan completed in {}ms", duration),
                )?;
            }
            Ok(ScanResult::Infected(virus_name)) => {
                let quarantine_dir = {
                    let cfg = lock_async_mutex(&self.config).await;
                    PathBuf::from(&cfg.quarantine_dir)
                };
                let quarantine_result = Self::quarantine_file_to_dir(
                    std::path::Path::new(path),
                    &quarantine_dir,
                    job.id,
                );

                {
                    let conn = lock_mutex(&self.conn)?;
                    match quarantine_result {
                        Ok(quarantine_path) => {
                            db::update_scan_status_with_job_status(
                                &conn,
                                job.id,
                                "infected",
                                JobStatus::Quarantined,
                            )?;
                            db::update_job_staged_path(
                                &conn,
                                job.id,
                                &quarantine_path.to_string_lossy(),
                            )?;
                            // Keep virus details visible in UI details panel.
                            db::update_job_error_with_status(
                                &conn,
                                job.id,
                                JobStatus::Quarantined,
                                &format!("Infected: {}", virus_name),
                            )?;
                            db::insert_event(
                                &conn,
                                job.id,
                                "scan",
                                &format!(
                                    "scan failed: infected with {} (moved to {})",
                                    virus_name,
                                    quarantine_path.to_string_lossy()
                                ),
                            )?;
                        }
                        Err(e) => {
                            error!(
                                "Failed to quarantine infected file for job {}: {}",
                                job.id, e
                            );
                            db::update_scan_status_with_job_status(
                                &conn,
                                job.id,
                                "infected",
                                JobStatus::Failed,
                            )?;
                            db::update_job_error_with_status(
                                &conn,
                                job.id,
                                JobStatus::Failed,
                                &format!("Infected: {} (quarantine failed: {})", virus_name, e),
                            )?;
                            db::insert_event(
                                &conn,
                                job.id,
                                "scan",
                                &format!(
                                    "scan failed: infected with {} (quarantine failed: {})",
                                    virus_name, e
                                ),
                            )?;
                        }
                    }
                }
                self.check_and_report(&job.session_id).await?;
            }
            Err(e) => {
                {
                    let conn = lock_mutex(&self.conn)?;
                    db::update_job_error_with_status(
                        &conn,
                        job.id,
                        JobStatus::Failed,
                        &format!("scan error: {}", e),
                    )?;
                }
                self.check_and_report(&job.session_id).await?;
            }
        }
        self.notify_work();
        Ok(())
    }

    async fn process_transfer(&self, job: &JobRow) -> Result<()> {
        let transfer_metadata = {
            let conn = lock_mutex(&self.conn)?;
            db::get_job_transfer_metadata(&conn, job.id)?
        };
        let transfer_direction = transfer_metadata
            .as_ref()
            .and_then(|metadata| metadata.transfer_direction.as_deref())
            .unwrap_or("local_to_s3");

        match transfer_direction {
            "local_to_s3" => self.process_upload(job, transfer_metadata.as_ref()).await,
            "s3_to_local" => {
                self.process_s3_to_local(job, transfer_metadata.as_ref())
                    .await
            }
            "s3_to_s3" => self.process_s3_to_s3(job, transfer_metadata.as_ref()).await,
            other => {
                {
                    let conn = lock_mutex(&self.conn)?;
                    db::update_job_error_with_status(
                        &conn,
                        job.id,
                        JobStatus::Failed,
                        &format!("unknown transfer direction '{}'", other),
                    )?;
                    db::insert_event(
                        &conn,
                        job.id,
                        "transfer",
                        &format!("unknown transfer direction '{}'", other),
                    )?;
                }
                self.check_and_report(&job.session_id).await?;
                self.notify_work();
                Ok(())
            }
        }
    }

    fn endpoint_config_string(config: &serde_json::Value, key: &str) -> Option<String> {
        config
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(std::string::ToString::to_string)
    }

    fn endpoint_profile_to_s3_config(
        conn: &Connection,
        profile: &db::EndpointProfileRow,
    ) -> Result<S3EndpointConfig> {
        if profile.kind != EndpointKind::S3 {
            anyhow::bail!("endpoint '{}' is not an S3 endpoint", profile.name);
        }

        let secret_key = if let Some(secret_ref) = profile.credential_ref.as_deref() {
            db::get_secret(conn, secret_ref)?
        } else {
            None
        };

        Ok(S3EndpointConfig {
            bucket: Self::endpoint_config_string(&profile.config, "bucket").unwrap_or_default(),
            prefix: Self::endpoint_config_string(&profile.config, "prefix"),
            region: Self::endpoint_config_string(&profile.config, "region"),
            endpoint: Self::endpoint_config_string(&profile.config, "endpoint"),
            access_key: Self::endpoint_config_string(&profile.config, "access_key"),
            secret_key,
        })
    }

    fn resolve_s3_to_s3_endpoints(
        conn: &Connection,
        metadata: Option<&db::JobTransferMetadata>,
    ) -> Result<(S3EndpointConfig, S3EndpointConfig)> {
        let source_profile = if let Some(id) = metadata.and_then(|m| m.source_endpoint_id) {
            db::get_endpoint_profile(conn, id)?
                .with_context(|| format!("missing source endpoint profile id={id}"))?
        } else {
            db::get_default_source_endpoint_profile(conn)?
                .context("missing default source endpoint profile for s3_to_s3 transfer")?
        };

        let destination_profile = if let Some(id) = metadata.and_then(|m| m.destination_endpoint_id)
        {
            db::get_endpoint_profile(conn, id)?
                .with_context(|| format!("missing destination endpoint profile id={id}"))?
        } else {
            db::get_default_destination_endpoint_profile(conn)?
                .context("missing default destination endpoint profile for s3_to_s3 transfer")?
        };

        let source = Self::endpoint_profile_to_s3_config(conn, &source_profile)?;
        let destination = Self::endpoint_profile_to_s3_config(conn, &destination_profile)?;
        Ok((source, destination))
    }

    fn resolve_upload_destination_endpoint(
        conn: &Connection,
        metadata: Option<&db::JobTransferMetadata>,
    ) -> Result<S3EndpointConfig> {
        let destination_profile = if let Some(id) = metadata.and_then(|m| m.destination_endpoint_id)
        {
            db::get_endpoint_profile(conn, id)?
                .with_context(|| format!("missing destination endpoint profile id={id}"))?
        } else {
            db::get_default_destination_endpoint_profile(conn)?
                .context("missing default destination endpoint profile for local_to_s3 transfer")?
        };
        Self::endpoint_profile_to_s3_config(conn, &destination_profile)
    }

    fn apply_s3_endpoint_to_config(cfg: &mut Config, endpoint: S3EndpointConfig) {
        cfg.s3_bucket = Some(endpoint.bucket);
        cfg.s3_prefix = endpoint.prefix;
        cfg.s3_region = endpoint.region;
        cfg.s3_endpoint = endpoint.endpoint;
        cfg.s3_access_key = endpoint.access_key;
        cfg.s3_secret_key = endpoint.secret_key;
    }

    async fn process_s3_to_s3(
        &self,
        job: &JobRow,
        metadata: Option<&db::JobTransferMetadata>,
    ) -> Result<()> {
        let endpoint_resolution = {
            let conn = lock_mutex(&self.conn)?;
            Self::resolve_s3_to_s3_endpoints(&conn, metadata)
        };
        let (source_endpoint, destination_endpoint) = match endpoint_resolution {
            Ok(v) => v,
            Err(e) => {
                {
                    let conn = lock_mutex(&self.conn)?;
                    db::update_job_error_with_status(
                        &conn,
                        job.id,
                        JobStatus::Failed,
                        &format!("unable to resolve s3 endpoints: {}", e),
                    )?;
                    db::insert_event(
                        &conn,
                        job.id,
                        "transfer",
                        &format!("unable to resolve s3 endpoints: {}", e),
                    )?;
                }
                self.check_and_report(&job.session_id).await?;
                self.notify_work();
                return Ok(());
            }
        };

        if !Uploader::supports_server_side_copy(&source_endpoint, &destination_endpoint) {
            {
                let conn = lock_mutex(&self.conn)?;
                db::update_job_error_with_status(
                    &conn,
                    job.id,
                    JobStatus::Failed,
                    "s3_to_s3 requires matching source/destination endpoint URLs for server-side copy",
                )?;
                db::insert_event(
                    &conn,
                    job.id,
                    "transfer",
                    "server-side copy requires matching source/destination endpoint URLs",
                )?;
            }
            self.check_and_report(&job.session_id).await?;
            self.notify_work();
            return Ok(());
        }

        let source_region = source_endpoint
            .region
            .as_deref()
            .unwrap_or("us-east-1")
            .trim()
            .to_ascii_lowercase();
        let destination_region = destination_endpoint
            .region
            .as_deref()
            .unwrap_or("us-east-1")
            .trim()
            .to_ascii_lowercase();
        if source_region != destination_region {
            {
                let conn = lock_mutex(&self.conn)?;
                db::update_job_error_with_status(
                    &conn,
                    job.id,
                    JobStatus::Failed,
                    &format!(
                        "s3_to_s3 requires same region in v1 (source={}, destination={})",
                        source_region, destination_region
                    ),
                )?;
                db::insert_event(
                    &conn,
                    job.id,
                    "transfer",
                    &format!(
                        "same-region validation failed (source={}, destination={})",
                        source_region, destination_region
                    ),
                )?;
            }
            self.check_and_report(&job.session_id).await?;
            self.notify_work();
            return Ok(());
        }

        let source_key = job
            .s3_key
            .clone()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| job.source_path.clone());
        let destination_key = job
            .staged_path
            .clone()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| source_key.clone());

        let conflict_policy = metadata
            .and_then(|m| m.conflict_policy.as_deref())
            .unwrap_or("overwrite");

        let destination_exists =
            match Uploader::object_exists(&destination_endpoint, &destination_key).await {
                Ok(v) => v,
                Err(e) => {
                    let max_retries = 5;
                    let should_report = {
                        let conn = lock_mutex(&self.conn)?;
                        if job.retry_count < max_retries {
                            let backoff_secs = Self::calculate_backoff_seconds(job.retry_count);
                            let next_retry =
                                chrono::Utc::now() + chrono::Duration::seconds(backoff_secs as i64);
                            let next_retry_str = next_retry.to_rfc3339();
                            error!(
                                "S3->S3 preflight failed for job {}: {}. Retrying in {}s...",
                                job.id, e, backoff_secs
                            );

                            db::update_job_retry_state(
                                &conn,
                                job.id,
                                job.retry_count,
                                Some(&next_retry_str),
                                "retry_pending",
                                &format!("Transfer preflight failed: {}. Retry pending.", e),
                            )?;
                            db::insert_event(
                                &conn,
                                job.id,
                                "retry_scheduled",
                                &format!("Scheduled transfer retry in {}s", backoff_secs),
                            )?;
                            false
                        } else {
                            db::update_job_error_with_status(
                                &conn,
                                job.id,
                                JobStatus::Failed,
                                &format!("Max retries exceeded. Transfer preflight error: {}", e),
                            )?;
                            true
                        }
                    };
                    if should_report {
                        self.check_and_report(&job.session_id).await?;
                    }
                    self.notify_work();
                    return Ok(());
                }
            };

        if destination_exists {
            match conflict_policy {
                "skip" => {
                    {
                        let conn = lock_mutex(&self.conn)?;
                        db::update_upload_status_with_job_status(
                            &conn,
                            job.id,
                            "skipped",
                            JobStatus::Complete,
                        )?;
                        db::insert_event(
                            &conn,
                            job.id,
                            "transfer",
                            &format!(
                                "s3 copy skipped by conflict policy (destination key exists: {})",
                                destination_key
                            ),
                        )?;
                    }
                    self.check_and_report(&job.session_id).await?;
                    self.notify_work();
                    return Ok(());
                }
                "prompt" => {
                    {
                        let conn = lock_mutex(&self.conn)?;
                        db::update_job_error_with_status(
                            &conn,
                            job.id,
                            JobStatus::Failed,
                            "conflict_policy=prompt is not supported for background transfers",
                        )?;
                        db::insert_event(
                            &conn,
                            job.id,
                            "transfer",
                            "conflict_policy=prompt is not supported for background transfers",
                        )?;
                    }
                    self.check_and_report(&job.session_id).await?;
                    self.notify_work();
                    return Ok(());
                }
                _ => {}
            }
        }

        {
            let conn = lock_mutex(&self.conn)?;
            db::update_upload_status_with_job_status(
                &conn,
                job.id,
                "copying",
                JobStatus::Transferring,
            )?;
            db::insert_event(
                &conn,
                job.id,
                "transfer",
                &format!(
                    "copying s3://{}/{} -> s3://{}/{}",
                    source_endpoint.bucket,
                    source_key,
                    destination_endpoint.bucket,
                    destination_key
                ),
            )?;
        }

        let part_size_mb = {
            let cfg = lock_async_mutex(&self.config).await;
            cfg.part_size_mb
        };

        let start_time = std::time::Instant::now();
        let result = Uploader::copy_between_endpoints(
            &source_endpoint,
            &destination_endpoint,
            &source_key,
            &destination_key,
            part_size_mb,
        )
        .await;

        match result {
            Ok(()) => {
                let duration = start_time.elapsed().as_millis() as i64;
                {
                    let conn = lock_mutex(&self.conn)?;
                    db::update_upload_status_with_job_status(
                        &conn,
                        job.id,
                        "completed",
                        JobStatus::Complete,
                    )?;
                    db::update_upload_duration(&conn, job.id, duration)?;
                    db::insert_event(
                        &conn,
                        job.id,
                        "transfer",
                        &format!("s3 copy completed in {}ms", duration),
                    )?;
                }

                if let Err(e) = self.app_tx.send(AppEvent::RefreshRemote) {
                    warn!("Failed to send RefreshRemote event: {}", e);
                }

                self.check_and_report(&job.session_id).await?;
            }
            Err(e) => {
                let max_retries = 5;
                let should_report = {
                    let conn = lock_mutex(&self.conn)?;
                    if job.retry_count < max_retries {
                        let backoff_secs = Self::calculate_backoff_seconds(job.retry_count);
                        let next_retry =
                            chrono::Utc::now() + chrono::Duration::seconds(backoff_secs as i64);
                        let next_retry_str = next_retry.to_rfc3339();
                        error!(
                            "S3->S3 transfer failed for job {}: {}. Retrying in {}s...",
                            job.id, e, backoff_secs
                        );

                        db::update_job_retry_state(
                            &conn,
                            job.id,
                            job.retry_count,
                            Some(&next_retry_str),
                            "retry_pending",
                            &format!("Transfer failed: {}. Retry pending.", e),
                        )?;

                        db::insert_event(
                            &conn,
                            job.id,
                            "retry_scheduled",
                            &format!("Scheduled transfer retry in {}s", backoff_secs),
                        )?;
                        false
                    } else {
                        db::update_job_error_with_status(
                            &conn,
                            job.id,
                            JobStatus::Failed,
                            &format!("Max retries exceeded. Transfer error: {}", e),
                        )?;
                        true
                    }
                };

                if should_report {
                    self.check_and_report(&job.session_id).await?;
                }
            }
        }

        self.notify_work();
        Ok(())
    }

    async fn process_s3_to_local(
        &self,
        job: &JobRow,
        metadata: Option<&db::JobTransferMetadata>,
    ) -> Result<()> {
        let key = job
            .s3_key
            .clone()
            .unwrap_or_else(|| job.source_path.clone());
        let destination = if let Some(staged) = &job.staged_path {
            PathBuf::from(staged)
        } else {
            let fallback_name = std::path::Path::new(&key)
                .file_name()
                .unwrap_or(std::ffi::OsStr::new("downloaded_file"));
            let download_dir = dirs::download_dir().unwrap_or_else(|| PathBuf::from("."));
            download_dir.join(fallback_name)
        };

        let conflict_policy = metadata
            .and_then(|m| m.conflict_policy.as_deref())
            .unwrap_or("overwrite");

        if destination.exists() {
            match conflict_policy {
                "skip" => {
                    {
                        let conn = lock_mutex(&self.conn)?;
                        db::update_upload_status_with_job_status(
                            &conn,
                            job.id,
                            "skipped",
                            JobStatus::Complete,
                        )?;
                        db::insert_event(
                            &conn,
                            job.id,
                            "transfer",
                            &format!(
                                "download skipped by conflict policy (destination exists: {})",
                                destination.to_string_lossy()
                            ),
                        )?;
                    }
                    self.check_and_report(&job.session_id).await?;
                    self.notify_work();
                    return Ok(());
                }
                "prompt" => {
                    {
                        let conn = lock_mutex(&self.conn)?;
                        db::update_job_error_with_status(
                            &conn,
                            job.id,
                            JobStatus::Failed,
                            "conflict_policy=prompt is not supported for background transfers",
                        )?;
                        db::insert_event(
                            &conn,
                            job.id,
                            "transfer",
                            "conflict_policy=prompt is not supported for background transfers",
                        )?;
                    }
                    self.check_and_report(&job.session_id).await?;
                    self.notify_work();
                    return Ok(());
                }
                _ => {}
            }
        }

        let config = {
            let config_guard = lock_async_mutex(&self.config).await;
            config_guard.clone()
        };

        if let Some(parent) = destination.parent()
            && let Err(e) = tokio::fs::create_dir_all(parent).await
        {
            {
                let conn = lock_mutex(&self.conn)?;
                db::update_job_error_with_status(
                    &conn,
                    job.id,
                    JobStatus::Failed,
                    &format!(
                        "failed to create local destination directory '{}': {}",
                        parent.to_string_lossy(),
                        e
                    ),
                )?;
                db::insert_event(
                    &conn,
                    job.id,
                    "transfer",
                    &format!(
                        "failed to create local destination directory '{}': {}",
                        parent.to_string_lossy(),
                        e
                    ),
                )?;
            }
            self.check_and_report(&job.session_id).await?;
            self.notify_work();
            return Ok(());
        }

        {
            let conn = lock_mutex(&self.conn)?;
            db::update_upload_status_with_job_status(
                &conn,
                job.id,
                "downloading",
                JobStatus::Uploading,
            )?;
            db::insert_event(
                &conn,
                job.id,
                "transfer",
                &format!(
                    "downloading s3://{key} -> {}",
                    destination.to_string_lossy()
                ),
            )?;
        }

        let start_time = std::time::Instant::now();
        let result = Uploader::download_file(&config, &key, &destination).await;
        match result {
            Ok(()) => {
                let duration = start_time.elapsed().as_millis() as i64;
                {
                    let conn = lock_mutex(&self.conn)?;
                    db::update_upload_status_with_job_status(
                        &conn,
                        job.id,
                        "completed",
                        JobStatus::Complete,
                    )?;
                    db::update_upload_duration(&conn, job.id, duration)?;
                    db::insert_event(
                        &conn,
                        job.id,
                        "transfer",
                        &format!(
                            "download completed in {}ms -> {}",
                            duration,
                            destination.to_string_lossy()
                        ),
                    )?;
                }
                self.check_and_report(&job.session_id).await?;
            }
            Err(e) => {
                let max_retries = 5;
                let should_report = {
                    let conn = lock_mutex(&self.conn)?;
                    if job.retry_count < max_retries {
                        let backoff_secs = Self::calculate_backoff_seconds(job.retry_count);
                        let next_retry =
                            chrono::Utc::now() + chrono::Duration::seconds(backoff_secs as i64);
                        let next_retry_str = next_retry.to_rfc3339();

                        error!(
                            "S3->local transfer failed for job {}: {}. Retrying in {}s...",
                            job.id, e, backoff_secs
                        );

                        db::update_job_retry_state(
                            &conn,
                            job.id,
                            job.retry_count,
                            Some(&next_retry_str),
                            "retry_pending",
                            &format!("Transfer failed: {}. Retry pending.", e),
                        )?;

                        db::insert_event(
                            &conn,
                            job.id,
                            "retry_scheduled",
                            &format!("Scheduled transfer retry in {}s", backoff_secs),
                        )?;
                        false
                    } else {
                        db::update_job_error_with_status(
                            &conn,
                            job.id,
                            JobStatus::Failed,
                            &format!("Max retries exceeded. Transfer error: {}", e),
                        )?;
                        true
                    }
                };

                if should_report {
                    self.check_and_report(&job.session_id).await?;
                }
            }
        }

        self.notify_work();
        Ok(())
    }

    async fn process_upload(
        &self,
        job: &JobRow,
        metadata: Option<&db::JobTransferMetadata>,
    ) -> Result<()> {
        let path = match &job.staged_path {
            Some(p) => p.clone(),
            None => return Ok(()),
        };

        let mut config = {
            let config_guard = lock_async_mutex(&self.config).await;
            config_guard.clone()
        };
        let endpoint_resolution = {
            let conn = lock_mutex(&self.conn)?;
            Self::resolve_upload_destination_endpoint(&conn, metadata)
        };
        match endpoint_resolution {
            Ok(endpoint) => {
                Self::apply_s3_endpoint_to_config(&mut config, endpoint);
            }
            Err(e) => {
                {
                    let conn = lock_mutex(&self.conn)?;
                    db::update_job_error_with_status(
                        &conn,
                        job.id,
                        JobStatus::Failed,
                        &format!("unable to resolve upload destination endpoint: {}", e),
                    )?;
                    db::insert_event(
                        &conn,
                        job.id,
                        "transfer",
                        &format!("unable to resolve upload destination endpoint: {}", e),
                    )?;
                }
                self.check_and_report(&job.session_id).await?;
                self.notify_work();
                return Ok(());
            }
        }
        let uploader = Uploader::new(&config);

        // Set status to "uploading" BEFORE starting upload
        {
            let conn = lock_mutex(&self.conn)?;
            db::update_upload_status_with_job_status(
                &conn,
                job.id,
                "starting",
                JobStatus::Uploading,
            )?;
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
                    db::update_upload_status_with_job_status(
                        &conn,
                        job.id,
                        "completed",
                        JobStatus::Complete,
                    )?;
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

                    if JobStatus::parse(&current_status) == Some(JobStatus::Paused) {
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
                        db::update_job_error_with_status(
                            &conn,
                            job.id,
                            JobStatus::Failed,
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

        self.notify_work();
        Ok(())
    }

    fn quarantine_destination_path(
        quarantine_dir: &std::path::Path,
        source_path: &std::path::Path,
        job_id: i64,
    ) -> PathBuf {
        let base_name = source_path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("unknown.bin");

        for attempt in 0..10_000usize {
            let candidate = if attempt == 0 {
                format!("job_{}_{}", job_id, base_name)
            } else {
                format!("job_{}_{}_{}", job_id, attempt, base_name)
            };
            let candidate_path = quarantine_dir.join(candidate);
            if !candidate_path.exists() {
                return candidate_path;
            }
        }

        let fallback = format!(
            "job_{}_{}_{}",
            job_id,
            chrono::Utc::now().timestamp(),
            base_name
        );
        quarantine_dir.join(fallback)
    }

    fn quarantine_file_to_dir(
        source_path: &std::path::Path,
        quarantine_dir: &std::path::Path,
        job_id: i64,
    ) -> Result<PathBuf> {
        if !source_path.exists() {
            anyhow::bail!(
                "source file does not exist: {}",
                source_path.to_string_lossy()
            );
        }

        std::fs::create_dir_all(quarantine_dir).with_context(|| {
            format!(
                "failed to create quarantine directory '{}'",
                quarantine_dir.to_string_lossy()
            )
        })?;

        let destination_path =
            Self::quarantine_destination_path(quarantine_dir, source_path, job_id);
        match std::fs::rename(source_path, &destination_path) {
            Ok(()) => Ok(destination_path),
            Err(rename_err) => {
                warn!(
                    "Rename into quarantine failed for '{}': {}. Trying copy+remove fallback.",
                    source_path.to_string_lossy(),
                    rename_err
                );

                std::fs::copy(source_path, &destination_path).with_context(|| {
                    format!(
                        "failed to copy '{}' to '{}'",
                        source_path.to_string_lossy(),
                        destination_path.to_string_lossy()
                    )
                })?;

                {
                    let file = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&destination_path)
                        .with_context(|| {
                            format!(
                                "failed to open copied quarantine file '{}'",
                                destination_path.to_string_lossy()
                            )
                        })?;
                    file.sync_all().with_context(|| {
                        format!(
                            "failed to fsync copied quarantine file '{}'",
                            destination_path.to_string_lossy()
                        )
                    })?;
                }

                if let Err(remove_err) = std::fs::remove_file(source_path) {
                    let _ = std::fs::remove_file(&destination_path);
                    return Err(anyhow::anyhow!(
                        "failed to remove original '{}' after copy fallback: {}",
                        source_path.to_string_lossy(),
                        remove_err
                    ));
                }

                Ok(destination_path)
            }
        }
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
        let _ = coord.process_cycle().await?;

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
            db::update_scan_status_with_job_status(&c, id, "clean", JobStatus::Scanned)?;
            id
        };

        // Run cycle
        // This will spawn the upload task, but we only care about the synchronous state update
        // that happens BEFORE the spawn.
        let _ = coord.process_cycle().await?;

        // Verify state changed to 'uploading'
        let c = lock_mutex(&conn)?;
        let job = db::get_job(&c, job_id)?.expect("Job not found");

        assert_eq!(job.status, "uploading");
        assert_eq!(job.upload_status, Some("uploading".to_string()));

        Ok(())
    }
}
