// Integration tests for drifter
// These tests verify that multiple components work correctly together

use anyhow::Result;
use rusqlite::Connection;
use tempfile::TempDir;

// Import drifter modules
use drifter::coordinator::Coordinator;
use drifter::core::config::{Config, ScanMode};
use drifter::db;
use drifter::services::ingest;
use drifter::services::reporter::Reporter;
use drifter::services::scanner::Scanner;
use drifter::services::uploader::Uploader;

// --- Integration Test Helpers ---

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

fn create_test_config(temp_dir: &TempDir) -> Config {
    Config {
        quarantine_dir: temp_dir
            .path()
            .join("quarantine")
            .to_string_lossy()
            .to_string(),
        reports_dir: temp_dir
            .path()
            .join("reports")
            .to_string_lossy()
            .to_string(),
        state_dir: temp_dir.path().to_string_lossy().to_string(),
        ..Config::default()
    }
}

// --- Database and Config Integration ---

#[test]
fn test_config_roundtrip_with_secrets() -> Result<()> {
    let conn = setup_test_db()?;

    let config = Config {
        s3_bucket: Some("test-bucket".to_string()),
        s3_access_key: Some("AKIATEST".to_string()),
        s3_secret_key: Some("super-secret-key".to_string()),
        part_size_mb: 256,
        ..Config::default()
    };

    // Save config
    drifter::core::config::save_config_to_db(&conn, &config)?;

    // Load config
    let loaded = drifter::core::config::load_config_from_db(&conn)?;

    // Verify secrets are persisted and obfuscated
    assert_eq!(loaded.s3_bucket, config.s3_bucket);
    assert_eq!(loaded.s3_access_key, config.s3_access_key);
    assert_eq!(loaded.s3_secret_key, config.s3_secret_key);
    assert_eq!(loaded.part_size_mb, config.part_size_mb);

    // Verify secret is actually obfuscated in DB
    let raw_secret: String = conn.query_row(
        "SELECT value FROM secrets WHERE key = ?",
        ["s3_secret"],
        |row| row.get(0),
    )?;
    assert_ne!(
        raw_secret, "super-secret-key",
        "Secret should be obfuscated"
    );

    Ok(())
}

#[test]
fn test_job_lifecycle_state_transitions() -> Result<()> {
    let conn = setup_test_db()?;

    // Create job
    let job_id = db::create_job(
        &conn,
        "test-session",
        "/tmp/file.txt",
        1024,
        Some("file.txt"),
    )?;

    // Initial state
    let job = db::get_job(&conn, job_id)?.unwrap();
    assert_eq!(job.status, db::JobStatus::Ingesting);
    assert_eq!(job.retry_count, 0);

    // Stage -> Queued
    db::update_job_staged(&conn, job_id, "/data/file.txt", db::JobStatus::Queued)?;
    let job = db::get_job(&conn, job_id)?.unwrap();
    assert_eq!(job.status, db::JobStatus::Queued);

    // Scan -> Scanned
    db::update_scan_status(&conn, job_id, "clean", db::JobStatus::Scanned)?;
    let job = db::get_job(&conn, job_id)?.unwrap();
    assert_eq!(job.scan_status, Some(db::ScanStatus::Clean));
    assert_eq!(job.status, db::JobStatus::Scanned);

    // Upload -> Complete
    db::update_upload_status(&conn, job_id, "completed", db::JobStatus::Complete)?;
    let job = db::get_job(&conn, job_id)?.unwrap();
    assert_eq!(job.status, db::JobStatus::Complete);

    Ok(())
}

#[test]
fn test_job_retry_workflow() -> Result<()> {
    let conn = setup_test_db()?;

    let job_id = db::create_job(&conn, "retry-session", "/tmp/file.txt", 1024, None)?;

    // Simulate failure
    db::update_job_error(&conn, job_id, db::JobStatus::Failed, "Network timeout")?;

    // Schedule retry with backoff
    let retry_count = 1;
    let backoff_secs = Coordinator::calculate_backoff_seconds(retry_count);
    let next_retry =
        (chrono::Utc::now() + chrono::Duration::seconds(backoff_secs as i64)).to_rfc3339();

    db::update_job_retry_state(
        &conn,
        job_id,
        retry_count,
        Some(&next_retry),
        "retry_pending",
        "Network timeout. Retry pending.",
    )?;

    let job = db::get_job(&conn, job_id)?.unwrap();
    assert_eq!(job.status, db::JobStatus::RetryPending);
    assert_eq!(job.retry_count, 1);
    assert!(job.next_retry_at.is_some());

    // Verify backoff calculation
    assert_eq!(backoff_secs, 10); // Second attempt = 10s

    Ok(())
}

#[test]
fn test_event_logging_integration() -> Result<()> {
    let conn = setup_test_db()?;

    let job_id = db::create_job(&conn, "event-session", "/tmp/file.txt", 1024, None)?;

    // Log multiple events
    db::insert_event(&conn, job_id, "ingest", "File ingested")?;
    db::insert_event(&conn, job_id, "stage", "Ready for scan")?;
    db::insert_event(&conn, job_id, "scan", "Scan started")?;
    db::insert_event(&conn, job_id, "scan", "Scan complete - clean")?;

    // Verify events were recorded
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM events WHERE job_id = ?")?;
    let count: i64 = stmt.query_row([job_id], |row| row.get(0))?;

    assert_eq!(count, 4);

    Ok(())
}

// --- Uploader and Checksum Integration ---

#[test]
fn test_part_size_calculation_with_config() -> Result<()> {
    let config = Config::default();

    // Test with default config (128MB)
    let part_size = Uploader::calculate_part_size(
        1024 * 1024 * 1024, // 1GB file
        config.part_size_mb,
        config.concurrency_upload_parts,
    );

    assert_eq!(part_size, 128 * 1024 * 1024);

    // Test with large file requiring bigger parts
    let large_file = 5_000_000_000_000u64; // 5TB
    let part_size_large = Uploader::calculate_part_size(
        large_file,
        config.part_size_mb,
        config.concurrency_upload_parts,
    );

    // Should enforce 10k parts limit
    let max_parts = large_file / part_size_large as u64;
    assert!(max_parts <= 10000);

    Ok(())
}

#[test]
fn test_checksum_workflow() -> Result<()> {
    let conn = setup_test_db()?;

    let job_id = db::create_job(&conn, "checksum-session", "/tmp/file.txt", 1024, None)?;

    // Simulate upload with checksums
    let data = b"test file content for checksum calculation";
    let local_checksum = Uploader::calculate_checksum(data);

    // Store checksums
    db::update_job_checksums(&conn, job_id, Some(&local_checksum), Some(&local_checksum))?;

    let job = db::get_job(&conn, job_id)?.unwrap();
    assert_eq!(job.checksum, Some(local_checksum.clone()));
    assert_eq!(job.remote_checksum, Some(local_checksum));

    Ok(())
}

#[test]
fn test_composite_checksum_multipart() -> Result<()> {
    // Simulate 3-part upload
    let part1 = b"Part 1 data";
    let part2 = b"Part 2 data";
    let part3 = b"Part 3 data";

    // Calculate individual checksums
    let mut hashes = Vec::new();
    for part in &[part1, part2, part3] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(part);
        hashes.push(hasher.finalize().to_vec());
    }

    // Calculate composite
    let composite = Uploader::calculate_composite_checksum(hashes);

    // Verify format: base64-partcount
    assert!(composite.contains('-'));
    assert!(composite.ends_with("-3"));

    let parts: Vec<&str> = composite.split('-').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[1], "3");

    Ok(())
}

// --- Scanner and Configuration Integration ---

#[test]
fn test_scanner_chunk_calculation_with_config() -> Result<()> {
    let config = Config::default();

    // Test with default config (24MB chunks)
    let file_size = 100 * 1024 * 1024; // 100MB
    let offsets = Scanner::calculate_chunk_offsets(file_size, config.scan_chunk_size_mb);

    // Verify chunks are created
    assert!(!offsets.is_empty());

    // Verify first chunk starts at 0
    assert_eq!(offsets[0], 0);

    // Verify chunks have proper overlap
    if offsets.len() > 1 {
        let chunk_size = Scanner::calculate_effective_chunk_size(config.scan_chunk_size_mb);
        let overlap = 1024 * 1024; // 1MB
        let stride = chunk_size - overlap;

        for i in 1..offsets.len() {
            let expected = offsets[i - 1] + stride as u64;
            assert_eq!(offsets[i], expected);
        }
    }

    Ok(())
}

#[test]
fn test_scan_mode_configuration() -> Result<()> {
    // Test different scan modes
    let scanner_skip = Scanner::new(&Config {
        scan_mode: ScanMode::Skip,
        ..Config::default()
    });
    // Scanner should be created successfully

    let scanner_stream = Scanner::new(&Config {
        scan_mode: ScanMode::Stream,
        ..Config::default()
    });
    // Scanner should be created successfully

    let scanner_full = Scanner::new(&Config {
        scan_mode: ScanMode::Full,
        ..Config::default()
    });
    // Scanner should be created successfully

    // All scanners should be created without errors
    drop(scanner_skip);
    drop(scanner_stream);
    drop(scanner_full);

    Ok(())
}

// --- Ingest and Path Handling Integration ---

#[test]
fn test_path_workflow_end_to_end() -> Result<()> {
    // Simulate full path handling workflow
    let source = std::path::PathBuf::from("/home/user/documents/2024/january/report.pdf");

    // Calculate base path (as ingest does)
    let base = ingest::calculate_base_path(&source);
    assert_eq!(
        base,
        std::path::PathBuf::from("/home/user/documents/2024/january")
    );

    // Calculate relative path for S3 key
    let file = std::path::PathBuf::from("/home/user/documents/2024/january/report.pdf");
    let relative = ingest::calculate_relative_path(&file, &base);
    assert_eq!(relative, "report.pdf");

    Ok(())
}

// --- Reporter Integration ---

#[test]
fn test_report_generation_with_real_data() -> Result<()> {
    let conn = setup_test_db()?;
    let temp_dir = TempDir::new()?;
    let config = create_test_config(&temp_dir);

    let session_id = "integration-test-session";

    // Create a realistic session with multiple jobs
    let job1 = db::create_job(
        &conn,
        session_id,
        "/data/clean_file.txt",
        1024,
        Some("clean_file.txt"),
    )?;
    db::update_scan_status(&conn, job1, "clean", db::JobStatus::Scanned)?;
    db::update_job_checksums(&conn, job1, Some("abc123"), Some("abc123"))?;
    db::update_upload_status(&conn, job1, "completed", db::JobStatus::Complete)?;

    let job2 = db::create_job(
        &conn,
        session_id,
        "/data/virus.exe",
        2048,
        Some("virus.exe"),
    )?;
    db::update_scan_status(&conn, job2, "infected", db::JobStatus::Quarantined)?;

    let job3 = db::create_job(
        &conn,
        session_id,
        "/data/failed.dat",
        512,
        Some("failed.dat"),
    )?;
    db::update_job_error(&conn, job3, db::JobStatus::Failed, "Network timeout")?;

    // Generate report
    Reporter::generate_report(&conn, &config, session_id)?;

    // Verify report files exist
    let json_path =
        std::path::Path::new(&config.reports_dir).join(format!("scan_report_{}.json", session_id));
    let txt_path =
        std::path::Path::new(&config.reports_dir).join(format!("scan_report_{}.txt", session_id));

    assert!(json_path.exists());
    assert!(txt_path.exists());

    // Parse and verify JSON content
    let json_content = std::fs::read_to_string(json_path)?;
    let report: serde_json::Value = serde_json::from_str(&json_content)?;

    assert_eq!(report["total_files"], 3);
    assert_eq!(report["clean_files"], 1);
    assert_eq!(report["infected_files"], 1);
    assert_eq!(report["failed_files"], 1);

    Ok(())
}

// --- Coordinator Integration ---

#[test]
fn test_exponential_backoff_progression() -> Result<()> {
    // Test the full backoff sequence as used by coordinator
    let retries = [0, 1, 2, 3, 4];
    let expected_delays = [5, 10, 20, 40, 80];

    for (retry_count, expected) in retries.iter().zip(expected_delays.iter()) {
        let backoff = Coordinator::calculate_backoff_seconds(*retry_count);
        assert_eq!(backoff, *expected);
    }

    // Calculate total wait time across all retries
    let total: u64 = retries
        .iter()
        .map(|&r| Coordinator::calculate_backoff_seconds(r))
        .sum();

    assert_eq!(total, 155); // 5 + 10 + 20 + 40 + 80

    Ok(())
}

#[test]
fn test_priority_based_job_selection() -> Result<()> {
    let conn = setup_test_db()?;

    // Create jobs with different priorities
    let low = db::create_job(&conn, "session", "/low.txt", 100, None)?;
    db::set_job_priority(&conn, low, 10)?;
    db::update_job_error(&conn, low, db::JobStatus::Queued, "")?;

    let high = db::create_job(&conn, "session", "/high.txt", 100, None)?;
    db::set_job_priority(&conn, high, 100)?;
    db::update_job_error(&conn, high, db::JobStatus::Queued, "")?;

    let medium = db::create_job(&conn, "session", "/medium.txt", 100, None)?;
    db::set_job_priority(&conn, medium, 50)?;
    db::update_job_error(&conn, medium, db::JobStatus::Queued, "")?;

    // Get next job should return highest priority
    let next = db::get_next_job(&conn, db::JobStatus::Queued)?.unwrap();
    assert_eq!(next.id, high);
    assert_eq!(next.priority, 100);

    Ok(())
}

// --- Configuration Validation Integration ---

#[test]
fn test_config_validation_with_invalid_values() -> Result<()> {
    // Test invalid part size
    assert!(
        Config {
            part_size_mb: 3, // Less than 5MB
            ..Config::default()
        }
        .validate()
        .is_err()
    );

    assert!(
        Config {
            part_size_mb: 6000, // More than 5GB
            ..Config::default()
        }
        .validate()
        .is_err()
    );

    // Test valid part size
    assert!(
        Config {
            part_size_mb: 128,
            ..Config::default()
        }
        .validate()
        .is_ok()
    );

    // Test invalid concurrency
    assert!(
        Config {
            concurrency_upload_global: 0,
            ..Config::default()
        }
        .validate()
        .is_err()
    );

    assert!(
        Config {
            concurrency_upload_global: 1,
            ..Config::default()
        }
        .validate()
        .is_ok()
    );

    Ok(())
}

// --- Multi-Component Integration ---

#[test]
fn test_complete_job_workflow_simulation() -> Result<()> {
    let conn = setup_test_db()?;
    let session_id = "complete-workflow";

    // Step 1: Ingest (create job)
    let source_path = "/data/important_file.pdf";
    let file_size = 5 * 1024 * 1024; // 5MB
    let job_id = db::create_job(
        &conn,
        session_id,
        source_path,
        file_size as i64,
        Some("important_file.pdf"),
    )?;
    db::insert_event(&conn, job_id, "ingest", "File ingested")?;

    // Step 2: Queue for scan
    db::update_job_staged(&conn, job_id, source_path, db::JobStatus::Queued)?;
    db::insert_event(&conn, job_id, "stage", "Ready for scan")?;

    // Step 3: Scan
    db::update_scan_status(&conn, job_id, "clean", db::JobStatus::Scanned)?;
    db::insert_event(&conn, job_id, "scan", "Scan complete - clean")?;
    db::update_scan_duration(&conn, job_id, 1500)?; // 1.5 seconds

    // Step 4: Upload with checksum
    let local_checksum = "abcd1234567890";
    db::update_job_checksums(&conn, job_id, Some(local_checksum), Some(local_checksum))?;
    db::update_upload_status(&conn, job_id, "completed", db::JobStatus::Complete)?;
    db::insert_event(&conn, job_id, "upload", "Upload complete")?;
    db::update_upload_duration(&conn, job_id, 5000)?; // 5 seconds

    // Verify final state
    let job = db::get_job(&conn, job_id)?.unwrap();
    assert_eq!(job.status, db::JobStatus::Complete);
    assert_eq!(job.scan_status, Some(db::ScanStatus::Clean));
    assert_eq!(job.checksum, Some(local_checksum.to_string()));
    assert_eq!(job.scan_duration_ms, Some(1500));
    assert_eq!(job.upload_duration_ms, Some(5000));

    // Verify events
    let event_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM events WHERE job_id = ?",
        [job_id],
        |row| row.get(0),
    )?;
    assert_eq!(event_count, 4);

    Ok(())
}
