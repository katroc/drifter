use crate::core::config::Config;
use crate::db::{self, JobStatus};
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::Serialize;
use std::fs;
use std::path::Path;
use tracing::info;

#[derive(Serialize)]
struct ReportSummary {
    session_id: String,
    generated_at: String,
    total_files: usize,
    clean_files: usize,
    infected_files: usize,
    failed_files: usize,
    skipped_files: usize,
    files: Vec<FileReport>,
}

#[derive(Serialize)]
struct FileReport {
    filename: String,
    status: String,
    scan_status: Option<String>,
    checksum: Option<String>,
    error: Option<String>,
}

pub struct Reporter;

impl Reporter {
    pub fn generate_report(conn: &Connection, config: &Config, session_id: &str) -> Result<()> {
        let jobs = db::get_session_jobs(conn, session_id)?;
        if jobs.is_empty() {
            return Ok(());
        }

        let mut summary = ReportSummary {
            session_id: session_id.to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            total_files: jobs.len(),
            clean_files: 0,
            infected_files: 0,
            failed_files: 0,
            skipped_files: 0,
            files: Vec::with_capacity(jobs.len()),
        };

        for job in jobs {
            let scan_status = job.scan_status.as_deref().unwrap_or("unknown");
            match scan_status {
                "clean" | "scanned" => summary.clean_files += 1,
                "infected" => summary.infected_files += 1,
                _ => {
                    if matches!(
                        JobStatus::parse(&job.status),
                        Some(JobStatus::Failed | JobStatus::Error)
                    ) {
                        summary.failed_files += 1;
                    } else {
                        summary.skipped_files += 1;
                    }
                }
            }

            summary.files.push(FileReport {
                filename: job.source_path.clone(),
                status: job.status.clone(),
                scan_status: job.scan_status.clone(),
                checksum: job.checksum.clone(),
                error: job.error.clone(),
            });
        }

        // Ensure reports directory exists
        let reports_dir = Path::new(&config.reports_dir);
        if !reports_dir.exists() {
            fs::create_dir_all(reports_dir).context("Failed to create reports directory")?;
        }

        // Write JSON report
        let json_filename = format!("scan_report_{}.json", session_id);
        let json_path = reports_dir.join(&json_filename);
        let json_content = serde_json::to_string_pretty(&summary)?;
        fs::write(&json_path, json_content).context("Failed to write JSON report")?;

        info!("Generated scan report: {:?}", json_path);

        // Optional: Text summary for easier reading
        let txt_filename = format!("scan_report_{}.txt", session_id);
        let txt_path = reports_dir.join(&txt_filename);

        let details = summary
            .files
            .iter()
            .map(|f| {
                format!(
                    "[{}] {} - Checksum: {}",
                    f.scan_status.as_deref().unwrap_or("N/A").to_uppercase(),
                    f.filename,
                    f.checksum.as_deref().unwrap_or("N/A")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let txt_content = format!(
            "Scan Report for Session: {}\nGenerated: {}\n----------------------------------------\nTotal Files:    {}\nClean:          {}\nInfected:       {}\nFailed:         {}\nSkipped:        {}\n----------------------------------------\nDetails:\n{}\n",
            summary.session_id,
            summary.generated_at,
            summary.total_files,
            summary.clean_files,
            summary.infected_files,
            summary.failed_files,
            summary.skipped_files,
            details
        );
        fs::write(&txt_path, txt_content).context("Failed to write text report")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn setup_test_db() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
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
            ",
        )?;
        Ok(conn)
    }

    fn create_test_config(temp_dir: &TempDir) -> Config {
        Config {
            reports_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Config::default()
        }
    }

    // --- Report Generation Tests ---

    #[test]
    fn test_generate_report_empty_session() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        // Generate report for session with no jobs
        Reporter::generate_report(&conn, &config, "empty-session")?;

        // No files should be created for empty session
        let json_path = temp_dir.path().join("scan_report_empty-session.json");
        assert!(
            !json_path.exists(),
            "JSON report should not exist for empty session"
        );

        Ok(())
    }

    #[test]
    fn test_generate_report_single_clean_file() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        // Create a job
        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status, checksum)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            ["test-session", "2024-01-01", "complete", "/test/file.txt", "1024", "clean", "abc123"],
        )?;

        Reporter::generate_report(&conn, &config, "test-session")?;

        // Verify JSON report exists
        let json_path = temp_dir.path().join("scan_report_test-session.json");
        assert!(json_path.exists(), "JSON report should exist");

        // Verify TXT report exists
        let txt_path = temp_dir.path().join("scan_report_test-session.txt");
        assert!(txt_path.exists(), "TXT report should exist");

        Ok(())
    }

    #[test]
    fn test_generate_report_json_format() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status, checksum)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            ["json-test", "2024-01-01", "complete", "/test/file.txt", "1024", "clean", "checksum123"],
        )?;

        Reporter::generate_report(&conn, &config, "json-test")?;

        let json_path = temp_dir.path().join("scan_report_json-test.json");
        let json_content = fs::read_to_string(json_path)?;

        // Parse JSON to verify format
        let report: serde_json::Value = serde_json::from_str(&json_content)?;

        assert_eq!(report["session_id"], "json-test");
        assert_eq!(report["total_files"], 1);
        assert_eq!(report["clean_files"], 1);
        assert_eq!(report["infected_files"], 0);
        assert_eq!(report["failed_files"], 0);

        assert!(report["files"].is_array());
        assert_eq!(report["files"].as_array().unwrap().len(), 1);

        let file = &report["files"][0];
        assert_eq!(file["filename"], "/test/file.txt");
        assert_eq!(file["status"], "complete");
        assert_eq!(file["scan_status"], "clean");
        assert_eq!(file["checksum"], "checksum123");

        Ok(())
    }

    #[test]
    fn test_generate_report_counts_clean_files() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        // Create multiple clean files
        for i in 1..=5 {
            conn.execute(
                "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status)
                 VALUES (?, ?, ?, ?, ?, ?)",
                ["clean-test", "2024-01-01", "complete", &format!("/test/file{}.txt", i), "1024", "clean"],
            )?;
        }

        Reporter::generate_report(&conn, &config, "clean-test")?;

        let json_path = temp_dir.path().join("scan_report_clean-test.json");
        let json_content = fs::read_to_string(json_path)?;
        let report: serde_json::Value = serde_json::from_str(&json_content)?;

        assert_eq!(report["total_files"], 5);
        assert_eq!(report["clean_files"], 5);
        assert_eq!(report["infected_files"], 0);

        Ok(())
    }

    #[test]
    fn test_generate_report_counts_infected_files() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        // Create infected files
        for i in 1..=3 {
            conn.execute(
                "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status)
                 VALUES (?, ?, ?, ?, ?, ?)",
                ["infected-test", "2024-01-01", "quarantined", &format!("/test/virus{}.exe", i), "1024", "infected"],
            )?;
        }

        Reporter::generate_report(&conn, &config, "infected-test")?;

        let json_path = temp_dir.path().join("scan_report_infected-test.json");
        let json_content = fs::read_to_string(json_path)?;
        let report: serde_json::Value = serde_json::from_str(&json_content)?;

        assert_eq!(report["total_files"], 3);
        assert_eq!(report["clean_files"], 0);
        assert_eq!(report["infected_files"], 3);

        Ok(())
    }

    #[test]
    fn test_generate_report_counts_failed_files() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        // Create failed files
        for i in 1..=2 {
            conn.execute(
                "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status, error)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                ["failed-test", "2024-01-01", "failed", &format!("/test/failed{}.txt", i), "1024", "unknown", "Network error"],
            )?;
        }

        Reporter::generate_report(&conn, &config, "failed-test")?;

        let json_path = temp_dir.path().join("scan_report_failed-test.json");
        let json_content = fs::read_to_string(json_path)?;
        let report: serde_json::Value = serde_json::from_str(&json_content)?;

        assert_eq!(report["total_files"], 2);
        assert_eq!(report["failed_files"], 2);

        Ok(())
    }

    #[test]
    fn test_generate_report_mixed_statuses() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        // Clean file
        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status)
             VALUES (?, ?, ?, ?, ?, ?)",
            ["mixed-test", "2024-01-01", "complete", "/test/clean.txt", "1024", "clean"],
        )?;

        // Infected file
        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status)
             VALUES (?, ?, ?, ?, ?, ?)",
            ["mixed-test", "2024-01-01", "quarantined", "/test/virus.exe", "2048", "infected"],
        )?;

        // Failed file
        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status, error)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            ["mixed-test", "2024-01-01", "failed", "/test/error.dat", "512", "unknown", "Upload failed"],
        )?;

        Reporter::generate_report(&conn, &config, "mixed-test")?;

        let json_path = temp_dir.path().join("scan_report_mixed-test.json");
        let json_content = fs::read_to_string(json_path)?;
        let report: serde_json::Value = serde_json::from_str(&json_content)?;

        assert_eq!(report["total_files"], 3);
        assert_eq!(report["clean_files"], 1);
        assert_eq!(report["infected_files"], 1);
        assert_eq!(report["failed_files"], 1);

        Ok(())
    }

    #[test]
    fn test_generate_report_txt_format() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status, checksum)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            ["txt-test", "2024-01-01", "complete", "/test/file.txt", "1024", "clean", "abc123"],
        )?;

        Reporter::generate_report(&conn, &config, "txt-test")?;

        let txt_path = temp_dir.path().join("scan_report_txt-test.txt");
        let txt_content = fs::read_to_string(txt_path)?;

        // Verify TXT format contains expected elements
        assert!(txt_content.contains("Scan Report for Session: txt-test"));
        assert!(txt_content.contains("Total Files:    1"));
        assert!(txt_content.contains("Clean:          1"));
        assert!(txt_content.contains("Infected:       0"));
        assert!(txt_content.contains("Failed:         0"));
        assert!(txt_content.contains("/test/file.txt"));
        assert!(txt_content.contains("CLEAN"));
        assert!(txt_content.contains("abc123"));

        Ok(())
    }

    #[test]
    fn test_generate_report_creates_directory() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;

        // Use a non-existent subdirectory
        let reports_dir = temp_dir.path().join("reports").join("nested");
        let config = Config {
            reports_dir: reports_dir.to_string_lossy().to_string(),
            ..Config::default()
        };

        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status)
             VALUES (?, ?, ?, ?, ?, ?)",
            ["dir-test", "2024-01-01", "complete", "/test/file.txt", "1024", "clean"],
        )?;

        // Should create the directory
        Reporter::generate_report(&conn, &config, "dir-test")?;

        assert!(reports_dir.exists(), "Reports directory should be created");

        Ok(())
    }

    #[test]
    fn test_generate_report_includes_error_details() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status, error)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            ["error-test", "2024-01-01", "failed", "/test/file.txt", "1024", "unknown", "Connection timeout"],
        )?;

        Reporter::generate_report(&conn, &config, "error-test")?;

        let json_path = temp_dir.path().join("scan_report_error-test.json");
        let json_content = fs::read_to_string(json_path)?;
        let report: serde_json::Value = serde_json::from_str(&json_content)?;

        let file = &report["files"][0];
        assert_eq!(file["error"], "Connection timeout");

        Ok(())
    }

    #[test]
    fn test_generate_report_scanned_status_counts_as_clean() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        // "scanned" is treated as clean
        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status)
             VALUES (?, ?, ?, ?, ?, ?)",
            ["scanned-test", "2024-01-01", "uploading", "/test/file.txt", "1024", "scanned"],
        )?;

        Reporter::generate_report(&conn, &config, "scanned-test")?;

        let json_path = temp_dir.path().join("scan_report_scanned-test.json");
        let json_content = fs::read_to_string(json_path)?;
        let report: serde_json::Value = serde_json::from_str(&json_content)?;

        assert_eq!(report["clean_files"], 1);
        assert_eq!(report["infected_files"], 0);

        Ok(())
    }

    #[test]
    fn test_generate_report_null_checksum() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status)
             VALUES (?, ?, ?, ?, ?, ?)",
            ["null-test", "2024-01-01", "complete", "/test/file.txt", "1024", "clean"],
        )?;

        Reporter::generate_report(&conn, &config, "null-test")?;

        let json_path = temp_dir.path().join("scan_report_null-test.json");
        let json_content = fs::read_to_string(json_path)?;
        let report: serde_json::Value = serde_json::from_str(&json_content)?;

        let file = &report["files"][0];
        assert!(file["checksum"].is_null());

        Ok(())
    }

    #[test]
    fn test_generate_report_timestamp_format() -> Result<()> {
        let conn = setup_test_db()?;
        let temp_dir = TempDir::new()?;
        let config = create_test_config(&temp_dir);

        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, scan_status)
             VALUES (?, ?, ?, ?, ?, ?)",
            ["timestamp-test", "2024-01-01", "complete", "/test/file.txt", "1024", "clean"],
        )?;

        Reporter::generate_report(&conn, &config, "timestamp-test")?;

        let json_path = temp_dir.path().join("scan_report_timestamp-test.json");
        let json_content = fs::read_to_string(json_path)?;
        let report: serde_json::Value = serde_json::from_str(&json_content)?;

        // Verify timestamp is RFC3339 format
        let timestamp = report["generated_at"].as_str().unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(timestamp).is_ok(),
            "Timestamp should be valid RFC3339 format"
        );

        Ok(())
    }
}
