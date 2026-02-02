use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::Serialize;
use std::fs;
use std::path::Path;
use tracing::info;
use crate::db;
use crate::core::config::Config;

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
                    if job.status == "failed" || job.status == "error" {
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
        
        let details = summary.files.iter().map(|f| format!(
                 "[{}] {} - Checksum: {}", 
                 f.scan_status.as_deref().unwrap_or("N/A").to_uppercase(), 
                 f.filename,
                 f.checksum.as_deref().unwrap_or("N/A")
             )).collect::<Vec<_>>().join("\n");

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
