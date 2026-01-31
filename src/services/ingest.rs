use crate::core::config::StagingMode;
use crate::db::{create_job, insert_event, update_job_error, update_job_staged};
use anyhow::Result;
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};

pub fn ingest_path(conn: &Connection, staging_dir: &str, staging_mode: &StagingMode, path: &str) -> Result<usize> {
    let root = PathBuf::from(path);
    if !root.exists() {
        return Ok(0);
    }
    
    // Determine the base for relative paths
    // We always want to be relative to the PARENT of the ingested path,
    // so that the folder name itself is preserved in the S3 key.
    let base_path = root.parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| {
             // If no parent (e.g. root is "/"), use root itself as base (preserving nothing more)
             if root == PathBuf::from("/") {
                 PathBuf::from("/")
             } else {
                 PathBuf::from(".")
             }
        });
    
    let files = collect_files(&root)?;
    let mut count = 0;
    for file_path in files {
        let metadata = match fs::metadata(&file_path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !metadata.is_file() {
            continue;
        }
        
        // Compute relative path from the base
        let relative_path = file_path.strip_prefix(&base_path)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| {
                file_path.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "file".to_string())
            });
        
        let size = metadata.len() as i64;
        let source_str = file_path.to_string_lossy().to_string();
        let job_id = create_job(conn, &source_str, size, Some(&relative_path))?;
        
        // Stage file based on mode
        let stage_result = match staging_mode {
            StagingMode::Direct => {
                // Direct mode: use source path directly, no copy
                insert_event(conn, job_id, "ingest", "using direct mode (no copy)")?;
                Ok(source_str.clone())
            }
            StagingMode::Copy => {
                insert_event(conn, job_id, "ingest", "queued for staging")?;
                stage_file(staging_dir, job_id, &file_path)
            }
        };

        match stage_result {
            Ok(staged_path) => {
                update_job_staged(conn, job_id, &staged_path, "queued")?;
                let event_msg = if *staging_mode == StagingMode::Direct {
                    "ready (direct mode)"
                } else {
                    "staged locally"
                };
                insert_event(conn, job_id, "stage", event_msg)?;
                count += 1;
            }
            Err(err) => {
                update_job_error(conn, job_id, "failed", &err.to_string())?;
                insert_event(conn, job_id, "stage", "staging failed")?;
            }
        }
    }
    Ok(count)
}


fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = match fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if metadata.is_dir() {
            let entries = match fs::read_dir(&path) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                stack.push(entry.path());
            }
        } else if metadata.is_file() {
            files.push(path);
        }
    }
    Ok(files)
}

fn stage_file(staging_dir: &str, job_id: i64, source: &Path) -> Result<String> {
    let job_dir = Path::new(staging_dir).join(job_id.to_string());
    fs::create_dir_all(&job_dir)?;
    let filename = source
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "data".to_string());
    let dest = job_dir.join(filename);
    fs::copy(source, &dest)?;
    Ok(dest.to_string_lossy().to_string())
}
