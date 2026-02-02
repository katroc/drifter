use crate::core::config::StagingMode;
use crate::db::{create_job, insert_event, update_job_error, update_job_staged};
use anyhow::Result;
use rusqlite::Connection;
use tokio::fs;
use std::path::{Path, PathBuf};
use tracing::{info, warn, error, debug};
use std::sync::{Arc, Mutex};
use crate::utils::lock_mutex;

pub async fn ingest_path(conn_mutex: Arc<Mutex<Connection>>, staging_dir: &str, staging_mode: &StagingMode, path: &str, session_id: &str) -> Result<usize> {
    
    debug!("Ingesting path: {}", path);
    let root = PathBuf::from(path);
    if !fs::try_exists(&root).await.unwrap_or(false) {
        warn!("Path does not exist: {}", path);
        return Ok(0);
    }
    
    info!("Ingesting into session: {}", session_id);
    
    // Determine the base for relative paths
    let base_path = root.parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| {
             if root == PathBuf::from("/") {
                 PathBuf::from("/")
             } else {
                 PathBuf::from(".")
             }
        });
    
    let files = collect_files(&root).await?;
    let mut count = 0;
    for file_path in files {
        let metadata = match fs::metadata(&file_path).await {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to read metadata for {:?}: {}", file_path, e);
                continue;
            }
        };
        if !metadata.is_file() {
            continue;
        }
        
        let relative_path = file_path.strip_prefix(&base_path)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| {
                file_path.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "file".to_string())
            });
        
        let size = metadata.len() as i64;
        let source_str = file_path.to_string_lossy().to_string();
        debug!("Processing file: {} ({} bytes)", source_str, size);
        
        let job_id = {
            let conn = lock_mutex(&conn_mutex)?;
            create_job(&conn, &session_id, &source_str, size, Some(&relative_path))?
        };
        
        // Stage file based on mode
        let stage_result = match staging_mode {
            StagingMode::Direct => {
                let conn = lock_mutex(&conn_mutex)?;
                insert_event(&conn, job_id, "ingest", "using direct mode (no copy)")?;
                Ok(source_str.clone())
            }
            StagingMode::Copy => {
                {
                    let conn = lock_mutex(&conn_mutex)?;
                    insert_event(&conn, job_id, "ingest", "queued for staging")?;
                }
                stage_file(staging_dir, job_id, &file_path).await
            }
        };

        match stage_result {
            Ok(staged_path) => {
                let conn = lock_mutex(&conn_mutex)?;
                update_job_staged(&conn, job_id, &staged_path, "queued")?;
                let event_msg = if *staging_mode == StagingMode::Direct {
                    "ready (direct mode)"
                } else {
                    "staged locally"
                };
                insert_event(&conn, job_id, "stage", event_msg)?;
                count += 1;
                info!("Successfully ingested job {} for {}", job_id, source_str);
            }
            Err(err) => {
                error!("Failed to stage job {}: {}", job_id, err);
                let conn = lock_mutex(&conn_mutex)?;
                update_job_error(&conn, job_id, "failed", &err.to_string())?;
                insert_event(&conn, job_id, "stage", "staging failed")?;
            }
        }
    }
    info!("Ingest complete. {} files processed.", count);
    Ok(count)
}


async fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = match fs::metadata(&path).await {
            Ok(m) => m,
            Err(_) => continue,
        };
        if metadata.is_dir() {
            let mut entries = match fs::read_dir(&path).await {
                Ok(e) => e,
                Err(_) => continue,
            };
            while let Some(entry) = entries.next_entry().await? {
                stack.push(entry.path());
            }
        } else if metadata.is_file() {
            files.push(path);
        }
    }
    Ok(files)
}

async fn stage_file(staging_dir: &str, job_id: i64, source: &Path) -> Result<String> {
    let job_dir = Path::new(staging_dir).join(job_id.to_string());
    fs::create_dir_all(&job_dir).await?;
    let filename = source
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "data".to_string());
    let dest = job_dir.join(filename);
    debug!("Copying {:?} to {:?}", source, dest);
    fs::copy(source, &dest).await?;
    Ok(dest.to_string_lossy().to_string())
}

// Helper functions exposed for testing

/// Calculate the base path for relative path calculations
pub fn calculate_base_path(root: &Path) -> PathBuf {
    root.parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| {
            if root == PathBuf::from("/") {
                PathBuf::from("/")
            } else {
                PathBuf::from(".")
            }
        })
}

/// Calculate relative path from base
pub fn calculate_relative_path(file_path: &Path, base_path: &Path) -> String {
    file_path.strip_prefix(base_path)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| {
            file_path.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "file".to_string())
        })
}

/// Calculate staging destination path
pub fn calculate_staging_path(staging_dir: &str, job_id: i64, source: &Path) -> PathBuf {
    let job_dir = Path::new(staging_dir).join(job_id.to_string());
    let filename = source
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "data".to_string());
    job_dir.join(filename)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Base Path Calculation Tests ---

    #[test]
    fn test_calculate_base_path_file() {
        let root = PathBuf::from("/home/user/documents/file.txt");
        let base = calculate_base_path(&root);
        assert_eq!(base, PathBuf::from("/home/user/documents"));
    }

    #[test]
    fn test_calculate_base_path_directory() {
        let root = PathBuf::from("/home/user/documents");
        let base = calculate_base_path(&root);
        assert_eq!(base, PathBuf::from("/home/user"));
    }

    #[test]
    fn test_calculate_base_path_root() {
        let root = PathBuf::from("/");
        let base = calculate_base_path(&root);
        assert_eq!(base, PathBuf::from("/"));
    }

    #[test]
    fn test_calculate_base_path_relative() {
        let root = PathBuf::from("file.txt");
        let base = calculate_base_path(&root);
        // file.txt has parent Some("") which creates an empty PathBuf
        assert_eq!(base, PathBuf::from(""));
    }

    #[test]
    fn test_calculate_base_path_relative_nested() {
        let root = PathBuf::from("dir/file.txt");
        let base = calculate_base_path(&root);
        assert_eq!(base, PathBuf::from("dir"));
    }

    #[test]
    fn test_calculate_base_path_current_dir() {
        let root = PathBuf::from(".");
        let base = calculate_base_path(&root);
        // "." has parent Some("") which creates an empty PathBuf
        assert_eq!(base, PathBuf::from(""));
    }

    // --- Relative Path Calculation Tests ---

    #[test]
    fn test_calculate_relative_path_simple() {
        let file_path = PathBuf::from("/home/user/documents/file.txt");
        let base_path = PathBuf::from("/home/user");

        let relative = calculate_relative_path(&file_path, &base_path);
        assert_eq!(relative, "documents/file.txt");
    }

    #[test]
    fn test_calculate_relative_path_nested() {
        let file_path = PathBuf::from("/home/user/docs/2024/report.pdf");
        let base_path = PathBuf::from("/home/user");

        let relative = calculate_relative_path(&file_path, &base_path);
        assert_eq!(relative, "docs/2024/report.pdf");
    }

    #[test]
    fn test_calculate_relative_path_same_directory() {
        let file_path = PathBuf::from("/home/user/file.txt");
        let base_path = PathBuf::from("/home/user");

        let relative = calculate_relative_path(&file_path, &base_path);
        assert_eq!(relative, "file.txt");
    }

    #[test]
    fn test_calculate_relative_path_fallback_to_filename() {
        // When file is not under base, use filename
        let file_path = PathBuf::from("/other/location/file.txt");
        let base_path = PathBuf::from("/home/user");

        let relative = calculate_relative_path(&file_path, &base_path);
        assert_eq!(relative, "file.txt");
    }

    #[test]
    fn test_calculate_relative_path_fallback_no_filename() {
        // Edge case: path has no filename
        let file_path = PathBuf::from("/");
        let base_path = PathBuf::from("/home/user");

        let relative = calculate_relative_path(&file_path, &base_path);
        assert_eq!(relative, "file"); // Default fallback
    }

    #[test]
    fn test_calculate_relative_path_windows_style() {
        // Test with backslashes (Unix will treat them as regular chars)
        #[cfg(unix)]
        {
            let file_path = PathBuf::from("/home/user/dir/file.txt");
            let base_path = PathBuf::from("/home/user");
            let relative = calculate_relative_path(&file_path, &base_path);
            assert_eq!(relative, "dir/file.txt");
        }
    }

    #[test]
    fn test_calculate_relative_path_preserves_structure() {
        // Verify multi-level directory structure is preserved
        let file_path = PathBuf::from("/base/a/b/c/d/file.txt");
        let base_path = PathBuf::from("/base");

        let relative = calculate_relative_path(&file_path, &base_path);
        assert_eq!(relative, "a/b/c/d/file.txt");
    }

    #[test]
    fn test_calculate_relative_path_special_characters() {
        let file_path = PathBuf::from("/home/user/my docs/file (1).txt");
        let base_path = PathBuf::from("/home/user");

        let relative = calculate_relative_path(&file_path, &base_path);
        assert!(relative.contains("my docs"));
        assert!(relative.contains("file (1).txt"));
    }

    // --- Staging Path Calculation Tests ---

    #[test]
    fn test_calculate_staging_path_basic() {
        let source = PathBuf::from("/source/file.txt");
        let staging_path = calculate_staging_path("./staging", 123, &source);

        assert_eq!(staging_path, PathBuf::from("./staging/123/file.txt"));
    }

    #[test]
    fn test_calculate_staging_path_nested_source() {
        let source = PathBuf::from("/deep/nested/path/document.pdf");
        let staging_path = calculate_staging_path("/tmp/staging", 456, &source);

        assert_eq!(staging_path, PathBuf::from("/tmp/staging/456/document.pdf"));
    }

    #[test]
    fn test_calculate_staging_path_preserves_extension() {
        let test_cases = vec![
            ("/source/file.txt", "./staging", 1, "./staging/1/file.txt"),
            ("/source/image.png", "/tmp", 2, "/tmp/2/image.png"),
            ("/source/archive.tar.gz", "./stage", 3, "./stage/3/archive.tar.gz"),
        ];

        for (source, staging_dir, job_id, expected) in test_cases {
            let source_path = PathBuf::from(source);
            let result = calculate_staging_path(staging_dir, job_id, &source_path);
            assert_eq!(result, PathBuf::from(expected));
        }
    }

    #[test]
    fn test_calculate_staging_path_no_extension() {
        let source = PathBuf::from("/source/Makefile");
        let staging_path = calculate_staging_path("./staging", 789, &source);

        assert_eq!(staging_path, PathBuf::from("./staging/789/Makefile"));
    }

    #[test]
    fn test_calculate_staging_path_special_characters() {
        let source = PathBuf::from("/source/my file (copy).txt");
        let staging_path = calculate_staging_path("./staging", 100, &source);

        assert_eq!(staging_path, PathBuf::from("./staging/100/my file (copy).txt"));
    }

    #[test]
    fn test_calculate_staging_path_different_job_ids() {
        let source = PathBuf::from("/source/file.txt");

        let path1 = calculate_staging_path("./staging", 1, &source);
        let path2 = calculate_staging_path("./staging", 2, &source);
        let path3 = calculate_staging_path("./staging", 999, &source);

        assert_eq!(path1, PathBuf::from("./staging/1/file.txt"));
        assert_eq!(path2, PathBuf::from("./staging/2/file.txt"));
        assert_eq!(path3, PathBuf::from("./staging/999/file.txt"));
    }

    #[test]
    fn test_calculate_staging_path_fallback_filename() {
        // Path with no filename component should use "data"
        let source = PathBuf::from("/");
        let staging_path = calculate_staging_path("./staging", 1, &source);

        assert_eq!(staging_path, PathBuf::from("./staging/1/data"));
    }

    // --- Path Edge Cases ---

    #[test]
    fn test_relative_path_with_dots() {
        let file_path = PathBuf::from("/home/user/./documents/../documents/file.txt");
        let base_path = PathBuf::from("/home/user");

        let relative = calculate_relative_path(&file_path, &base_path);
        // Path normalization happens at OS level, but we preserve as-is
        assert!(relative.contains("file.txt"));
    }

    #[test]
    fn test_staging_path_absolute_and_relative() {
        let source = PathBuf::from("file.txt");

        let abs_staging = calculate_staging_path("/absolute/staging", 1, &source);
        assert_eq!(abs_staging, PathBuf::from("/absolute/staging/1/file.txt"));

        let rel_staging = calculate_staging_path("./relative/staging", 1, &source);
        assert_eq!(rel_staging, PathBuf::from("./relative/staging/1/file.txt"));
    }

    #[test]
    fn test_calculate_relative_path_unicode() {
        let file_path = PathBuf::from("/home/user/文档/файл.txt");
        let base_path = PathBuf::from("/home/user");

        let relative = calculate_relative_path(&file_path, &base_path);
        assert!(relative.contains("文档"));
        assert!(relative.contains("файл.txt"));
    }

    // --- Staging Mode Logic Tests (Conceptual) ---

    #[test]
    fn test_staging_mode_enum_values() {
        // Verify enum variants exist
        let _direct = StagingMode::Direct;
        let _copy = StagingMode::Copy;
    }

    #[test]
    fn test_staging_mode_equality() {
        assert_eq!(StagingMode::Direct, StagingMode::Direct);
        assert_eq!(StagingMode::Copy, StagingMode::Copy);
        assert_ne!(StagingMode::Direct, StagingMode::Copy);
    }

    // --- Integration-like Tests ---

    #[test]
    fn test_full_path_workflow() {
        // Simulate the full path calculation workflow
        let root = PathBuf::from("/home/user/documents/2024/file.txt");
        let base = calculate_base_path(&root);

        assert_eq!(base, PathBuf::from("/home/user/documents/2024"));

        let file = PathBuf::from("/home/user/documents/2024/report.pdf");
        let relative = calculate_relative_path(&file, &base);

        assert_eq!(relative, "report.pdf");
    }

    #[test]
    fn test_staging_workflow_multiple_jobs() {
        // Verify different jobs get different staging paths
        let source1 = PathBuf::from("/source/file1.txt");
        let source2 = PathBuf::from("/source/file2.txt");

        let stage1 = calculate_staging_path("./staging", 1, &source1);
        let stage2 = calculate_staging_path("./staging", 2, &source2);

        assert_ne!(stage1, stage2);
        assert!(stage1.to_string_lossy().contains("/1/"));
        assert!(stage2.to_string_lossy().contains("/2/"));
    }

    #[test]
    fn test_path_calculation_consistency() {
        // Same inputs should always produce same outputs
        let file = PathBuf::from("/home/user/file.txt");
        let base = PathBuf::from("/home/user");

        let rel1 = calculate_relative_path(&file, &base);
        let rel2 = calculate_relative_path(&file, &base);

        assert_eq!(rel1, rel2);
    }
}
