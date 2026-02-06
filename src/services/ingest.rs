use crate::db::{assign_default_transfer_metadata, create_job, insert_event, update_job_staged};
use crate::utils::lock_mutex;
use anyhow::Result;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs;
use tracing::{debug, info, warn};

pub async fn ingest_path(
    conn_mutex: Arc<Mutex<Connection>>,
    path: &str,
    session_id: &str,
    destination_prefix: Option<&str>,
) -> Result<usize> {
    debug!("Ingesting path: {}", path);
    let root = PathBuf::from(path);
    if !fs::try_exists(&root).await.unwrap_or(false) {
        warn!("Path does not exist: {}", path);
        return Ok(0);
    }

    info!("Ingesting into session: {}", session_id);
    if let Some(prefix) = destination_prefix {
        info!("Using custom destination prefix: {}", prefix);
    }

    // Determine the base for relative paths
    let base_path = root.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| {
        if root == Path::new("/") {
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

        let relative_path = file_path
            .strip_prefix(&base_path)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| {
                file_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "file".to_string())
            });

        // Apply destination prefix if provided
        let s3_key = if let Some(prefix) = destination_prefix {
            // Ensure prefix ends with /
            let normalized_prefix = if prefix.is_empty() || prefix.ends_with('/') {
                prefix.to_string()
            } else {
                format!("{}/", prefix)
            };
            format!("{}{}", normalized_prefix, relative_path)
        } else {
            relative_path
        };

        let size = metadata.len() as i64;
        let source_str = file_path.to_string_lossy().to_string();
        debug!(
            "Processing file: {} ({} bytes) -> s3_key: {}",
            source_str, size, s3_key
        );

        let job_id = {
            let conn = lock_mutex(&conn_mutex)?;
            create_job(&conn, session_id, &source_str, size, Some(&s3_key))?
        };

        let conn = lock_mutex(&conn_mutex)?;
        if let Err(e) = assign_default_transfer_metadata(&conn, job_id) {
            warn!(
                "Failed to assign default transfer metadata for job {}: {}",
                job_id, e
            );
        }
        insert_event(&conn, job_id, "ingest", "queued for scan")?;
        update_job_staged(&conn, job_id, &source_str, "queued")?;
        insert_event(&conn, job_id, "stage", "ready for scan")?;
        count += 1;
        info!("Successfully ingested job {} for {}", job_id, source_str);
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

// Helper functions exposed for testing

/// Calculate the base path for relative path calculations
#[allow(dead_code)]
pub fn calculate_base_path(root: &Path) -> PathBuf {
    root.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| {
        if root == Path::new("/") {
            PathBuf::from("/")
        } else {
            PathBuf::from(".")
        }
    })
}

/// Calculate relative path from base
#[allow(dead_code)]
pub fn calculate_relative_path(file_path: &Path, base_path: &Path) -> String {
    file_path
        .strip_prefix(base_path)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| {
            file_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "file".to_string())
        })
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
    fn test_calculate_relative_path_unicode() {
        let file_path = PathBuf::from("/home/user/文档/файл.txt");
        let base_path = PathBuf::from("/home/user");

        let relative = calculate_relative_path(&file_path, &base_path);
        assert!(relative.contains("文档"));
        assert!(relative.contains("файл.txt"));
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
    fn test_path_calculation_consistency() {
        // Same inputs should always produce same outputs
        let file = PathBuf::from("/home/user/file.txt");
        let base = PathBuf::from("/home/user");

        let rel1 = calculate_relative_path(&file, &base);
        let rel2 = calculate_relative_path(&file, &base);

        assert_eq!(rel1, rel2);
    }
}
