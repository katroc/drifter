use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tracing::{info, error, debug};

#[derive(Debug, Clone)]
pub struct JobRow {
    pub id: i64,
    pub created_at: String,
    pub status: String,
    pub source_path: String,
    pub size_bytes: i64,
    pub staged_path: Option<String>,
    pub error: Option<String>,
    pub scan_status: Option<String>,
    pub s3_upload_id: Option<String>,
    pub s3_key: Option<String>,
    pub priority: i64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct UploadPart {
    pub part_number: i64,
    pub size_bytes: i64,
    pub status: String,
    pub etag: Option<String>,
}

pub fn init_db(state_dir: &str) -> Result<Connection> {
    debug!("Initializing database in: {}", state_dir);
    if !Path::new(state_dir).exists() {
        info!("Creating state directory: {}", state_dir);
        fs::create_dir_all(state_dir).context("create state dir")?;
    }
    let db_path = Path::new(state_dir).join("drifter.db");
    let conn = Connection::open(&db_path).context("open sqlite db")?;
    
    if let Err(e) = conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = OFF;
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            upload_id TEXT,
            part_size INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );
        CREATE TABLE IF NOT EXISTS parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_id INTEGER NOT NULL,
            part_number INTEGER NOT NULL,
            etag TEXT,
            size_bytes INTEGER NOT NULL,
            status TEXT NOT NULL,
            retries INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(upload_id) REFERENCES uploads(id)
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );
        CREATE TABLE IF NOT EXISTS secrets (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        ",
    ) {
        error!("Failed to initialize database schema: {}", e);
        return Err(e.into());
    }

    // Migration for existing databases
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN s3_upload_id TEXT", []);
    // Migration for priority
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0", []);

    info!("Database initialized successfully at {:?}", db_path);
    Ok(conn)
}

pub fn list_active_jobs(conn: &Connection, limit: i64) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, created_at, status, source_path, size_bytes, staged_path, error, scan_status, s3_upload_id, s3_key, priority FROM jobs 
         WHERE status NOT IN ('complete', 'quarantined', 'quarantined_removed', 'cancelled') 
         OR datetime(created_at) > datetime('now', '-15 seconds')
         ORDER BY priority DESC, id DESC LIMIT ?",
    )?;
    let rows = stmt
        .query_map(params![limit], |row| {
            Ok(JobRow {
                id: row.get(0)?,
                created_at: row.get(1)?,
                status: row.get(2)?,
                source_path: row.get(3)?,
                size_bytes: row.get(4)?,
                staged_path: row.get(5)?,
                error: row.get(6)?,
                scan_status: row.get(7)?,
                s3_upload_id: row.get(8)?,
                s3_key: row.get(9)?,
                priority: row.get(10).unwrap_or(0),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn list_history_jobs(conn: &Connection, limit: i64, filter: Option<&str>) -> Result<Vec<JobRow>> {
    let sql = match filter {
        Some("Complete") => "SELECT id, created_at, status, source_path, size_bytes, staged_path, error, scan_status, s3_upload_id, s3_key, priority FROM jobs WHERE status = 'complete' ORDER BY id DESC LIMIT ?",
        Some("Quarantined") => "SELECT id, created_at, status, source_path, size_bytes, staged_path, error, scan_status, s3_upload_id, s3_key, priority FROM jobs WHERE status IN ('quarantined', 'quarantined_removed') ORDER BY id DESC LIMIT ?",
        _ => "SELECT id, created_at, status, source_path, size_bytes, staged_path, error, scan_status, s3_upload_id, s3_key, priority FROM jobs WHERE status IN ('complete', 'quarantined', 'quarantined_removed', 'cancelled') ORDER BY id DESC LIMIT ?",
    };
    
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map(params![limit], |row| {
            Ok(JobRow {
                id: row.get(0)?,
                created_at: row.get(1)?,
                status: row.get(2)?,
                source_path: row.get(3)?,
                size_bytes: row.get(4)?,
                staged_path: row.get(5)?,
                error: row.get(6)?,
                scan_status: row.get(7)?,
                s3_upload_id: row.get(8)?,
                s3_key: row.get(9)?,
                priority: row.get(10).unwrap_or(0),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn list_quarantined_jobs(conn: &Connection, limit: i64) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, created_at, status, source_path, size_bytes, staged_path, error, scan_status, s3_upload_id, s3_key, priority FROM jobs WHERE status = 'quarantined' ORDER BY id DESC LIMIT ?",
    )?;
    let rows = stmt
        .query_map(params![limit], |row| {
            Ok(JobRow {
                id: row.get(0)?,
                created_at: row.get(1)?,
                status: row.get(2)?,
                source_path: row.get(3)?,
                size_bytes: row.get(4)?,
                staged_path: row.get(5)?,
                error: row.get(6)?,
                scan_status: row.get(7)?,
                s3_upload_id: row.get(8)?,
                s3_key: row.get(9)?,
                priority: row.get(10).unwrap_or(0),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn update_job_upload_id(conn: &Connection, job_id: i64, upload_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET s3_upload_id = ? WHERE id = ?",
        params![upload_id, job_id],
    )?;
    Ok(())
}

pub fn create_job(conn: &Connection, source_path: &str, size_bytes: i64, s3_key: Option<&str>) -> Result<i64> {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO jobs (created_at, status, source_path, size_bytes, s3_key) VALUES (?, ?, ?, ?, ?)",
        params![now, "ingesting", source_path, size_bytes, s3_key],
    )?;
    let id = conn.last_insert_rowid();
    debug!("Created job ID {} for file: {}", id, source_path);
    Ok(id)
}

pub fn update_job_staged(
    conn: &Connection,
    job_id: i64,
    staged_path: &str,
    status: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET staged_path = ?, status = ?, error = NULL WHERE id = ?",
        params![staged_path, status, job_id],
    )?;
    Ok(())
}

pub fn update_job_error(conn: &Connection, job_id: i64, status: &str, error: &str) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET status = ?, error = ? WHERE id = ?",
        params![status, error, job_id],
    )?;
    Ok(())
}

pub fn retry_job(conn: &Connection, job_id: i64) -> Result<()> {
    info!("Retrying job ID {}", job_id);
    // Check if scan was already completed
    let mut scan_completed = false;
    let mut stmt = conn.prepare("SELECT scan_status FROM jobs WHERE id = ?")?;
    let mut rows = stmt.query(params![job_id])?;
    if let Some(row) = rows.next()? {
        let status: Option<String> = row.get(0)?;
        if status.as_deref() == Some("completed") {
            scan_completed = true;
        }
    }
    drop(rows);
    drop(stmt);

    if scan_completed {
        // Resume upload (skip scan)
        debug!("Job {} scan already complete, resuming upload", job_id);
        conn.execute(
            "UPDATE jobs SET status = ?, error = NULL, upload_status = NULL WHERE id = ?",
            params!["scanned", job_id],
        )?;
        insert_event(conn, job_id, "retry", "job resumed (scan skipped)")?;
    } else {
        // Full retry
        debug!("Job {} scan incomplete/failed, retrying from start", job_id);
        conn.execute(
            "UPDATE jobs SET status = ?, error = NULL, scan_status = NULL, upload_status = NULL WHERE id = ?",
            params!["queued", job_id],
        )?;
        insert_event(conn, job_id, "retry", "job retried manually")?;
    }
    Ok(())
}

pub fn delete_job(conn: &Connection, job_id: i64) -> Result<()> {
    info!("Deleting job ID {}", job_id);
    // Delete associated events first (foreign key constraint)
    conn.execute("DELETE FROM events WHERE job_id = ?", params![job_id])?;
    // Delete the job
    conn.execute("DELETE FROM jobs WHERE id = ?", params![job_id])?;
    Ok(())
}



pub fn pause_job(conn: &Connection, job_id: i64) -> Result<()> {
    info!("Pausing job ID {}", job_id);
    // Set status to 'paused'. We don't clear s3_upload_id because we want to resume.
    conn.execute(
        "UPDATE jobs SET status = 'paused', error = NULL WHERE id = ?",
        params![job_id],
    )?;
    insert_event(conn, job_id, "pause", "job paused by user")?;
    Ok(())
}

pub fn resume_job(conn: &Connection, job_id: i64) -> Result<()> {
    info!("Resuming job ID {}", job_id);
    
    // Determine target status based on current state (or just 'scanned' if it has an upload_id?)
    // If it has staged_path and we assume it was scanning or uploading.
    // Simplifying assumption: If it was paused, it was likely 'queued', 'scanning' or 'uploading'.
    // If we set it to 'queued', it might re-scan.
    // If we set it to 'scanned', it skips scan.
    
    // Check if scan was complete (similar to retry_job logic)
    let mut scan_completed = false;
    let mut stmt = conn.prepare("SELECT scan_status FROM jobs WHERE id = ?")?;
    let mut rows = stmt.query(params![job_id])?;
    if let Some(row) = rows.next()? {
        let status: Option<String> = row.get(0)?;
        if status.as_deref() == Some("completed") || status.as_deref() == Some("clean") || status.as_deref() == Some("scanned") {
            scan_completed = true;
        }
    }
    drop(rows);
    drop(stmt);

    let new_status = if scan_completed { "scanned" } else { "queued" };

    conn.execute(
        "UPDATE jobs SET status = ?, error = NULL WHERE id = ?",
        params![new_status, job_id],
    )?;
    insert_event(conn, job_id, "resume", &format!("job resumed to {}", new_status))?;
    Ok(())
}

pub fn cancel_job(conn: &Connection, job_id: i64) -> Result<()> {
    info!("Cancelling job ID {}", job_id);
    conn.execute(
        "UPDATE jobs SET status = 'cancelled', error = 'Cancelled by user' WHERE id = ?",
        params![job_id],
    )?;
    Ok(())
}

// Clear history based on filter (All, Complete, Quarantined)
// Always excludes "active" jobs (pending, scanning, uploading)
pub fn clear_history(conn: &Connection, filter: Option<&str>) -> Result<()> {
    // 1. Identify IDs to delete
    let mut query = "SELECT id FROM jobs WHERE status NOT IN ('pending', 'scanning', 'uploading', 'failed_retryable')".to_string();
    
    match filter {
        Some("Quarantined") => {
            query.push_str(" AND (status = 'quarantined' OR status = 'quarantined_removed')");
        }
        Some("Complete") => {
            query.push_str(" AND status = 'complete'");
        }
        _ => {
            // All: match base query
        }
    }

    let mut stmt = conn.prepare(&query)?;
    let ids_iter = stmt.query_map([], |row| row.get(0))?;
    let mut ids: Vec<i64> = Vec::new();
    for id in ids_iter {
        ids.push(id?);
    }

    if ids.is_empty() {
        return Ok(());
    }

    // 2. Delete dependencies
    // Construct safe "IN (?,?,?)" clause
    // Since SQLite limit is usually 999 parameters, batching might be needed for huge lists.
    // For now, simpler iteration or string construction if local
    // Let's use iteration for simplicity and safety, wrapped in transaction? 
    // Or just robust one-by-one deletions since user interaction isn't high frequency high volume here usually.
    // Actually better: just use delete_job for each ID, but ensure delete_job deletes uploads/parts too.
    
    // Check if we should update delete_job first? Yes.
    // But let's fix it here for now to be self-contained.
    
    for id in ids {
        // Delete parts
        conn.execute("DELETE FROM parts WHERE upload_id IN (SELECT id FROM uploads WHERE job_id = ?)", params![id])?;
        // Delete uploads
        conn.execute("DELETE FROM uploads WHERE job_id = ?", params![id])?;
        // Delete events
        conn.execute("DELETE FROM events WHERE job_id = ?", params![id])?;
        // Delete job
        conn.execute("DELETE FROM jobs WHERE id = ?", params![id])?;
    }
    
    Ok(())
}

pub fn insert_event(conn: &Connection, job_id: i64, event_type: &str, message: &str) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO events (job_id, event_type, message, created_at) VALUES (?, ?, ?, ?)",
        params![job_id, event_type, message, now],
    )?;
    Ok(())
}

pub fn update_scan_status(conn: &Connection, job_id: i64, status: &str, global_status: &str) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET scan_status = ?, status = ? WHERE id = ?",
        params![status, global_status, job_id],
    )?;
    Ok(())
}

pub fn update_upload_status(conn: &Connection, job_id: i64, status: &str, global_status: &str) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET upload_status = ?, status = ? WHERE id = ?",
        params![status, global_status, job_id],
    )?;
    Ok(())
}

pub fn get_next_job(conn: &Connection, current_status: &str) -> Result<Option<JobRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, created_at, status, source_path, size_bytes, staged_path, error, scan_status, s3_upload_id, s3_key, priority FROM jobs WHERE status = ? ORDER BY priority DESC, id ASC LIMIT 1",
    )?;
    let mut rows = stmt.query_map(params![current_status], |row| {
        Ok(JobRow {
            id: row.get(0)?,
            created_at: row.get(1)?,
            status: row.get(2)?,
            source_path: row.get(3)?,
            size_bytes: row.get(4)?,
            staged_path: row.get(5)?,
            error: row.get(6)?,
            scan_status: row.get(7)?,
            s3_upload_id: row.get(8)?,
            s3_key: row.get(9)?,
            priority: row.get(10).unwrap_or(0),
        })
    })?;

    if let Some(row) = rows.next() {
        Ok(Some(row?))
    } else {
        Ok(None)
    }
}

pub fn get_job(conn: &Connection, job_id: i64) -> Result<Option<JobRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, created_at, status, source_path, size_bytes, staged_path, error, scan_status, s3_upload_id, s3_key, priority FROM jobs WHERE id = ?",
    )?;
    let mut rows = stmt.query_map(params![job_id], |row| {
        Ok(JobRow {
            id: row.get(0)?,
            created_at: row.get(1)?,
            status: row.get(2)?,
            source_path: row.get(3)?,
            size_bytes: row.get(4)?,
            staged_path: row.get(5)?,
            error: row.get(6)?,
            scan_status: row.get(7)?,
            s3_upload_id: row.get(8)?,
            s3_key: row.get(9)?,
            priority: row.get(10).unwrap_or(0),
        })
    })?;

    if let Some(row) = rows.next() {
        Ok(Some(row?))
    } else {
        Ok(None)
    }
}

pub fn set_job_priority(conn: &Connection, job_id: i64, priority: i64) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET priority = ? WHERE id = ?",
        params![priority, job_id],
    )?;
    Ok(())
}

const SECRET_KEY_XOR: &[u8] = b"drifter-secret-pad-123";

fn obfuscate(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    for (i, &b) in bytes.iter().enumerate() {
        result.push(b ^ SECRET_KEY_XOR[i % SECRET_KEY_XOR.len()]);
    }
    hex::encode(result)
}

fn deobfuscate(input: &str) -> Result<String> {
    let bytes = hex::decode(input).context("decode hex secret")?;
    let mut result = Vec::with_capacity(bytes.len());
    for (i, &b) in bytes.iter().enumerate() {
        result.push(b ^ SECRET_KEY_XOR[i % SECRET_KEY_XOR.len()]);
    }
    String::from_utf8(result).context("parse utf8 secret")
}

pub fn set_secret(conn: &Connection, key: &str, value: &str) -> Result<()> {
    let val = obfuscate(value);
    conn.execute(
        "INSERT OR REPLACE INTO secrets (key, value) VALUES (?, ?)",
        params![key, val],
    )?;
    Ok(())
}

pub fn get_secret(conn: &Connection, key: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT value FROM secrets WHERE key = ?")?;
    let mut rows = stmt.query(params![key])?;
    if let Some(row) = rows.next()? {
        let val: String = row.get(0)?;
        Ok(Some(deobfuscate(&val)?))
    } else {
        Ok(None)
    }
}

// --- Settings (non-secret config) ---

pub fn set_setting(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        params![key, value],
    )?;
    Ok(())
}

#[allow(dead_code)]
pub fn get_setting(conn: &Connection, key: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT value FROM settings WHERE key = ?")?;
    let mut rows = stmt.query(params![key])?;
    if let Some(row) = rows.next()? {
        Ok(Some(row.get(0)?))
    } else {
        Ok(None)
    }
}

pub fn load_all_settings(conn: &Connection) -> Result<HashMap<String, String>> {
    let mut stmt = conn.prepare("SELECT key, value FROM settings")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut map = HashMap::new();
    for row in rows {
        let (k, v) = row?;
        map.insert(k, v);
    }
    Ok(map)
}

pub fn has_settings(conn: &Connection) -> Result<bool> {
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM settings", [], |row| row.get(0))?;
    Ok(count > 0)
}


