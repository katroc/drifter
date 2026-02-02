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
    pub session_id: String,
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
    pub checksum: Option<String>,
    pub remote_checksum: Option<String>,
    pub retry_count: i64,
    pub next_retry_at: Option<String>,
    pub scan_duration_ms: Option<i64>,
    pub upload_duration_ms: Option<i64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct UploadPart {
    pub part_number: i64,
    pub size_bytes: i64,
    pub status: String,
    pub etag: Option<String>,
    pub checksum_sha256: Option<String>,
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
            retry_count INTEGER DEFAULT 0,
            next_retry_at TEXT
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
            checksum_sha256 TEXT,
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
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN session_id TEXT DEFAULT 'legacy'", []);
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN s3_upload_id TEXT", []);
    // Migration for priority
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0", []);
    // Migration for checksums
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN checksum TEXT", []);
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN remote_checksum TEXT", []);
    let _ = conn.execute("ALTER TABLE parts ADD COLUMN checksum_sha256 TEXT", []);
    // Migration for retries
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN retry_count INTEGER DEFAULT 0", []);
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN next_retry_at TEXT", []);
    // Migration for timing
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN scan_duration_ms INTEGER", []);
    let _ = conn.execute("ALTER TABLE jobs ADD COLUMN upload_duration_ms INTEGER", []);

    info!("Database initialized successfully at {:?}", db_path);
    Ok(conn)
}

pub const JOB_COLUMNS: &str = "id, session_id, created_at, status, source_path, size_bytes, staged_path, error, scan_status, s3_upload_id, s3_key, priority, checksum, remote_checksum, retry_count, next_retry_at, scan_duration_ms, upload_duration_ms";

impl<'a> TryFrom<&'a rusqlite::Row<'a>> for JobRow {
    type Error = rusqlite::Error;

    fn try_from(row: &'a rusqlite::Row<'a>) -> Result<Self, Self::Error> {
        Ok(JobRow {
            id: row.get(0)?,
            session_id: row.get(1)?,
            created_at: row.get(2)?,
            status: row.get(3)?,
            source_path: row.get(4)?,
            size_bytes: row.get(5)?,
            staged_path: row.get(6)?,
            error: row.get(7)?,
            scan_status: row.get(8)?,
            s3_upload_id: row.get(9)?,
            s3_key: row.get(10)?,
            priority: row.get(11).unwrap_or(0),
            checksum: row.get(12)?,
            remote_checksum: row.get(13)?,
            retry_count: row.get(14).unwrap_or(0),
            next_retry_at: row.get(15)?,
            scan_duration_ms: row.get(16)?,
            upload_duration_ms: row.get(17)?,
        })
    }
}

// Helper to map a row to a JobRow
fn map_job_row(row: &rusqlite::Row) -> rusqlite::Result<JobRow> {
    JobRow::try_from(row)
}

pub fn list_active_jobs(conn: &Connection, limit: i64) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(
        &format!("SELECT {} FROM jobs 
         WHERE status NOT IN ('complete', 'quarantined', 'quarantined_removed', 'cancelled') 
         OR datetime(created_at) > datetime('now', '-15 seconds')
         ORDER BY priority DESC, id DESC LIMIT ?", JOB_COLUMNS),
    )?;
    let rows = stmt
        .query_map(params![limit], map_job_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn list_history_jobs(conn: &Connection, limit: i64, filter: Option<&str>) -> Result<Vec<JobRow>> {
    let sql = match filter {
        Some("Complete") => format!("SELECT {} FROM jobs WHERE status = 'complete' ORDER BY id DESC LIMIT ?", JOB_COLUMNS),
        Some("Quarantined") => format!("SELECT {} FROM jobs WHERE status IN ('quarantined', 'quarantined_removed') ORDER BY id DESC LIMIT ?", JOB_COLUMNS),
        _ => format!("SELECT {} FROM jobs WHERE status IN ('complete', 'quarantined', 'quarantined_removed', 'cancelled') ORDER BY id DESC LIMIT ?", JOB_COLUMNS),
    };
    
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params![limit], map_job_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn list_quarantined_jobs(conn: &Connection, limit: i64) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(
        &format!("SELECT {} FROM jobs WHERE status = 'quarantined' ORDER BY id DESC LIMIT ?", JOB_COLUMNS),
    )?;
    let rows = stmt
        .query_map(params![limit], map_job_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn update_job_retry_state(conn: &Connection, job_id: i64, retry_count: i64, next_retry_at: Option<&str>, status: &str, error: &str) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET retry_count = ?, next_retry_at = ?, status = ?, error = ? WHERE id = ?",
        params![retry_count, next_retry_at, status, error, job_id],
    )?;
    Ok(())
}

pub fn list_retryable_jobs(conn: &Connection) -> Result<Vec<JobRow>> {
    let now = Utc::now().to_rfc3339();
    let mut stmt = conn.prepare(
        &format!("SELECT {} FROM jobs 
         WHERE status = 'retry_pending' AND next_retry_at <= ?
         ORDER BY priority DESC, id ASC", JOB_COLUMNS),
    )?;
    let rows = stmt
        .query_map(params![now], map_job_row)?
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

pub fn create_job(conn: &Connection, session_id: &str, source_path: &str, size_bytes: i64, s3_key: Option<&str>) -> Result<i64> {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, s3_key, retry_count) VALUES (?, ?, ?, ?, ?, ?, 0)",
        params![session_id, now, "ingesting", source_path, size_bytes, s3_key],
    )?;
    let id = conn.last_insert_rowid();
    debug!("Created job ID {} for file: {}", id, source_path);
    Ok(id)
}

pub fn get_session_jobs(conn: &Connection, session_id: &str) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(
        &format!("SELECT {} FROM jobs WHERE session_id = ? ORDER BY id ASC", JOB_COLUMNS),
    )?;
    let rows = stmt
        .query_map(params![session_id], map_job_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn count_jobs_with_status(conn: &Connection, status: &str) -> Result<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM jobs WHERE status = ?",
        params![status],
        |row| row.get(0),
    )?;
    Ok(count)
}

pub fn count_pending_session_jobs(conn: &Connection, session_id: &str) -> Result<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM jobs WHERE session_id = ? AND status NOT IN ('complete', 'quarantined', 'quarantined_removed', 'cancelled', 'failed', 'failed_retryable')",
        params![session_id],
        |row| row.get(0),
    )?;
    Ok(count)
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

pub fn update_job_checksums(conn: &Connection, job_id: i64, local: Option<&str>, remote: Option<&str>) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET checksum = ?, remote_checksum = ? WHERE id = ?",
        params![local, remote, job_id],
    )?;
    Ok(())
}

pub fn update_scan_duration(conn: &Connection, job_id: i64, duration_ms: i64) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET scan_duration_ms = ? WHERE id = ?",
        params![duration_ms, job_id],
    )?;
    Ok(())
}

pub fn update_upload_duration(conn: &Connection, job_id: i64, duration_ms: i64) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET upload_duration_ms = ? WHERE id = ?",
        params![duration_ms, job_id],
    )?;
    Ok(())
}

pub fn get_next_job(conn: &Connection, current_status: &str) -> Result<Option<JobRow>> {
    let mut stmt = conn.prepare(
        &format!("SELECT {} FROM jobs WHERE status = ? ORDER BY priority DESC, id ASC LIMIT 1", JOB_COLUMNS),
    )?;
    let mut rows = stmt.query_map(params![current_status], map_job_row)?;

    if let Some(row) = rows.next() {
        Ok(Some(row?))
    } else {
        Ok(None)
    }
}

pub fn get_job(conn: &Connection, job_id: i64) -> Result<Option<JobRow>> {
    let mut stmt = conn.prepare(
        &format!("SELECT {} FROM jobs WHERE id = ?", JOB_COLUMNS),
    )?;
    let mut rows = stmt.query_map(params![job_id], map_job_row)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_columns_count() {
        let count = JOB_COLUMNS.split(',').count();
        assert_eq!(count, 18, "JOB_COLUMNS should have 18 fields");
    }

    #[test]
    fn test_job_row_mapping() -> Result<()> {
        let conn = Connection::open_in_memory()?;

        conn.execute(
            "CREATE TABLE jobs (
                id INTEGER PRIMARY KEY,
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
                priority INTEGER DEFAULT 0,
                checksum TEXT,
                remote_checksum TEXT,
                error TEXT,
                retry_count INTEGER DEFAULT 0,
                next_retry_at TEXT,
                scan_duration_ms INTEGER,
                upload_duration_ms INTEGER
            )",
            [],
        )?;

        conn.execute(
            "INSERT INTO jobs (
                session_id, created_at, status, source_path, size_bytes, priority, retry_count
            ) VALUES (
                'test-session', '2023-01-01', 'pending', '/tmp/test', 100, 10, 5
            )",
            [],
        )?;

        let mut stmt = conn.prepare(&format!("SELECT {} FROM jobs", JOB_COLUMNS))?;
        let rows = stmt.query_map([], |row| JobRow::try_from(row))?;

        let jobs: Vec<JobRow> = rows.collect::<Result<_, _>>()?;
        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];

        assert_eq!(job.session_id, "test-session");
        assert_eq!(job.status, "pending");
        assert_eq!(job.source_path, "/tmp/test");
        assert_eq!(job.size_bytes, 100);
        assert_eq!(job.priority, 10);
        assert_eq!(job.retry_count, 5);

        Ok(())
    }

    // Helper function to initialize test database with schema
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
                created_at TEXT NOT NULL
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

    // --- Job CRUD Tests ---

    #[test]
    fn test_create_job_success() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "test-session", "/tmp/file.txt", 1024, Some("s3-key.txt"))?;

        assert!(job_id > 0);
        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.session_id, "test-session");
        assert_eq!(job.source_path, "/tmp/file.txt");
        assert_eq!(job.size_bytes, 1024);
        assert_eq!(job.s3_key, Some("s3-key.txt".to_string()));
        assert_eq!(job.status, "ingesting");
        assert_eq!(job.retry_count, 0);

        Ok(())
    }

    #[test]
    fn test_get_job_not_found() -> Result<()> {
        let conn = setup_test_db()?;

        let job = get_job(&conn, 999)?;
        assert!(job.is_none());

        Ok(())
    }

    #[test]
    fn test_list_active_jobs_filtering() -> Result<()> {
        let conn = setup_test_db()?;

        create_job(&conn, "session1", "/tmp/active1.txt", 100, None)?;
        create_job(&conn, "session1", "/tmp/active2.txt", 200, None)?;

        // Create complete and quarantined jobs with old timestamps (more than 15 seconds ago)
        let old_time = (Utc::now() - chrono::Duration::seconds(30)).to_rfc3339();
        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, retry_count) VALUES (?, ?, ?, ?, ?, 0)",
            params!["session1", &old_time, "complete", "/tmp/complete.txt", 300],
        )?;
        conn.execute(
            "INSERT INTO jobs (session_id, created_at, status, source_path, size_bytes, retry_count) VALUES (?, ?, ?, ?, ?, 0)",
            params!["session1", &old_time, "quarantined", "/tmp/quarantined.txt", 400],
        )?;

        let active = list_active_jobs(&conn, 100)?;
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].source_path, "/tmp/active2.txt");
        assert_eq!(active[1].source_path, "/tmp/active1.txt");

        Ok(())
    }

    #[test]
    fn test_list_history_jobs_all_filter() -> Result<()> {
        let conn = setup_test_db()?;

        let id1 = create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        update_job_error(&conn, id1, "complete", "")?;

        let id2 = create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        update_job_error(&conn, id2, "quarantined", "infected")?;

        let id3 = create_job(&conn, "session1", "/tmp/file3.txt", 300, None)?;
        update_job_error(&conn, id3, "cancelled", "user cancelled")?;

        create_job(&conn, "session1", "/tmp/active.txt", 400, None)?;

        let history = list_history_jobs(&conn, 100, None)?;
        assert_eq!(history.len(), 3);

        Ok(())
    }

    #[test]
    fn test_list_history_jobs_complete_filter() -> Result<()> {
        let conn = setup_test_db()?;

        let id1 = create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        update_job_error(&conn, id1, "complete", "")?;

        let id2 = create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        update_job_error(&conn, id2, "quarantined", "infected")?;

        let history = list_history_jobs(&conn, 100, Some("Complete"))?;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].status, "complete");

        Ok(())
    }

    #[test]
    fn test_list_history_jobs_quarantined_filter() -> Result<()> {
        let conn = setup_test_db()?;

        let id1 = create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        update_job_error(&conn, id1, "complete", "")?;

        let id2 = create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        update_job_error(&conn, id2, "quarantined", "infected")?;

        let id3 = create_job(&conn, "session1", "/tmp/file3.txt", 300, None)?;
        update_job_error(&conn, id3, "quarantined_removed", "infected and removed")?;

        let history = list_history_jobs(&conn, 100, Some("Quarantined"))?;
        assert_eq!(history.len(), 2);

        Ok(())
    }

    #[test]
    fn test_list_quarantined_jobs() -> Result<()> {
        let conn = setup_test_db()?;

        let id1 = create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        update_job_error(&conn, id1, "quarantined", "infected")?;

        let id2 = create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        update_job_error(&conn, id2, "complete", "")?;

        let quarantined = list_quarantined_jobs(&conn, 100)?;
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].status, "quarantined");

        Ok(())
    }

    #[test]
    fn test_get_session_jobs() -> Result<()> {
        let conn = setup_test_db()?;

        create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        create_job(&conn, "session2", "/tmp/file3.txt", 300, None)?;

        let jobs = get_session_jobs(&conn, "session1")?;
        assert_eq!(jobs.len(), 2);

        let jobs2 = get_session_jobs(&conn, "session2")?;
        assert_eq!(jobs2.len(), 1);

        Ok(())
    }

    #[test]
    fn test_count_jobs_with_status() -> Result<()> {
        let conn = setup_test_db()?;

        create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;

        let id3 = create_job(&conn, "session1", "/tmp/file3.txt", 300, None)?;
        update_job_error(&conn, id3, "complete", "")?;

        let count = count_jobs_with_status(&conn, "ingesting")?;
        assert_eq!(count, 2);

        let count_complete = count_jobs_with_status(&conn, "complete")?;
        assert_eq!(count_complete, 1);

        Ok(())
    }

    #[test]
    fn test_count_pending_session_jobs() -> Result<()> {
        let conn = setup_test_db()?;

        create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;

        let id3 = create_job(&conn, "session1", "/tmp/file3.txt", 300, None)?;
        update_job_error(&conn, id3, "complete", "")?;

        create_job(&conn, "session2", "/tmp/file4.txt", 400, None)?;

        let count = count_pending_session_jobs(&conn, "session1")?;
        assert_eq!(count, 2);

        Ok(())
    }

    #[test]
    fn test_update_job_staged() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_job_staged(&conn, job_id, "/staging/file.txt", "queued")?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.staged_path, Some("/staging/file.txt".to_string()));
        assert_eq!(job.status, "queued");
        assert_eq!(job.error, None);

        Ok(())
    }

    #[test]
    fn test_update_job_error() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_job_error(&conn, job_id, "failed", "Network timeout")?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.status, "failed");
        assert_eq!(job.error, Some("Network timeout".to_string()));

        Ok(())
    }

    #[test]
    fn test_set_job_priority() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        set_job_priority(&conn, job_id, 50)?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.priority, 50);

        Ok(())
    }

    #[test]
    fn test_get_next_job_priority_ordering() -> Result<()> {
        let conn = setup_test_db()?;

        let id1 = create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        update_job_error(&conn, id1, "queued", "")?;
        set_job_priority(&conn, id1, 10)?;

        let id2 = create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        update_job_error(&conn, id2, "queued", "")?;
        set_job_priority(&conn, id2, 50)?;

        let id3 = create_job(&conn, "session1", "/tmp/file3.txt", 300, None)?;
        update_job_error(&conn, id3, "queued", "")?;
        set_job_priority(&conn, id3, 30)?;

        let next = get_next_job(&conn, "queued")?.expect("Should have next job");
        assert_eq!(next.id, id2);
        assert_eq!(next.priority, 50);

        Ok(())
    }

    #[test]
    fn test_cancel_job() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        cancel_job(&conn, job_id)?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.status, "cancelled");
        assert_eq!(job.error, Some("Cancelled by user".to_string()));

        Ok(())
    }

    #[test]
    fn test_delete_job() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        insert_event(&conn, job_id, "test", "test message")?;

        delete_job(&conn, job_id)?;

        let job = get_job(&conn, job_id)?;
        assert!(job.is_none());

        Ok(())
    }

    // --- Retry Logic Tests ---

    #[test]
    fn test_update_job_retry_state() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        let next_retry = "2025-01-01T12:00:00Z";
        update_job_retry_state(&conn, job_id, 3, Some(next_retry), "retry_pending", "Temporary failure")?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.retry_count, 3);
        assert_eq!(job.next_retry_at, Some(next_retry.to_string()));
        assert_eq!(job.status, "retry_pending");
        assert_eq!(job.error, Some("Temporary failure".to_string()));

        Ok(())
    }

    #[test]
    fn test_list_retryable_jobs_timing() -> Result<()> {
        let conn = setup_test_db()?;

        let now = Utc::now();
        let past = (now - chrono::Duration::seconds(60)).to_rfc3339();
        let future = (now + chrono::Duration::seconds(60)).to_rfc3339();

        let id1 = create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        update_job_retry_state(&conn, id1, 1, Some(&past), "retry_pending", "error")?;

        let id2 = create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        update_job_retry_state(&conn, id2, 1, Some(&future), "retry_pending", "error")?;

        let id3 = create_job(&conn, "session1", "/tmp/file3.txt", 300, None)?;
        update_job_retry_state(&conn, id3, 1, Some(&past), "retry_pending", "error")?;

        let retryable = list_retryable_jobs(&conn)?;
        assert_eq!(retryable.len(), 2);
        assert!(retryable.iter().any(|j| j.id == id1));
        assert!(retryable.iter().any(|j| j.id == id3));

        Ok(())
    }

    #[test]
    fn test_retry_job_with_completed_scan() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_scan_status(&conn, job_id, "completed", "scanned")?;
        update_job_error(&conn, job_id, "failed", "Upload error")?;

        retry_job(&conn, job_id)?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.status, "scanned");
        assert_eq!(job.error, None);

        Ok(())
    }

    #[test]
    fn test_retry_job_without_completed_scan() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_job_error(&conn, job_id, "failed", "Scan error")?;

        retry_job(&conn, job_id)?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.status, "queued");
        assert_eq!(job.error, None);

        Ok(())
    }

    #[test]
    fn test_pause_and_resume_job() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_scan_status(&conn, job_id, "completed", "scanned")?;

        pause_job(&conn, job_id)?;
        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.status, "paused");

        resume_job(&conn, job_id)?;
        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.status, "scanned");

        Ok(())
    }

    // --- Checksum Tests ---

    #[test]
    fn test_update_job_checksums() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_job_checksums(&conn, job_id, Some("local-sha256"), Some("remote-sha256"))?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.checksum, Some("local-sha256".to_string()));
        assert_eq!(job.remote_checksum, Some("remote-sha256".to_string()));

        Ok(())
    }

    #[test]
    fn test_update_job_checksums_partial() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_job_checksums(&conn, job_id, Some("local-sha256"), None)?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.checksum, Some("local-sha256".to_string()));
        assert_eq!(job.remote_checksum, None);

        Ok(())
    }

    // --- Upload ID Tests ---

    #[test]
    fn test_update_job_upload_id() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_job_upload_id(&conn, job_id, "upload-12345")?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.s3_upload_id, Some("upload-12345".to_string()));

        Ok(())
    }

    // --- Status Update Tests ---

    #[test]
    fn test_update_scan_status() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_scan_status(&conn, job_id, "clean", "scanned")?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.scan_status, Some("clean".to_string()));
        assert_eq!(job.status, "scanned");

        Ok(())
    }

    #[test]
    fn test_update_upload_status() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_upload_status(&conn, job_id, "in_progress", "uploading")?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.status, "uploading");

        Ok(())
    }

    // --- Duration Tests ---

    #[test]
    fn test_update_scan_duration() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_scan_duration(&conn, job_id, 1500)?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.scan_duration_ms, Some(1500));

        Ok(())
    }

    #[test]
    fn test_update_upload_duration() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        update_upload_duration(&conn, job_id, 5000)?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.upload_duration_ms, Some(5000));

        Ok(())
    }

    // --- Event Tests ---

    #[test]
    fn test_insert_event() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        insert_event(&conn, job_id, "test_event", "Test message")?;

        let mut stmt = conn.prepare("SELECT event_type, message FROM events WHERE job_id = ?")?;
        let mut rows = stmt.query(params![job_id])?;

        if let Some(row) = rows.next()? {
            let event_type: String = row.get(0)?;
            let message: String = row.get(1)?;
            assert_eq!(event_type, "test_event");
            assert_eq!(message, "Test message");
        } else {
            panic!("Event not found");
        }

        Ok(())
    }

    // --- Secrets Tests ---

    #[test]
    fn test_set_and_get_secret() -> Result<()> {
        let conn = setup_test_db()?;

        set_secret(&conn, "test_key", "secret_value")?;
        let value = get_secret(&conn, "test_key")?;

        assert_eq!(value, Some("secret_value".to_string()));

        Ok(())
    }

    #[test]
    fn test_get_nonexistent_secret() -> Result<()> {
        let conn = setup_test_db()?;

        let value = get_secret(&conn, "nonexistent")?;
        assert!(value.is_none());

        Ok(())
    }

    #[test]
    fn test_secret_obfuscation() -> Result<()> {
        let conn = setup_test_db()?;

        set_secret(&conn, "password", "my-secret-password")?;

        let mut stmt = conn.prepare("SELECT value FROM secrets WHERE key = ?")?;
        let mut rows = stmt.query(params!["password"])?;

        if let Some(row) = rows.next()? {
            let stored: String = row.get(0)?;
            assert_ne!(stored, "my-secret-password");
            assert!(stored.len() > 0);
        }

        let decrypted = get_secret(&conn, "password")?;
        assert_eq!(decrypted, Some("my-secret-password".to_string()));

        Ok(())
    }

    // --- Settings Tests ---

    #[test]
    fn test_set_and_get_setting() -> Result<()> {
        let conn = setup_test_db()?;

        set_setting(&conn, "theme", "dark")?;
        let value = get_setting(&conn, "theme")?;

        assert_eq!(value, Some("dark".to_string()));

        Ok(())
    }

    #[test]
    fn test_load_all_settings() -> Result<()> {
        let conn = setup_test_db()?;

        set_setting(&conn, "theme", "dark")?;
        set_setting(&conn, "log_level", "debug")?;
        set_setting(&conn, "concurrency", "4")?;

        let settings = load_all_settings(&conn)?;

        assert_eq!(settings.len(), 3);
        assert_eq!(settings.get("theme"), Some(&"dark".to_string()));
        assert_eq!(settings.get("log_level"), Some(&"debug".to_string()));
        assert_eq!(settings.get("concurrency"), Some(&"4".to_string()));

        Ok(())
    }

    #[test]
    fn test_has_settings() -> Result<()> {
        let conn = setup_test_db()?;

        assert!(!has_settings(&conn)?);

        set_setting(&conn, "theme", "dark")?;

        assert!(has_settings(&conn)?);

        Ok(())
    }

    // --- Clear History Tests ---

    #[test]
    fn test_clear_history_all() -> Result<()> {
        let conn = setup_test_db()?;

        let id1 = create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        update_job_error(&conn, id1, "complete", "")?;

        let id2 = create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        update_job_error(&conn, id2, "quarantined", "infected")?;

        // Create an active job with "pending" status (which is excluded from clear_history)
        let id3 = create_job(&conn, "session1", "/tmp/active.txt", 300, None)?;
        update_job_error(&conn, id3, "pending", "")?;

        clear_history(&conn, None)?;

        let job1 = get_job(&conn, id1)?;
        assert!(job1.is_none());

        let job2 = get_job(&conn, id2)?;
        assert!(job2.is_none());

        let job3 = get_job(&conn, id3)?;
        assert!(job3.is_some());

        Ok(())
    }

    #[test]
    fn test_clear_history_complete_only() -> Result<()> {
        let conn = setup_test_db()?;

        let id1 = create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        update_job_error(&conn, id1, "complete", "")?;

        let id2 = create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        update_job_error(&conn, id2, "quarantined", "infected")?;

        clear_history(&conn, Some("Complete"))?;

        let job1 = get_job(&conn, id1)?;
        assert!(job1.is_none());

        let job2 = get_job(&conn, id2)?;
        assert!(job2.is_some());

        Ok(())
    }

    #[test]
    fn test_clear_history_quarantined_only() -> Result<()> {
        let conn = setup_test_db()?;

        let id1 = create_job(&conn, "session1", "/tmp/file1.txt", 100, None)?;
        update_job_error(&conn, id1, "complete", "")?;

        let id2 = create_job(&conn, "session1", "/tmp/file2.txt", 200, None)?;
        update_job_error(&conn, id2, "quarantined", "infected")?;

        clear_history(&conn, Some("Quarantined"))?;

        let job1 = get_job(&conn, id1)?;
        assert!(job1.is_some());

        let job2 = get_job(&conn, id2)?;
        assert!(job2.is_none());

        Ok(())
    }
}


