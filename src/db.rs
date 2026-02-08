use crate::core::transfer::EndpointKind;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{Connection, params};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tracing::{debug, error, info, warn};

static ENCRYPTION_KEY: OnceLock<[u8; 32]> = OnceLock::new();
static STATE_DIR: OnceLock<PathBuf> = OnceLock::new();

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
    pub upload_status: Option<String>,
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
pub struct JobEventRow {
    pub event_type: String,
    pub message: String,
    pub created_at: String,
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

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct EndpointProfileRow {
    pub id: i64,
    pub name: String,
    pub kind: EndpointKind,
    pub config: Value,
    pub credential_ref: Option<String>,
    pub is_default_source: bool,
    pub is_default_destination: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct NewEndpointProfile {
    pub name: String,
    pub kind: EndpointKind,
    pub config: Value,
    pub credential_ref: Option<String>,
    pub is_default_source: bool,
    pub is_default_destination: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobTransferMetadata {
    pub source_endpoint_id: Option<i64>,
    pub destination_endpoint_id: Option<i64>,
    pub transfer_direction: Option<String>,
    pub conflict_policy: Option<String>,
    pub scan_policy: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WizardStatus {
    NotComplete,
    Complete,
    Skipped,
}

impl WizardStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotComplete => "not_complete",
            Self::Complete => "complete",
            Self::Skipped => "skipped",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "not_complete" => Some(Self::NotComplete),
            "complete" => Some(Self::Complete),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }
}

pub fn init_db(state_dir: &str) -> Result<Connection> {
    debug!("Initializing database in: {}", state_dir);
    let configured_state_dir = PathBuf::from(state_dir);
    if let Some(existing) = STATE_DIR.get() {
        if existing != &configured_state_dir {
            warn!(
                "State dir already set to {:?}; ignoring new value {:?}",
                existing, configured_state_dir
            );
        }
    } else {
        let _ = STATE_DIR.set(configured_state_dir.clone());
    }

    if !Path::new(state_dir).exists() {
        info!("Creating state directory: {}", state_dir);
        fs::create_dir_all(state_dir).context("create state dir")?;
    }
    let db_path = Path::new(state_dir).join("drifter.db");
    let conn = Connection::open(&db_path).context("open sqlite db")?;

    if let Err(e) = conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL DEFAULT 'legacy',
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            source_path TEXT NOT NULL,
            staged_path TEXT,
            size_bytes INTEGER NOT NULL,
            source_endpoint_id INTEGER,
            destination_endpoint_id INTEGER,
            transfer_direction TEXT,
            conflict_policy TEXT,
            scan_policy TEXT,
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
        CREATE TABLE IF NOT EXISTS endpoint_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL,
            config_json TEXT NOT NULL DEFAULT '{}',
            credential_ref TEXT,
            is_default_source INTEGER NOT NULL DEFAULT 0,
            is_default_destination INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        ",
    ) {
        error!("Failed to initialize database schema: {}", e);
        return Err(e.into());
    }

    // Migration for existing databases
    apply_optional_migration(
        &conn,
        "ALTER TABLE jobs ADD COLUMN session_id TEXT DEFAULT 'legacy'",
    )?;
    apply_optional_migration(&conn, "ALTER TABLE jobs ADD COLUMN s3_upload_id TEXT")?;
    // Migration for priority
    apply_optional_migration(
        &conn,
        "ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0",
    )?;
    // Migration for checksums
    apply_optional_migration(&conn, "ALTER TABLE jobs ADD COLUMN checksum TEXT")?;
    apply_optional_migration(&conn, "ALTER TABLE jobs ADD COLUMN remote_checksum TEXT")?;
    apply_optional_migration(&conn, "ALTER TABLE parts ADD COLUMN checksum_sha256 TEXT")?;
    // Migration for retries
    apply_optional_migration(
        &conn,
        "ALTER TABLE jobs ADD COLUMN retry_count INTEGER DEFAULT 0",
    )?;
    apply_optional_migration(&conn, "ALTER TABLE jobs ADD COLUMN next_retry_at TEXT")?;
    // Migration for timing
    apply_optional_migration(
        &conn,
        "ALTER TABLE jobs ADD COLUMN scan_duration_ms INTEGER",
    )?;
    apply_optional_migration(
        &conn,
        "ALTER TABLE jobs ADD COLUMN upload_duration_ms INTEGER",
    )?;
    // Migration for endpoint-based transfer metadata
    apply_optional_migration(
        &conn,
        "ALTER TABLE jobs ADD COLUMN source_endpoint_id INTEGER",
    )?;
    apply_optional_migration(
        &conn,
        "ALTER TABLE jobs ADD COLUMN destination_endpoint_id INTEGER",
    )?;
    apply_optional_migration(&conn, "ALTER TABLE jobs ADD COLUMN transfer_direction TEXT")?;
    apply_optional_migration(&conn, "ALTER TABLE jobs ADD COLUMN conflict_policy TEXT")?;
    apply_optional_migration(&conn, "ALTER TABLE jobs ADD COLUMN scan_policy TEXT")?;

    conn.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS idx_jobs_status_priority_id
            ON jobs(status, priority DESC, id ASC);
        CREATE INDEX IF NOT EXISTS idx_jobs_status_next_retry
            ON jobs(status, next_retry_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_status_id_desc
            ON jobs(status, id DESC);
        CREATE INDEX IF NOT EXISTS idx_jobs_session_id_id
            ON jobs(session_id, id);
        CREATE INDEX IF NOT EXISTS idx_jobs_source_endpoint_id
            ON jobs(source_endpoint_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_destination_endpoint_id
            ON jobs(destination_endpoint_id);
        CREATE INDEX IF NOT EXISTS idx_uploads_job_id
            ON uploads(job_id);
        CREATE INDEX IF NOT EXISTS idx_parts_upload_id
            ON parts(upload_id);
        CREATE INDEX IF NOT EXISTS idx_events_job_id
            ON events(job_id);
        CREATE INDEX IF NOT EXISTS idx_endpoint_profiles_kind
            ON endpoint_profiles(kind);
        CREATE INDEX IF NOT EXISTS idx_endpoint_profiles_default_source
            ON endpoint_profiles(is_default_source);
        CREATE INDEX IF NOT EXISTS idx_endpoint_profiles_default_destination
            ON endpoint_profiles(is_default_destination);
        ",
    )
    .context("create sqlite indexes")?;

    bootstrap_endpoint_profiles_from_legacy_settings(&conn)?;

    info!("Database initialized successfully at {:?}", db_path);
    Ok(conn)
}

fn apply_optional_migration(conn: &Connection, sql: &str) -> Result<()> {
    match conn.execute(sql, []) {
        Ok(_) => {
            info!("Applied migration: {}", sql);
            Ok(())
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("duplicate column name") {
                debug!("Skipping existing migration '{}': {}", sql, msg);
                Ok(())
            } else {
                Err(e).with_context(|| format!("Migration failed: {}", sql))
            }
        }
    }
}

pub const JOB_COLUMNS: &str = "id, session_id, created_at, status, source_path, size_bytes, staged_path, error, scan_status, upload_status, s3_upload_id, s3_key, priority, checksum, remote_checksum, retry_count, next_retry_at, scan_duration_ms, upload_duration_ms";

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
            upload_status: row.get(9)?,
            s3_upload_id: row.get(10)?,
            s3_key: row.get(11)?,
            priority: row.get(12).unwrap_or(0),
            checksum: row.get(13)?,
            remote_checksum: row.get(14)?,
            retry_count: row.get(15).unwrap_or(0),
            next_retry_at: row.get(16)?,
            scan_duration_ms: row.get(17)?,
            upload_duration_ms: row.get(18)?,
        })
    }
}

// Helper to map a row to a JobRow
fn map_job_row(row: &rusqlite::Row) -> rusqlite::Result<JobRow> {
    JobRow::try_from(row)
}

pub const ENDPOINT_PROFILE_COLUMNS: &str = "id, name, kind, config_json, credential_ref, is_default_source, is_default_destination, created_at, updated_at";

fn endpoint_kind_to_str(kind: EndpointKind) -> &'static str {
    match kind {
        EndpointKind::Local => "local",
        EndpointKind::S3 => "s3",
    }
}

fn parse_endpoint_kind(raw: &str) -> Result<EndpointKind> {
    match raw {
        "local" => Ok(EndpointKind::Local),
        "s3" => Ok(EndpointKind::S3),
        other => anyhow::bail!("unsupported endpoint kind '{}'", other),
    }
}

fn endpoint_profile_from_row(row: &rusqlite::Row) -> Result<EndpointProfileRow> {
    let kind_raw: String = row.get(2)?;
    let config_json: String = row.get(3)?;

    let kind = parse_endpoint_kind(&kind_raw)?;
    let config = serde_json::from_str::<Value>(&config_json)
        .with_context(|| format!("parse endpoint config_json '{}'", config_json))?;

    Ok(EndpointProfileRow {
        id: row.get(0)?,
        name: row.get(1)?,
        kind,
        config,
        credential_ref: row.get(4)?,
        is_default_source: row.get::<_, i64>(5)? != 0,
        is_default_destination: row.get::<_, i64>(6)? != 0,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn endpoint_profiles_table_exists(conn: &Connection) -> Result<bool> {
    let mut stmt = conn.prepare(
        "SELECT 1
         FROM sqlite_master
         WHERE type = 'table' AND name = 'endpoint_profiles'
         LIMIT 1",
    )?;
    let mut rows = stmt.query([])?;
    Ok(rows.next()?.is_some())
}

fn has_secret_value(conn: &Connection, key: &str) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT value FROM secrets WHERE key = ? LIMIT 1")?;
    let mut rows = stmt.query(params![key])?;
    if let Some(row) = rows.next()? {
        let stored_value: String = row.get(0)?;
        Ok(!stored_value.trim().is_empty())
    } else {
        Ok(false)
    }
}

fn bootstrap_endpoint_profiles_from_legacy_settings(conn: &Connection) -> Result<()> {
    if count_endpoint_profiles(conn)? > 0 {
        return Ok(());
    }

    let settings = load_all_settings(conn)?;
    let get_setting = |key: &str| {
        settings
            .get(key)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };

    let bucket = get_setting("s3_bucket");
    let region = get_setting("s3_region");
    let endpoint = get_setting("s3_endpoint");
    let prefix = get_setting("s3_prefix");
    let access_key = get_setting("s3_access_key");
    let has_secret = has_secret_value(conn, "s3_secret")?;

    let has_legacy_s3 = bucket.is_some()
        || region.is_some()
        || endpoint.is_some()
        || prefix.is_some()
        || access_key.is_some()
        || has_secret;

    let local_profile = NewEndpointProfile {
        name: "Local Filesystem".to_string(),
        kind: EndpointKind::Local,
        config: serde_json::json!({ "root": "." }),
        credential_ref: None,
        is_default_source: true,
        is_default_destination: !has_legacy_s3,
    };
    create_endpoint_profile(conn, &local_profile)?;

    let mut config = Map::new();
    if let Some(v) = bucket {
        config.insert("bucket".to_string(), Value::String(v));
    }
    if let Some(v) = region {
        config.insert("region".to_string(), Value::String(v));
    }
    if let Some(v) = endpoint {
        config.insert("endpoint".to_string(), Value::String(v));
    }
    if let Some(v) = prefix {
        config.insert("prefix".to_string(), Value::String(v));
    }
    if let Some(v) = access_key {
        config.insert("access_key".to_string(), Value::String(v));
    }

    let credential_ref = if has_secret {
        Some("s3_secret".to_string())
    } else {
        None
    };

    if has_legacy_s3 {
        let profile = NewEndpointProfile {
            name: "Legacy S3".to_string(),
            kind: EndpointKind::S3,
            config: Value::Object(config),
            credential_ref,
            is_default_source: false,
            is_default_destination: true,
        };

        create_endpoint_profile(conn, &profile)?;
        info!("Bootstrapped local and legacy S3 endpoint profiles");
    } else {
        info!("Bootstrapped local endpoint profile");
    }
    Ok(())
}

pub fn count_endpoint_profiles(conn: &Connection) -> Result<i64> {
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM endpoint_profiles", [], |row| {
        row.get(0)
    })?;
    Ok(count)
}

pub fn create_endpoint_profile(conn: &Connection, profile: &NewEndpointProfile) -> Result<i64> {
    let now = Utc::now().to_rfc3339();
    let config_json = serde_json::to_string(&profile.config)?;
    conn.execute(
        "INSERT INTO endpoint_profiles (
            name, kind, config_json, credential_ref, is_default_source, is_default_destination, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            profile.name.as_str(),
            endpoint_kind_to_str(profile.kind),
            config_json,
            profile.credential_ref.as_deref(),
            if profile.is_default_source { 1 } else { 0 },
            if profile.is_default_destination { 1 } else { 0 },
            now,
            now
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

#[allow(dead_code)]
pub fn get_endpoint_profile(
    conn: &Connection,
    endpoint_id: i64,
) -> Result<Option<EndpointProfileRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM endpoint_profiles WHERE id = ?",
        ENDPOINT_PROFILE_COLUMNS
    ))?;
    let mut rows = stmt.query(params![endpoint_id])?;
    if let Some(row) = rows.next()? {
        Ok(Some(endpoint_profile_from_row(row)?))
    } else {
        Ok(None)
    }
}

#[allow(dead_code)]
pub fn get_endpoint_profile_by_name(
    conn: &Connection,
    name: &str,
) -> Result<Option<EndpointProfileRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM endpoint_profiles WHERE name = ?",
        ENDPOINT_PROFILE_COLUMNS
    ))?;
    let mut rows = stmt.query(params![name])?;
    if let Some(row) = rows.next()? {
        Ok(Some(endpoint_profile_from_row(row)?))
    } else {
        Ok(None)
    }
}

#[allow(dead_code)]
pub fn list_endpoint_profiles(conn: &Connection) -> Result<Vec<EndpointProfileRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM endpoint_profiles ORDER BY id ASC",
        ENDPOINT_PROFILE_COLUMNS
    ))?;
    let mut rows = stmt.query([])?;
    let mut profiles = Vec::new();
    while let Some(row) = rows.next()? {
        profiles.push(endpoint_profile_from_row(row)?);
    }
    Ok(profiles)
}

pub fn get_default_source_endpoint_profile(
    conn: &Connection,
) -> Result<Option<EndpointProfileRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM endpoint_profiles WHERE is_default_source = 1 ORDER BY id ASC LIMIT 1",
        ENDPOINT_PROFILE_COLUMNS
    ))?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        Ok(Some(endpoint_profile_from_row(row)?))
    } else {
        Ok(None)
    }
}

pub fn get_default_destination_endpoint_profile(
    conn: &Connection,
) -> Result<Option<EndpointProfileRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM endpoint_profiles WHERE is_default_destination = 1 ORDER BY id ASC LIMIT 1",
        ENDPOINT_PROFILE_COLUMNS
    ))?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        Ok(Some(endpoint_profile_from_row(row)?))
    } else {
        Ok(None)
    }
}

pub fn update_endpoint_profile(
    conn: &Connection,
    endpoint_id: i64,
    config: &Value,
    credential_ref: Option<&str>,
    is_default_source: bool,
    is_default_destination: bool,
) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    let config_json = serde_json::to_string(config)?;
    conn.execute(
        "UPDATE endpoint_profiles
         SET config_json = ?, credential_ref = ?, is_default_source = ?, is_default_destination = ?, updated_at = ?
         WHERE id = ?",
        params![
            config_json,
            credential_ref,
            if is_default_source { 1 } else { 0 },
            if is_default_destination { 1 } else { 0 },
            now,
            endpoint_id
        ],
    )?;
    Ok(())
}

pub fn rename_endpoint_profile(conn: &Connection, endpoint_id: i64, name: &str) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "UPDATE endpoint_profiles
         SET name = ?, updated_at = ?
         WHERE id = ?",
        params![name, now, endpoint_id],
    )?;
    Ok(())
}

pub fn delete_endpoint_profile(conn: &Connection, endpoint_id: i64) -> Result<()> {
    let mut stmt = conn.prepare("SELECT kind FROM endpoint_profiles WHERE id = ?")?;
    let mut rows = stmt.query(params![endpoint_id])?;
    if let Some(row) = rows.next()? {
        let kind_raw: String = row.get(0)?;
        if parse_endpoint_kind(&kind_raw)? == EndpointKind::Local {
            anyhow::bail!("cannot delete local endpoint profile");
        }
    }

    conn.execute(
        "DELETE FROM endpoint_profiles WHERE id = ?",
        params![endpoint_id],
    )?;
    Ok(())
}

pub fn sync_default_destination_s3_profile(
    conn: &Connection,
    bucket: Option<&str>,
    prefix: Option<&str>,
    region: Option<&str>,
    endpoint: Option<&str>,
    access_key: Option<&str>,
    credential_ref: Option<&str>,
) -> Result<()> {
    if !endpoint_profiles_table_exists(conn)? {
        return Ok(());
    }

    let trim_non_empty = |v: Option<&str>| {
        v.map(str::trim)
            .filter(|v| !v.is_empty())
            .map(std::string::ToString::to_string)
    };

    let mut config = Map::new();
    if let Some(v) = trim_non_empty(bucket) {
        config.insert("bucket".to_string(), Value::String(v));
    }
    if let Some(v) = trim_non_empty(prefix) {
        config.insert("prefix".to_string(), Value::String(v));
    }
    if let Some(v) = trim_non_empty(region) {
        config.insert("region".to_string(), Value::String(v));
    }
    if let Some(v) = trim_non_empty(endpoint) {
        config.insert("endpoint".to_string(), Value::String(v));
    }
    if let Some(v) = trim_non_empty(access_key) {
        config.insert("access_key".to_string(), Value::String(v));
    }

    let config_json = Value::Object(config);
    let has_s3_data = config_json.as_object().is_some_and(|obj| !obj.is_empty())
        || trim_non_empty(credential_ref).is_some();

    let mut stmt = conn.prepare(
        "SELECT id, kind
         FROM endpoint_profiles
         WHERE is_default_destination = 1
         ORDER BY id ASC
         LIMIT 1",
    )?;
    let mut rows = stmt.query([])?;
    let default_destination = if let Some(row) = rows.next()? {
        Some((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    } else {
        None
    };
    let now = Utc::now().to_rfc3339();

    match default_destination {
        Some((id, kind)) if kind == endpoint_kind_to_str(EndpointKind::S3) => {
            conn.execute(
                "UPDATE endpoint_profiles
                 SET config_json = ?, credential_ref = ?, updated_at = ?
                 WHERE id = ?",
                params![
                    serde_json::to_string(&config_json)?,
                    trim_non_empty(credential_ref),
                    now,
                    id
                ],
            )?;
        }
        Some((id, _kind)) if has_s3_data => {
            conn.execute(
                "UPDATE endpoint_profiles
                 SET is_default_destination = 0, updated_at = ?
                 WHERE id = ?",
                params![now, id],
            )?;
            let mut existing_stmt = conn.prepare(
                "SELECT id
                 FROM endpoint_profiles
                 WHERE kind = 's3'
                 ORDER BY id ASC
                 LIMIT 1",
            )?;
            let mut existing_rows = existing_stmt.query([])?;
            if let Some(row) = existing_rows.next()? {
                let existing_id: i64 = row.get(0)?;
                conn.execute(
                    "UPDATE endpoint_profiles
                     SET config_json = ?, credential_ref = ?, is_default_destination = 1, updated_at = ?
                     WHERE id = ?",
                    params![
                        serde_json::to_string(&config_json)?,
                        trim_non_empty(credential_ref),
                        now,
                        existing_id
                    ],
                )?;
            } else {
                create_endpoint_profile(
                    conn,
                    &NewEndpointProfile {
                        name: "Primary S3".to_string(),
                        kind: EndpointKind::S3,
                        config: config_json,
                        credential_ref: trim_non_empty(credential_ref),
                        is_default_source: false,
                        is_default_destination: true,
                    },
                )?;
            }
        }
        None if has_s3_data => {
            let mut existing_stmt = conn.prepare(
                "SELECT id
                 FROM endpoint_profiles
                 WHERE kind = 's3'
                 ORDER BY id ASC
                 LIMIT 1",
            )?;
            let mut existing_rows = existing_stmt.query([])?;
            if let Some(row) = existing_rows.next()? {
                let existing_id: i64 = row.get(0)?;
                conn.execute(
                    "UPDATE endpoint_profiles
                     SET config_json = ?, credential_ref = ?, is_default_destination = 1, updated_at = ?
                     WHERE id = ?",
                    params![
                        serde_json::to_string(&config_json)?,
                        trim_non_empty(credential_ref),
                        now,
                        existing_id
                    ],
                )?;
            } else {
                create_endpoint_profile(
                    conn,
                    &NewEndpointProfile {
                        name: "Primary S3".to_string(),
                        kind: EndpointKind::S3,
                        config: config_json,
                        credential_ref: trim_non_empty(credential_ref),
                        is_default_source: false,
                        is_default_destination: true,
                    },
                )?;
            }
        }
        _ => {}
    }

    Ok(())
}

pub fn assign_default_transfer_metadata(conn: &Connection, job_id: i64) -> Result<()> {
    let source = match get_default_source_endpoint_profile(conn) {
        Ok(v) => v,
        Err(e) if e.to_string().contains("no such table: endpoint_profiles") => {
            debug!(
                "Skipping transfer metadata assignment for job {}: {}",
                job_id, e
            );
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let destination = match get_default_destination_endpoint_profile(conn) {
        Ok(v) => v,
        Err(e) if e.to_string().contains("no such table: endpoint_profiles") => {
            debug!(
                "Skipping transfer metadata assignment for job {}: {}",
                job_id, e
            );
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    let (transfer_direction, scan_policy) = match (&source, &destination) {
        (Some(src), Some(dst)) => {
            let direction = match (src.kind, dst.kind) {
                (EndpointKind::Local, EndpointKind::S3) => Some("local_to_s3"),
                (EndpointKind::S3, EndpointKind::Local) => Some("s3_to_local"),
                (EndpointKind::S3, EndpointKind::S3) => Some("s3_to_s3"),
                _ => None,
            };
            let scan_policy = if direction == Some("local_to_s3") {
                Some("upload_only")
            } else {
                Some("never")
            };
            (
                direction.map(std::string::ToString::to_string),
                scan_policy.map(std::string::ToString::to_string),
            )
        }
        _ => (None, None),
    };

    let metadata = JobTransferMetadata {
        source_endpoint_id: source.map(|p| p.id),
        destination_endpoint_id: destination.map(|p| p.id),
        transfer_direction,
        conflict_policy: Some("overwrite".to_string()),
        scan_policy,
    };

    update_job_transfer_metadata(conn, job_id, &metadata)?;
    Ok(())
}

pub fn list_active_jobs(conn: &Connection, limit: i64) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM jobs 
         WHERE status NOT IN ('complete', 'quarantined', 'quarantined_removed', 'cancelled') 
         OR datetime(created_at) > datetime('now', '-15 seconds')
         ORDER BY priority DESC, id DESC LIMIT ?",
        JOB_COLUMNS
    ))?;
    let rows = stmt
        .query_map(params![limit], map_job_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn list_history_jobs(
    conn: &Connection,
    limit: i64,
    filter: Option<&str>,
) -> Result<Vec<JobRow>> {
    let sql = match filter {
        Some("Complete") => format!(
            "SELECT {} FROM jobs WHERE status = 'complete' ORDER BY id DESC LIMIT ?",
            JOB_COLUMNS
        ),
        Some("Quarantined") => format!(
            "SELECT {} FROM jobs WHERE status IN ('quarantined', 'quarantined_removed') ORDER BY id DESC LIMIT ?",
            JOB_COLUMNS
        ),
        _ => format!(
            "SELECT {} FROM jobs WHERE status IN ('complete', 'quarantined', 'quarantined_removed', 'cancelled') ORDER BY id DESC LIMIT ?",
            JOB_COLUMNS
        ),
    };

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params![limit], map_job_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn list_quarantined_jobs(conn: &Connection, limit: i64) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM jobs WHERE status = 'quarantined' ORDER BY id DESC LIMIT ?",
        JOB_COLUMNS
    ))?;
    let rows = stmt
        .query_map(params![limit], map_job_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn update_job_retry_state(
    conn: &Connection,
    job_id: i64,
    retry_count: i64,
    next_retry_at: Option<&str>,
    status: &str,
    error: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET retry_count = ?, next_retry_at = ?, status = ?, error = ? WHERE id = ?",
        params![retry_count, next_retry_at, status, error, job_id],
    )?;
    Ok(())
}

pub fn list_retryable_jobs(conn: &Connection) -> Result<Vec<JobRow>> {
    let now = Utc::now().to_rfc3339();
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM jobs 
         WHERE status = 'retry_pending' AND next_retry_at <= ?
         ORDER BY priority DESC, id ASC",
        JOB_COLUMNS
    ))?;
    let rows = stmt
        .query_map(params![now], map_job_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn next_retry_due_at(conn: &Connection) -> Result<Option<chrono::DateTime<Utc>>> {
    let mut stmt = conn.prepare(
        "SELECT next_retry_at
         FROM jobs
         WHERE status = 'retry_pending' AND next_retry_at IS NOT NULL
         ORDER BY next_retry_at ASC
         LIMIT 1",
    )?;

    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let next_retry_at: String = row.get(0)?;
        let parsed = chrono::DateTime::parse_from_rfc3339(&next_retry_at)
            .with_context(|| format!("parse next_retry_at '{}'", next_retry_at))?;
        Ok(Some(parsed.with_timezone(&Utc)))
    } else {
        Ok(None)
    }
}

pub fn update_job_upload_id(conn: &Connection, job_id: i64, upload_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET s3_upload_id = ? WHERE id = ?",
        params![upload_id, job_id],
    )?;
    Ok(())
}

pub fn update_job_transfer_metadata(
    conn: &Connection,
    job_id: i64,
    metadata: &JobTransferMetadata,
) -> Result<()> {
    conn.execute(
        "UPDATE jobs
         SET source_endpoint_id = ?,
             destination_endpoint_id = ?,
             transfer_direction = ?,
             conflict_policy = ?,
             scan_policy = ?
         WHERE id = ?",
        params![
            metadata.source_endpoint_id,
            metadata.destination_endpoint_id,
            metadata.transfer_direction,
            metadata.conflict_policy,
            metadata.scan_policy,
            job_id
        ],
    )?;
    Ok(())
}

pub fn get_job_transfer_metadata(
    conn: &Connection,
    job_id: i64,
) -> Result<Option<JobTransferMetadata>> {
    let mut stmt = conn.prepare(
        "SELECT source_endpoint_id, destination_endpoint_id, transfer_direction, conflict_policy, scan_policy
         FROM jobs
         WHERE id = ?",
    )?;
    let mut rows = stmt.query(params![job_id])?;
    if let Some(row) = rows.next()? {
        Ok(Some(JobTransferMetadata {
            source_endpoint_id: row.get(0)?,
            destination_endpoint_id: row.get(1)?,
            transfer_direction: row.get(2)?,
            conflict_policy: row.get(3)?,
            scan_policy: row.get(4)?,
        }))
    } else {
        Ok(None)
    }
}

pub fn create_job(
    conn: &Connection,
    session_id: &str,
    source_path: &str,
    size_bytes: i64,
    s3_key: Option<&str>,
) -> Result<i64> {
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
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM jobs WHERE session_id = ? ORDER BY id ASC",
        JOB_COLUMNS
    ))?;
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

pub fn update_job_staged_path(conn: &Connection, job_id: i64, staged_path: &str) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET staged_path = ? WHERE id = ?",
        params![staged_path, job_id],
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
        if status.as_deref() == Some("completed")
            || status.as_deref() == Some("clean")
            || status.as_deref() == Some("scanned")
        {
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
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "DELETE FROM parts WHERE upload_id IN (SELECT id FROM uploads WHERE job_id = ?)",
        params![job_id],
    )?;
    tx.execute("DELETE FROM uploads WHERE job_id = ?", params![job_id])?;
    tx.execute("DELETE FROM events WHERE job_id = ?", params![job_id])?;
    tx.execute("DELETE FROM jobs WHERE id = ?", params![job_id])?;
    tx.commit()?;
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
        if status.as_deref() == Some("completed")
            || status.as_deref() == Some("clean")
            || status.as_deref() == Some("scanned")
        {
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
    insert_event(
        conn,
        job_id,
        "resume",
        &format!("job resumed to {}", new_status),
    )?;
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
// Always excludes "active" jobs (pending, scanning, uploading, transferring)
pub fn clear_history(conn: &Connection, filter: Option<&str>) -> Result<()> {
    // 1. Identify IDs to delete
    let mut query = "SELECT id FROM jobs WHERE status NOT IN ('pending', 'scanning', 'uploading', 'transferring', 'failed_retryable')".to_string();

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

    let tx = conn.unchecked_transaction()?;
    for id in ids {
        // Delete parts
        tx.execute(
            "DELETE FROM parts WHERE upload_id IN (SELECT id FROM uploads WHERE job_id = ?)",
            params![id],
        )?;
        // Delete uploads
        tx.execute("DELETE FROM uploads WHERE job_id = ?", params![id])?;
        // Delete events
        tx.execute("DELETE FROM events WHERE job_id = ?", params![id])?;
        // Delete job
        tx.execute("DELETE FROM jobs WHERE id = ?", params![id])?;
    }
    tx.commit()?;

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

pub fn list_job_events(conn: &Connection, job_id: i64, limit: usize) -> Result<Vec<JobEventRow>> {
    let mut stmt = conn.prepare(
        "SELECT event_type, message, created_at
         FROM events
         WHERE job_id = ?
         ORDER BY id DESC
         LIMIT ?",
    )?;
    let rows = stmt.query_map(params![job_id, limit as i64], |row| {
        Ok(JobEventRow {
            event_type: row.get(0)?,
            message: row.get(1)?,
            created_at: row.get(2)?,
        })
    })?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn update_scan_status(
    conn: &Connection,
    job_id: i64,
    status: &str,
    global_status: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET scan_status = ?, status = ? WHERE id = ?",
        params![status, global_status, job_id],
    )?;
    Ok(())
}

pub fn update_upload_status(
    conn: &Connection,
    job_id: i64,
    status: &str,
    global_status: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET upload_status = ?, status = ? WHERE id = ?",
        params![status, global_status, job_id],
    )?;
    Ok(())
}

pub fn update_job_checksums(
    conn: &Connection,
    job_id: i64,
    local: Option<&str>,
    remote: Option<&str>,
) -> Result<()> {
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
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM jobs WHERE status = ? ORDER BY priority DESC, id ASC LIMIT 1",
        JOB_COLUMNS
    ))?;
    let mut rows = stmt.query_map(params![current_status], map_job_row)?;

    if let Some(row) = rows.next() {
        Ok(Some(row?))
    } else {
        Ok(None)
    }
}

pub fn get_job(conn: &Connection, job_id: i64) -> Result<Option<JobRow>> {
    let mut stmt = conn.prepare(&format!("SELECT {} FROM jobs WHERE id = ?", JOB_COLUMNS))?;
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

const XOR_KEY_LEGACY: &[u8] = b"drifter-secret-pad-123";

fn deobfuscate_legacy(input: &str) -> Result<String> {
    let bytes = hex::decode(input).context("decode hex secret")?;
    let mut result = Vec::with_capacity(bytes.len());
    for (i, &b) in bytes.iter().enumerate() {
        result.push(b ^ XOR_KEY_LEGACY[i % XOR_KEY_LEGACY.len()]);
    }
    String::from_utf8(result).context("parse utf8 secret")
}

fn get_or_create_key() -> Result<[u8; 32]> {
    if let Some(key) = ENCRYPTION_KEY.get() {
        return Ok(*key);
    }

    let keyfile = state_dir().join("keyfile");
    let keyfile = keyfile.as_path();
    let key = if keyfile.exists() {
        let data = fs::read(keyfile).context("read keyfile")?;
        match <[u8; 32]>::try_from(data.as_slice()) {
            Ok(key) => key,
            Err(_) => {
                warn!("Keyfile has unexpected length {}, regenerating", data.len());
                generate_and_write_keyfile(keyfile)?
            }
        }
    } else {
        generate_and_write_keyfile(keyfile)?
    };

    let _ = ENCRYPTION_KEY.set(key);
    Ok(key)
}

fn state_dir() -> PathBuf {
    STATE_DIR
        .get()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("./state"))
}

fn generate_and_write_keyfile(keyfile: &Path) -> Result<[u8; 32]> {
    let key: [u8; 32] = rand::random();
    if let Some(parent) = keyfile.parent() {
        fs::create_dir_all(parent).context("create state dir for keyfile")?;
    }
    fs::write(keyfile, key).context("write keyfile")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(keyfile, fs::Permissions::from_mode(0o600))
            .context("set keyfile permissions")?;
    }
    info!("Generated new encryption keyfile");
    Ok(key)
}

fn encrypt_secret(plaintext: &str) -> Result<String> {
    let key = get_or_create_key()?;
    let cipher = Aes256Gcm::new_from_slice(&key).context("create AES cipher")?;
    let nonce_bytes: [u8; 12] = rand::random();
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| anyhow::anyhow!("encryption failed: {}", e))?;
    let mut blob = Vec::with_capacity(12 + ciphertext.len());
    blob.extend_from_slice(&nonce_bytes);
    blob.extend_from_slice(&ciphertext);
    Ok(hex::encode(blob))
}

fn decrypt_secret(hex_blob: &str) -> Result<String> {
    let blob = hex::decode(hex_blob).context("decode hex secret")?;
    if blob.len() < 13 {
        anyhow::bail!("ciphertext too short");
    }
    let (nonce_bytes, ciphertext) = blob.split_at(12);
    let key = get_or_create_key()?;
    let cipher = Aes256Gcm::new_from_slice(&key).context("create AES cipher")?;
    let nonce = Nonce::from_slice(nonce_bytes);
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow::anyhow!("decryption failed: {}", e))?;
    String::from_utf8(plaintext).context("parse utf8 secret")
}

pub fn set_secret(conn: &Connection, key: &str, value: &str) -> Result<()> {
    let val = encrypt_secret(value)?;
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
        // Try AES decryption first; fall back to legacy XOR for migration
        match decrypt_secret(&val) {
            Ok(plaintext) => Ok(Some(plaintext)),
            Err(_) => {
                let plaintext = deobfuscate_legacy(&val)?;
                // Re-encrypt with AES and update the row
                let new_val = encrypt_secret(&plaintext)?;
                conn.execute(
                    "UPDATE secrets SET value = ? WHERE key = ?",
                    params![new_val, key],
                )?;
                info!("Migrated secret '{}' from XOR to AES-GCM", key);
                Ok(Some(plaintext))
            }
        }
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

pub fn get_wizard_status(conn: &Connection) -> Result<WizardStatus> {
    const WIZARD_STATUS_KEY: &str = "wizard_status";

    if let Some(raw) = get_setting(conn, WIZARD_STATUS_KEY)? {
        if let Some(parsed) = WizardStatus::from_str(raw.trim()) {
            return Ok(parsed);
        }
        warn!(
            "Unknown wizard_status '{}'; treating wizard as not complete",
            raw
        );
        return Ok(WizardStatus::NotComplete);
    }

    // Legacy behavior fallback: if any settings exist, assume setup already completed.
    if has_settings(conn)? {
        Ok(WizardStatus::Complete)
    } else {
        Ok(WizardStatus::NotComplete)
    }
}

pub fn set_wizard_status(conn: &Connection, status: WizardStatus) -> Result<()> {
    const WIZARD_STATUS_KEY: &str = "wizard_status";
    set_setting(conn, WIZARD_STATUS_KEY, status.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_columns_count() {
        let count = JOB_COLUMNS.split(',').count();
        assert_eq!(count, 19, "JOB_COLUMNS should have 19 fields");
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
                source_endpoint_id INTEGER,
                destination_endpoint_id INTEGER,
                transfer_direction TEXT,
                conflict_policy TEXT,
                scan_policy TEXT,
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
            CREATE TABLE endpoint_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                kind TEXT NOT NULL,
                config_json TEXT NOT NULL DEFAULT '{}',
                credential_ref TEXT,
                is_default_source INTEGER NOT NULL DEFAULT 0,
                is_default_destination INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            ",
        )?;
        Ok(conn)
    }

    // --- Job CRUD Tests ---

    #[test]
    fn test_create_job_success() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(
            &conn,
            "test-session",
            "/tmp/file.txt",
            1024,
            Some("s3-key.txt"),
        )?;

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
        update_job_staged(&conn, job_id, "/data/file.txt", "queued")?;

        let job = get_job(&conn, job_id)?.expect("Job should exist");
        assert_eq!(job.staged_path, Some("/data/file.txt".to_string()));
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
        update_job_retry_state(
            &conn,
            job_id,
            3,
            Some(next_retry),
            "retry_pending",
            "Temporary failure",
        )?;

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

    #[test]
    fn test_update_and_get_job_transfer_metadata() -> Result<()> {
        let conn = setup_test_db()?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        let metadata = JobTransferMetadata {
            source_endpoint_id: Some(10),
            destination_endpoint_id: Some(20),
            transfer_direction: Some("s3_to_local".to_string()),
            conflict_policy: Some("overwrite".to_string()),
            scan_policy: Some("never".to_string()),
        };
        update_job_transfer_metadata(&conn, job_id, &metadata)?;

        let loaded = get_job_transfer_metadata(&conn, job_id)?
            .expect("Transfer metadata should exist for created job");
        assert_eq!(loaded, metadata);

        Ok(())
    }

    #[test]
    fn test_assign_default_transfer_metadata_local_to_s3() -> Result<()> {
        let conn = setup_test_db()?;

        let local_id = create_endpoint_profile(
            &conn,
            &NewEndpointProfile {
                name: "Local FS".to_string(),
                kind: EndpointKind::Local,
                config: serde_json::json!({ "root": "." }),
                credential_ref: None,
                is_default_source: true,
                is_default_destination: false,
            },
        )?;
        let s3_id = create_endpoint_profile(
            &conn,
            &NewEndpointProfile {
                name: "Archive S3".to_string(),
                kind: EndpointKind::S3,
                config: serde_json::json!({ "bucket": "archive", "region": "us-east-1" }),
                credential_ref: Some("s3_secret".to_string()),
                is_default_source: false,
                is_default_destination: true,
            },
        )?;

        let job_id = create_job(&conn, "session1", "/tmp/file.txt", 100, None)?;
        assign_default_transfer_metadata(&conn, job_id)?;

        let loaded = get_job_transfer_metadata(&conn, job_id)?
            .expect("Transfer metadata should exist for created job");
        assert_eq!(loaded.source_endpoint_id, Some(local_id));
        assert_eq!(loaded.destination_endpoint_id, Some(s3_id));
        assert_eq!(loaded.transfer_direction, Some("local_to_s3".to_string()));
        assert_eq!(loaded.conflict_policy, Some("overwrite".to_string()));
        assert_eq!(loaded.scan_policy, Some("upload_only".to_string()));

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
            assert!(!stored.is_empty());
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

    #[test]
    fn test_get_wizard_status_defaults_not_complete_without_settings() -> Result<()> {
        let conn = setup_test_db()?;
        assert_eq!(get_wizard_status(&conn)?, WizardStatus::NotComplete);
        Ok(())
    }

    #[test]
    fn test_get_wizard_status_defaults_complete_for_legacy_settings() -> Result<()> {
        let conn = setup_test_db()?;
        set_setting(&conn, "theme", "nord")?;
        assert_eq!(get_wizard_status(&conn)?, WizardStatus::Complete);
        Ok(())
    }

    #[test]
    fn test_set_and_get_wizard_status() -> Result<()> {
        let conn = setup_test_db()?;

        set_wizard_status(&conn, WizardStatus::NotComplete)?;
        assert_eq!(get_wizard_status(&conn)?, WizardStatus::NotComplete);

        set_wizard_status(&conn, WizardStatus::Skipped)?;
        assert_eq!(get_wizard_status(&conn)?, WizardStatus::Skipped);

        set_wizard_status(&conn, WizardStatus::Complete)?;
        assert_eq!(get_wizard_status(&conn)?, WizardStatus::Complete);
        Ok(())
    }

    // --- Endpoint Profile Tests ---

    #[test]
    fn test_create_and_list_endpoint_profiles() -> Result<()> {
        let conn = setup_test_db()?;

        let endpoint_id = create_endpoint_profile(
            &conn,
            &NewEndpointProfile {
                name: "Primary S3".to_string(),
                kind: EndpointKind::S3,
                config: serde_json::json!({
                    "bucket": "files-prod",
                    "region": "us-east-1",
                    "prefix": "uploads/"
                }),
                credential_ref: Some("s3_secret".to_string()),
                is_default_source: false,
                is_default_destination: true,
            },
        )?;

        let endpoint = get_endpoint_profile(&conn, endpoint_id)?.expect("Endpoint should exist");
        assert_eq!(endpoint.name, "Primary S3");
        assert_eq!(endpoint.kind, EndpointKind::S3);
        assert_eq!(endpoint.credential_ref, Some("s3_secret".to_string()));
        assert!(endpoint.is_default_destination);
        assert!(!endpoint.is_default_source);

        let listed = list_endpoint_profiles(&conn)?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, endpoint_id);
        assert_eq!(
            listed[0].config.get("bucket"),
            Some(&Value::String("files-prod".to_string()))
        );

        Ok(())
    }

    #[test]
    fn test_get_and_update_endpoint_profile_by_name() -> Result<()> {
        let conn = setup_test_db()?;

        let endpoint_id = create_endpoint_profile(
            &conn,
            &NewEndpointProfile {
                name: "Secondary S3".to_string(),
                kind: EndpointKind::S3,
                config: serde_json::json!({
                    "bucket": "secondary-bucket",
                    "region": "us-east-1"
                }),
                credential_ref: Some("secondary_secret_old".to_string()),
                is_default_source: false,
                is_default_destination: false,
            },
        )?;

        let endpoint =
            get_endpoint_profile_by_name(&conn, "Secondary S3")?.expect("Endpoint should exist");
        assert_eq!(endpoint.id, endpoint_id);
        assert_eq!(
            endpoint.config.get("bucket"),
            Some(&Value::String("secondary-bucket".to_string()))
        );

        let updated = serde_json::json!({
            "bucket": "secondary-bucket-updated",
            "region": "us-east-2",
            "prefix": "x/"
        });
        update_endpoint_profile(
            &conn,
            endpoint_id,
            &updated,
            Some("secondary_secret_new"),
            true,
            false,
        )?;

        let updated_row = get_endpoint_profile_by_name(&conn, "Secondary S3")?
            .expect("Updated endpoint should exist");
        assert_eq!(
            updated_row.config.get("bucket"),
            Some(&Value::String("secondary-bucket-updated".to_string()))
        );
        assert_eq!(
            updated_row.credential_ref,
            Some("secondary_secret_new".to_string())
        );
        assert!(updated_row.is_default_source);
        assert!(!updated_row.is_default_destination);

        Ok(())
    }

    #[test]
    fn test_bootstrap_endpoint_profiles_from_legacy_settings() -> Result<()> {
        let conn = setup_test_db()?;

        set_setting(&conn, "s3_bucket", "legacy-bucket")?;
        set_setting(&conn, "s3_region", "us-east-1")?;
        set_setting(&conn, "s3_prefix", "incoming/")?;
        set_setting(&conn, "s3_access_key", "AKIA_TEST")?;
        set_secret(&conn, "s3_secret", "legacy-secret")?;

        bootstrap_endpoint_profiles_from_legacy_settings(&conn)?;

        let profiles = list_endpoint_profiles(&conn)?;
        assert_eq!(profiles.len(), 2);

        let local = profiles
            .iter()
            .find(|p| p.kind == EndpointKind::Local)
            .expect("Local profile should be created during bootstrap");
        let profile = profiles
            .iter()
            .find(|p| p.name == "Legacy S3")
            .expect("Legacy S3 profile should be created during bootstrap");

        assert_eq!(local.name, "Local Filesystem");
        assert!(local.is_default_source);
        assert!(!local.is_default_destination);
        assert_eq!(profile.kind, EndpointKind::S3);
        assert_eq!(profile.credential_ref, Some("s3_secret".to_string()));
        assert_eq!(
            profile.config.get("bucket"),
            Some(&Value::String("legacy-bucket".to_string()))
        );
        assert_eq!(
            profile.config.get("region"),
            Some(&Value::String("us-east-1".to_string()))
        );

        // Idempotent on subsequent runs.
        bootstrap_endpoint_profiles_from_legacy_settings(&conn)?;
        assert_eq!(count_endpoint_profiles(&conn)?, 2);

        Ok(())
    }

    #[test]
    fn test_bootstrap_endpoint_profiles_without_legacy_settings() -> Result<()> {
        let conn = setup_test_db()?;

        bootstrap_endpoint_profiles_from_legacy_settings(&conn)?;

        let profiles = list_endpoint_profiles(&conn)?;
        assert_eq!(profiles.len(), 1);
        let local = &profiles[0];
        assert_eq!(local.kind, EndpointKind::Local);
        assert_eq!(local.name, "Local Filesystem");
        assert!(local.is_default_source);
        assert!(local.is_default_destination);

        Ok(())
    }

    #[test]
    fn test_get_default_endpoint_profiles() -> Result<()> {
        let conn = setup_test_db()?;

        create_endpoint_profile(
            &conn,
            &NewEndpointProfile {
                name: "Local FS".to_string(),
                kind: EndpointKind::Local,
                config: serde_json::json!({ "root": "/" }),
                credential_ref: None,
                is_default_source: true,
                is_default_destination: false,
            },
        )?;

        create_endpoint_profile(
            &conn,
            &NewEndpointProfile {
                name: "Archive S3".to_string(),
                kind: EndpointKind::S3,
                config: serde_json::json!({
                    "bucket": "archive-bucket",
                    "region": "us-east-1"
                }),
                credential_ref: Some("s3_secret".to_string()),
                is_default_source: false,
                is_default_destination: true,
            },
        )?;

        let default_source = get_default_source_endpoint_profile(&conn)?
            .expect("Default source endpoint should be present");
        let default_destination = get_default_destination_endpoint_profile(&conn)?
            .expect("Default destination endpoint should be present");

        assert_eq!(default_source.name, "Local FS");
        assert_eq!(default_source.kind, EndpointKind::Local);
        assert_eq!(default_destination.name, "Archive S3");
        assert_eq!(default_destination.kind, EndpointKind::S3);

        Ok(())
    }

    #[test]
    fn test_sync_default_destination_s3_profile_creates_profile() -> Result<()> {
        let conn = setup_test_db()?;

        sync_default_destination_s3_profile(
            &conn,
            Some("sync-bucket"),
            Some("inbound/"),
            Some("us-east-1"),
            Some("https://sync.example"),
            Some("SYNC_ACCESS_KEY"),
            Some("s3_secret"),
        )?;

        let profile = get_default_destination_endpoint_profile(&conn)?
            .expect("Default destination profile should exist");
        assert_eq!(profile.kind, EndpointKind::S3);
        assert_eq!(
            profile.config.get("bucket"),
            Some(&Value::String("sync-bucket".to_string()))
        );
        assert_eq!(
            profile.config.get("access_key"),
            Some(&Value::String("SYNC_ACCESS_KEY".to_string()))
        );
        assert_eq!(profile.credential_ref, Some("s3_secret".to_string()));

        Ok(())
    }

    #[test]
    fn test_sync_default_destination_s3_profile_updates_existing_s3_default() -> Result<()> {
        let conn = setup_test_db()?;

        create_endpoint_profile(
            &conn,
            &NewEndpointProfile {
                name: "Initial S3".to_string(),
                kind: EndpointKind::S3,
                config: serde_json::json!({
                    "bucket": "old-bucket",
                    "region": "us-west-1"
                }),
                credential_ref: Some("old_secret".to_string()),
                is_default_source: false,
                is_default_destination: true,
            },
        )?;

        sync_default_destination_s3_profile(
            &conn,
            Some("new-bucket"),
            Some("archive/"),
            Some("us-east-1"),
            None,
            Some("NEW_ACCESS_KEY"),
            Some("new_secret"),
        )?;

        let profile = get_default_destination_endpoint_profile(&conn)?
            .expect("Default destination profile should still exist");
        assert_eq!(profile.kind, EndpointKind::S3);
        assert_eq!(
            profile.config.get("bucket"),
            Some(&Value::String("new-bucket".to_string()))
        );
        assert_eq!(
            profile.config.get("region"),
            Some(&Value::String("us-east-1".to_string()))
        );
        assert_eq!(profile.credential_ref, Some("new_secret".to_string()));

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
