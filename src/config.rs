use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::theme::Theme;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanMode {
    Stream,
    Full,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingMode {
    Copy,
    Link,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum S3KeyMode {
    Original,
    Template,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SseMode {
    Off,
    S3,
    Kms,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_staging_dir")]
    pub staging_dir: String,
    #[serde(default = "default_quarantine_dir")]
    pub quarantine_dir: String,
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
    pub watch_dir: Option<String>,
    pub scan_mode: ScanMode,
    pub scan_chunk_size_mb: u64,
    pub max_scan_size_mb: Option<u64>,
    pub clamd_host: String,
    pub clamd_port: u16,
    pub clamd_socket: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_prefix: Option<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_access_key: Option<String>,
    #[serde(skip_serializing)]
    pub s3_secret_key: Option<String>,
    pub s3_key_mode: S3KeyMode,
    pub s3_key_template: Option<String>,
    pub sse: SseMode,
    pub kms_key_id: Option<String>,
    pub part_size_mb: u64,
    pub concurrency_upload_global: usize,
    pub concurrency_parts_per_file: usize,
    #[serde(default = "default_theme")]
    pub theme: String,
    #[serde(default = "default_scanner_enabled")]
    pub scanner_enabled: bool,
}

fn default_scanner_enabled() -> bool { true }

impl Default for Config {
    fn default() -> Self {
        Self {
            staging_dir: "./staging".to_string(),
            quarantine_dir: "./quarantine".to_string(),
            state_dir: "./state".to_string(),
            watch_dir: None,
            scan_mode: ScanMode::Stream,
            scan_chunk_size_mb: 24,
            max_scan_size_mb: None,
            clamd_host: "127.0.0.1".to_string(),
            clamd_port: 3310,
            clamd_socket: None,
            s3_bucket: None,
            s3_prefix: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key: None,
            s3_secret_key: None,
            s3_key_mode: S3KeyMode::Original,
            s3_key_template: None,
            sse: SseMode::Off,
            kms_key_id: None,
            part_size_mb: 128,
            concurrency_upload_global: 4,
            concurrency_parts_per_file: 4,
            theme: Theme::default_name().to_string(),
            scanner_enabled: true,
        }
    }
}



fn default_staging_dir() -> String { "./staging".to_string() }
fn default_quarantine_dir() -> String { "./quarantine".to_string() }
fn default_state_dir() -> String { "./state".to_string() }
fn default_theme() -> String { Theme::default_name().to_string() }

// --- Database-backed config ---

use rusqlite::Connection;
use crate::db;

pub fn load_config_from_db(conn: &Connection) -> Result<Config> {
    let settings = db::load_all_settings(conn)?;
    
    let get = |key: &str| -> Option<String> {
        settings.get(key).cloned().filter(|s| !s.is_empty())
    };
    
    let get_or = |key: &str, default: &str| -> String {
        settings.get(key).cloned().unwrap_or_else(|| default.to_string())
    };
    
    let get_u64 = |key: &str, default: u64| -> u64 {
        settings.get(key).and_then(|s| s.parse().ok()).unwrap_or(default)
    };
    
    let get_usize = |key: &str, default: usize| -> usize {
        settings.get(key).and_then(|s| s.parse().ok()).unwrap_or(default)
    };
    
    let get_u16 = |key: &str, default: u16| -> u16 {
        settings.get(key).and_then(|s| s.parse().ok()).unwrap_or(default)
    };
    
    let scan_mode = match settings.get("scan_mode").map(|s| s.as_str()) {
        Some("full") => ScanMode::Full,
        Some("skip") => ScanMode::Skip,
        _ => ScanMode::Stream,
    };
    
    let s3_key_mode = match settings.get("s3_key_mode").map(|s| s.as_str()) {
        Some("template") => S3KeyMode::Template,
        _ => S3KeyMode::Original,
    };
    
    let sse = match settings.get("sse").map(|s| s.as_str()) {
        Some("s3") => SseMode::S3,
        Some("kms") => SseMode::Kms,
        _ => SseMode::Off,
    };
    
    // Load secret separately
    let s3_secret_key = db::get_secret(conn, "s3_secret")?;
    
    Ok(Config {
        staging_dir: get_or("staging_dir", "./staging"),
        quarantine_dir: get_or("quarantine_dir", "./quarantine"),
        state_dir: get_or("state_dir", "./state"),
        watch_dir: get("watch_dir"),
        scan_mode,
        scan_chunk_size_mb: get_u64("scan_chunk_size_mb", 24),
        max_scan_size_mb: get("max_scan_size_mb").and_then(|s| s.parse().ok()),
        clamd_host: get_or("clamd_host", "127.0.0.1"),
        clamd_port: get_u16("clamd_port", 3310),
        clamd_socket: get("clamd_socket"),
        s3_bucket: get("s3_bucket"),
        s3_prefix: get("s3_prefix"),
        s3_region: get("s3_region"),
        s3_endpoint: get("s3_endpoint"),
        s3_access_key: get("s3_access_key"),
        s3_secret_key,
        s3_key_mode,
        s3_key_template: get("s3_key_template"),
        sse,
        kms_key_id: get("kms_key_id"),
        part_size_mb: get_u64("part_size_mb", 128),
        concurrency_upload_global: get_usize("concurrency_upload_global", 4),
        concurrency_parts_per_file: get_usize("concurrency_parts_per_file", 4),
        theme: Theme::resolve_name(&get_or("theme", Theme::default_name()))
            .unwrap_or(Theme::default_name())
            .to_string(),
        scanner_enabled: get("scanner_enabled").map(|s| s == "true").unwrap_or(true),
    })
}

pub fn save_config_to_db(conn: &Connection, cfg: &Config) -> Result<()> {
    db::set_setting(conn, "staging_dir", &cfg.staging_dir)?;
    db::set_setting(conn, "quarantine_dir", &cfg.quarantine_dir)?;
    db::set_setting(conn, "state_dir", &cfg.state_dir)?;
    db::set_setting(conn, "watch_dir", cfg.watch_dir.as_deref().unwrap_or(""))?;
    
    let scan_mode = match cfg.scan_mode {
        ScanMode::Stream => "stream",
        ScanMode::Full => "full",
        ScanMode::Skip => "skip",
    };
    db::set_setting(conn, "scan_mode", scan_mode)?;
    db::set_setting(conn, "scan_chunk_size_mb", &cfg.scan_chunk_size_mb.to_string())?;
    db::set_setting(conn, "max_scan_size_mb", &cfg.max_scan_size_mb.map(|v| v.to_string()).unwrap_or_default())?;
    db::set_setting(conn, "clamd_host", &cfg.clamd_host)?;
    db::set_setting(conn, "clamd_port", &cfg.clamd_port.to_string())?;
    db::set_setting(conn, "clamd_socket", cfg.clamd_socket.as_deref().unwrap_or(""))?;
    
    db::set_setting(conn, "s3_bucket", cfg.s3_bucket.as_deref().unwrap_or(""))?;
    db::set_setting(conn, "s3_prefix", cfg.s3_prefix.as_deref().unwrap_or(""))?;
    db::set_setting(conn, "s3_region", cfg.s3_region.as_deref().unwrap_or(""))?;
    db::set_setting(conn, "s3_endpoint", cfg.s3_endpoint.as_deref().unwrap_or(""))?;
    db::set_setting(conn, "s3_access_key", cfg.s3_access_key.as_deref().unwrap_or(""))?;
    
    let s3_key_mode = match cfg.s3_key_mode {
        S3KeyMode::Original => "original",
        S3KeyMode::Template => "template",
    };
    db::set_setting(conn, "s3_key_mode", s3_key_mode)?;
    db::set_setting(conn, "s3_key_template", cfg.s3_key_template.as_deref().unwrap_or(""))?;
    
    let sse = match cfg.sse {
        SseMode::Off => "off",
        SseMode::S3 => "s3",
        SseMode::Kms => "kms",
    };
    db::set_setting(conn, "sse", sse)?;
    db::set_setting(conn, "kms_key_id", cfg.kms_key_id.as_deref().unwrap_or(""))?;
    
    db::set_setting(conn, "part_size_mb", &cfg.part_size_mb.to_string())?;
    db::set_setting(conn, "concurrency_upload_global", &cfg.concurrency_upload_global.to_string())?;
    db::set_setting(conn, "concurrency_parts_per_file", &cfg.concurrency_parts_per_file.to_string())?;
    db::set_setting(conn, "concurrency_parts_per_file", &cfg.concurrency_parts_per_file.to_string())?;
    db::set_setting(conn, "theme", &cfg.theme)?;
    db::set_setting(conn, "scanner_enabled", if cfg.scanner_enabled { "true" } else { "false" })?;
    
    // Save secret separately with obfuscation
    if let Some(ref secret) = cfg.s3_secret_key {
        db::set_secret(conn, "s3_secret", secret)?;
    }
    
    Ok(())
}
