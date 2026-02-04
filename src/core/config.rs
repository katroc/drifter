use crate::ui::theme::Theme;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanMode {
    Stream,
    Full,
    Skip,
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
    #[serde(default = "default_quarantine_dir")]
    pub quarantine_dir: String,
    #[serde(default = "default_reports_dir")]
    pub reports_dir: String,
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
    pub watch_dir: Option<String>,
    #[serde(default)]
    pub delete_source_after_upload: bool,
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
    pub concurrency_upload_parts: usize,
    pub concurrency_scan_parts: usize,
    #[serde(default = "default_theme")]
    pub theme: String,
    #[serde(default = "default_scanner_enabled")]
    pub scanner_enabled: bool,
    #[serde(default)]
    pub host_metrics_enabled: bool,

    // Layout dimensions
    #[serde(default = "default_local_width_percent", alias = "local_width_percent")]
    pub local_width_percent: u16,
    #[serde(default = "default_history_width")]
    pub history_width: u16,

    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_scanner_enabled() -> bool {
    true
}
fn default_local_width_percent() -> u16 {
    50
}
fn default_history_width() -> u16 {
    45
}

impl Default for Config {
    fn default() -> Self {
        Self {
            quarantine_dir: "./quarantine".to_string(),
            reports_dir: "./reports".to_string(),
            state_dir: "./state".to_string(),
            watch_dir: None,
            delete_source_after_upload: false,
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
            concurrency_upload_global: 1,
            concurrency_upload_parts: 4,
            concurrency_scan_parts: 4,
            theme: Theme::default_name().to_string(),
            scanner_enabled: true,
            host_metrics_enabled: true,
            local_width_percent: 50,
            history_width: 60,
            log_level: "info".to_string(),
        }
    }
}
fn default_quarantine_dir() -> String {
    "./quarantine".to_string()
}
fn default_reports_dir() -> String {
    "./reports".to_string()
}
fn default_state_dir() -> String {
    "./state".to_string()
}
fn default_theme() -> String {
    Theme::default_name().to_string()
}

// --- Database-backed config ---

use crate::db;
use rusqlite::Connection;

pub fn load_config_from_db(conn: &Connection) -> Result<Config> {
    let settings = db::load_all_settings(conn)?;

    let get =
        |key: &str| -> Option<String> { settings.get(key).cloned().filter(|s| !s.is_empty()) };

    let get_or = |key: &str, default: &str| -> String {
        settings
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    };

    let get_u64 = |key: &str, default: u64| -> u64 {
        settings
            .get(key)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    };

    let get_usize = |key: &str, default: usize| -> usize {
        settings
            .get(key)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    };

    let get_u16 = |key: &str, default: u16| -> u16 {
        settings
            .get(key)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
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

    let legacy_parts = get_usize("concurrency_parts_per_file", 4);

    Ok(Config {
        quarantine_dir: get_or("quarantine_dir", "./quarantine"),
        reports_dir: get_or("reports_dir", "./reports"),
        state_dir: get_or("state_dir", "./state"),
        watch_dir: get("watch_dir"),
        delete_source_after_upload: get("delete_source_after_upload")
            .map(|s| s == "true")
            .unwrap_or(false),
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
        concurrency_upload_global: get_usize("concurrency_upload_global", 1),
        concurrency_upload_parts: get_usize("concurrency_upload_parts", legacy_parts),
        concurrency_scan_parts: get_usize("concurrency_scan_parts", legacy_parts),
        theme: Theme::resolve_name(&get_or("theme", Theme::default_name()))
            .unwrap_or(Theme::default_name())
            .to_string(),
        scanner_enabled: get("scanner_enabled").map(|s| s == "true").unwrap_or(true),
        host_metrics_enabled: get("host_metrics_enabled")
            .map(|s| s == "true")
            .unwrap_or(true),
        local_width_percent: get_u16("local_width_percent", get_u16("local_width_percent", 50)), // Fallback to old name for migration
        history_width: get_u16("history_width", 60),
        log_level: get_or("log_level", "info"),
    })
}

pub fn save_config_to_db(conn: &Connection, cfg: &Config) -> Result<()> {
    db::set_setting(conn, "quarantine_dir", &cfg.quarantine_dir)?;
    db::set_setting(conn, "reports_dir", &cfg.reports_dir)?;
    db::set_setting(conn, "state_dir", &cfg.state_dir)?;
    db::set_setting(conn, "watch_dir", cfg.watch_dir.as_deref().unwrap_or(""))?;
    db::set_setting(
        conn,
        "delete_source_after_upload",
        if cfg.delete_source_after_upload {
            "true"
        } else {
            "false"
        },
    )?;

    let scan_mode = match cfg.scan_mode {
        ScanMode::Stream => "stream",
        ScanMode::Full => "full",
        ScanMode::Skip => "skip",
    };
    db::set_setting(conn, "scan_mode", scan_mode)?;
    db::set_setting(
        conn,
        "scan_chunk_size_mb",
        &cfg.scan_chunk_size_mb.to_string(),
    )?;
    db::set_setting(
        conn,
        "max_scan_size_mb",
        &cfg.max_scan_size_mb
            .map(|v| v.to_string())
            .unwrap_or_default(),
    )?;
    db::set_setting(conn, "clamd_host", &cfg.clamd_host)?;
    db::set_setting(conn, "clamd_port", &cfg.clamd_port.to_string())?;
    db::set_setting(
        conn,
        "clamd_socket",
        cfg.clamd_socket.as_deref().unwrap_or(""),
    )?;

    db::set_setting(conn, "s3_bucket", cfg.s3_bucket.as_deref().unwrap_or(""))?;
    db::set_setting(conn, "s3_prefix", cfg.s3_prefix.as_deref().unwrap_or(""))?;
    db::set_setting(conn, "s3_region", cfg.s3_region.as_deref().unwrap_or(""))?;
    db::set_setting(
        conn,
        "s3_endpoint",
        cfg.s3_endpoint.as_deref().unwrap_or(""),
    )?;
    db::set_setting(
        conn,
        "s3_access_key",
        cfg.s3_access_key.as_deref().unwrap_or(""),
    )?;

    let s3_key_mode = match cfg.s3_key_mode {
        S3KeyMode::Original => "original",
        S3KeyMode::Template => "template",
    };
    db::set_setting(conn, "s3_key_mode", s3_key_mode)?;
    db::set_setting(
        conn,
        "s3_key_template",
        cfg.s3_key_template.as_deref().unwrap_or(""),
    )?;

    let sse = match cfg.sse {
        SseMode::Off => "off",
        SseMode::S3 => "s3",
        SseMode::Kms => "kms",
    };
    db::set_setting(conn, "sse", sse)?;
    db::set_setting(conn, "kms_key_id", cfg.kms_key_id.as_deref().unwrap_or(""))?;

    db::set_setting(conn, "part_size_mb", &cfg.part_size_mb.to_string())?;
    db::set_setting(
        conn,
        "concurrency_upload_global",
        &cfg.concurrency_upload_global.to_string(),
    )?;
    db::set_setting(
        conn,
        "concurrency_upload_parts",
        &cfg.concurrency_upload_parts.to_string(),
    )?;
    db::set_setting(
        conn,
        "concurrency_scan_parts",
        &cfg.concurrency_scan_parts.to_string(),
    )?;
    db::set_setting(conn, "theme", &cfg.theme)?;
    db::set_setting(
        conn,
        "scanner_enabled",
        if cfg.scanner_enabled { "true" } else { "false" },
    )?;
    db::set_setting(
        conn,
        "host_metrics_enabled",
        if cfg.host_metrics_enabled {
            "true"
        } else {
            "false"
        },
    )?;

    db::set_setting(
        conn,
        "local_width_percent",
        &cfg.local_width_percent.to_string(),
    )?;
    db::set_setting(conn, "history_width", &cfg.history_width.to_string())?;
    db::set_setting(conn, "log_level", &cfg.log_level)?;

    // Save secret separately with obfuscation
    if let Some(ref secret) = cfg.s3_secret_key {
        db::set_secret(conn, "s3_secret", secret)?;
    }

    Ok(())
}

impl Config {
    /// Validate configuration values
    /// Returns errors for invalid configurations
    #[allow(dead_code)]
    pub fn validate(&self) -> Result<()> {
        // Validate part_size_mb (S3 requires 5MB-5GB)
        if self.part_size_mb < 5 || self.part_size_mb > 5 * 1024 {
            anyhow::bail!("part_size_mb must be between 5 and 5120 (5MB-5GB)");
        }

        // Validate concurrency
        if self.concurrency_upload_global == 0 {
            anyhow::bail!("concurrency_upload_global must be > 0");
        }
        if self.concurrency_upload_parts == 0 {
            anyhow::bail!("concurrency_upload_parts must be > 0");
        }
        if self.concurrency_scan_parts == 0 {
            anyhow::bail!("concurrency_scan_parts must be > 0");
        }

        // Validate scan_chunk_size_mb
        if self.scan_chunk_size_mb == 0 {
            anyhow::bail!("scan_chunk_size_mb must be > 0");
        }

        // Validate layout dimensions
        if self.local_width_percent > 100 {
            anyhow::bail!("local_width_percent must be <= 100");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_db() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
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

    // --- Default Config Tests ---

    #[test]
    fn test_default_config_values() {
        let config = Config::default();

        assert_eq!(config.quarantine_dir, "./quarantine");
        assert_eq!(config.reports_dir, "./reports");
        assert_eq!(config.state_dir, "./state");
        assert!(!config.delete_source_after_upload);
        assert_eq!(config.scan_chunk_size_mb, 24);
        assert_eq!(config.clamd_host, "127.0.0.1");
        assert_eq!(config.clamd_port, 3310);
        assert_eq!(config.part_size_mb, 128);
        assert_eq!(config.concurrency_upload_global, 1);
        assert_eq!(config.concurrency_upload_parts, 4);
        assert_eq!(config.concurrency_scan_parts, 4);
        assert!(config.scanner_enabled);
        assert!(config.host_metrics_enabled);
        assert_eq!(config.local_width_percent, 50);
        assert_eq!(config.history_width, 60);
        assert_eq!(config.log_level, "info");
    }

    #[test]
    fn test_default_config_is_valid() {
        let config = Config::default();
        assert!(config.validate().is_ok(), "Default config should be valid");
    }

    // --- Validation Tests ---

    #[test]
    fn test_validate_part_size_too_small() {
        let config = Config {
            part_size_mb: 4, // Less than 5MB minimum
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err(), "Should fail with part size < 5MB");
    }

    #[test]
    fn test_validate_part_size_too_large() {
        let config = Config {
            part_size_mb: 6000, // More than 5GB
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err(), "Should fail with part size > 5GB");
    }

    #[test]
    fn test_validate_part_size_valid_range() {
        let mut config = Config {
            part_size_mb: 5, // Min
            ..Config::default()
        };
        assert!(config.validate().is_ok());

        config.part_size_mb = 128; // Default
        assert!(config.validate().is_ok());

        config.part_size_mb = 5 * 1024; // Max (5GB)
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_concurrency_upload_global_zero() {
        let config = Config {
            concurrency_upload_global: 0,
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err(), "Should fail with zero concurrency");
    }

    #[test]
    fn test_validate_concurrency_upload_parts_zero() {
        let config = Config {
            concurrency_upload_parts: 0,
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err(), "Should fail with zero concurrency");
    }

    #[test]
    fn test_validate_concurrency_scan_parts_zero() {
        let config = Config {
            concurrency_scan_parts: 0,
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err(), "Should fail with zero concurrency");
    }

    #[test]
    fn test_validate_scan_chunk_size_zero() {
        let config = Config {
            scan_chunk_size_mb: 0,
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err(), "Should fail with zero scan chunk size");
    }

    #[test]
    fn test_validate_local_width_percent_over_100() {
        let config = Config {
            local_width_percent: 101,
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err(), "Should fail with local width > 100%");
    }

    // --- Enum Serialization Tests ---

    #[test]
    fn test_scan_mode_serialization() {
        let stream = serde_json::to_string(&ScanMode::Stream).unwrap();
        assert_eq!(stream, "\"stream\"");

        let full = serde_json::to_string(&ScanMode::Full).unwrap();
        assert_eq!(full, "\"full\"");

        let skip = serde_json::to_string(&ScanMode::Skip).unwrap();
        assert_eq!(skip, "\"skip\"");
    }

    #[test]
    fn test_scan_mode_deserialization() {
        let stream: ScanMode = serde_json::from_str("\"stream\"").unwrap();
        assert!(matches!(stream, ScanMode::Stream));

        let full: ScanMode = serde_json::from_str("\"full\"").unwrap();
        assert!(matches!(full, ScanMode::Full));

        let skip: ScanMode = serde_json::from_str("\"skip\"").unwrap();
        assert!(matches!(skip, ScanMode::Skip));
    }

    #[test]
    fn test_s3_key_mode_serialization() {
        let original = serde_json::to_string(&S3KeyMode::Original).unwrap();
        assert_eq!(original, "\"original\"");

        let template = serde_json::to_string(&S3KeyMode::Template).unwrap();
        assert_eq!(template, "\"template\"");
    }

    #[test]
    fn test_s3_key_mode_deserialization() {
        let original: S3KeyMode = serde_json::from_str("\"original\"").unwrap();
        assert!(matches!(original, S3KeyMode::Original));

        let template: S3KeyMode = serde_json::from_str("\"template\"").unwrap();
        assert!(matches!(template, S3KeyMode::Template));
    }

    #[test]
    fn test_sse_mode_serialization() {
        let off = serde_json::to_string(&SseMode::Off).unwrap();
        assert_eq!(off, "\"off\"");

        let s3 = serde_json::to_string(&SseMode::S3).unwrap();
        assert_eq!(s3, "\"s3\"");

        let kms = serde_json::to_string(&SseMode::Kms).unwrap();
        assert_eq!(kms, "\"kms\"");
    }

    #[test]
    fn test_sse_mode_deserialization() {
        let off: SseMode = serde_json::from_str("\"off\"").unwrap();
        assert!(matches!(off, SseMode::Off));

        let s3: SseMode = serde_json::from_str("\"s3\"").unwrap();
        assert!(matches!(s3, SseMode::S3));

        let kms: SseMode = serde_json::from_str("\"kms\"").unwrap();
        assert!(matches!(kms, SseMode::Kms));
    }

    // --- DB Persistence Tests ---

    #[test]
    fn test_save_and_load_config_round_trip() -> Result<()> {
        let conn = setup_test_db()?;

        let config = Config {
            part_size_mb: 256,
            concurrency_upload_global: 2,
            scanner_enabled: false,
            s3_bucket: Some("test-bucket".to_string()),
            s3_secret_key: Some("test-secret".to_string()),
            ..Config::default()
        };

        save_config_to_db(&conn, &config)?;
        let loaded = load_config_from_db(&conn)?;

        assert_eq!(loaded.part_size_mb, 256);
        assert_eq!(loaded.concurrency_upload_global, 2);
        assert!(!loaded.scanner_enabled);
        assert_eq!(loaded.s3_bucket, Some("test-bucket".to_string()));
        assert_eq!(loaded.s3_secret_key, Some("test-secret".to_string()));

        Ok(())
    }

    #[test]
    fn test_save_and_load_enum_modes() -> Result<()> {
        let conn = setup_test_db()?;

        let config = Config {
            scan_mode: ScanMode::Skip,
            s3_key_mode: S3KeyMode::Template,
            sse: SseMode::Kms,
            ..Config::default()
        };

        save_config_to_db(&conn, &config)?;
        let loaded = load_config_from_db(&conn)?;

        assert!(matches!(loaded.scan_mode, ScanMode::Skip));
        assert!(matches!(loaded.s3_key_mode, S3KeyMode::Template));
        assert!(matches!(loaded.sse, SseMode::Kms));

        Ok(())
    }

    #[test]
    fn test_load_config_with_missing_values_uses_defaults() -> Result<()> {
        let conn = setup_test_db()?;

        // Save minimal config (some settings missing)
        db::set_setting(&conn, "part_size_mb", "256")?;

        let loaded = load_config_from_db(&conn)?;

        // Should use default for missing values
        assert_eq!(loaded.part_size_mb, 256); // saved value
        assert_eq!(loaded.concurrency_upload_global, 1); // default

        Ok(())
    }

    #[test]
    fn test_save_config_with_optional_none_values() -> Result<()> {
        let conn = setup_test_db()?;

        let config = Config {
            watch_dir: None,
            s3_bucket: None,
            s3_secret_key: None,
            ..Config::default()
        };

        save_config_to_db(&conn, &config)?;
        let loaded = load_config_from_db(&conn)?;

        assert_eq!(loaded.watch_dir, None);
        assert_eq!(loaded.s3_bucket, None);
        assert_eq!(loaded.s3_secret_key, None);

        Ok(())
    }

    #[test]
    fn test_save_config_with_optional_some_values() -> Result<()> {
        let conn = setup_test_db()?;

        let config = Config {
            watch_dir: Some("/watch".to_string()),
            s3_bucket: Some("bucket".to_string()),
            s3_region: Some("us-west-2".to_string()),
            ..Config::default()
        };

        save_config_to_db(&conn, &config)?;
        let loaded = load_config_from_db(&conn)?;

        assert_eq!(loaded.watch_dir, Some("/watch".to_string()));
        assert_eq!(loaded.s3_bucket, Some("bucket".to_string()));
        assert_eq!(loaded.s3_region, Some("us-west-2".to_string()));

        Ok(())
    }

    #[test]
    fn test_save_config_boolean_flags() -> Result<()> {
        let conn = setup_test_db()?;

        let config = Config {
            delete_source_after_upload: true,
            scanner_enabled: false,
            host_metrics_enabled: false,
            ..Config::default()
        };

        save_config_to_db(&conn, &config)?;
        let loaded = load_config_from_db(&conn)?;

        assert!(loaded.delete_source_after_upload);
        assert!(!loaded.scanner_enabled);
        assert!(!loaded.host_metrics_enabled);

        Ok(())
    }

    #[test]
    fn test_config_persistence_all_fields() -> Result<()> {
        let conn = setup_test_db()?;

        let config = Config {
            quarantine_dir: "/quarantine".to_string(),
            reports_dir: "/reports".to_string(),
            state_dir: "/state".to_string(),
            watch_dir: Some("/watch".to_string()),
            delete_source_after_upload: true,
            scan_mode: ScanMode::Full,
            scan_chunk_size_mb: 50,
            max_scan_size_mb: Some(1000),
            clamd_host: "scanner.local".to_string(),
            clamd_port: 9999,
            clamd_socket: Some("/var/run/clamd.sock".to_string()),
            s3_bucket: Some("my-bucket".to_string()),
            s3_prefix: Some("uploads/".to_string()),
            s3_region: Some("eu-west-1".to_string()),
            s3_endpoint: Some("https://s3.example.com".to_string()),
            s3_access_key: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            s3_secret_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            s3_key_mode: S3KeyMode::Template,
            s3_key_template: Some("{date}/{filename}".to_string()),
            sse: SseMode::Kms,
            kms_key_id: Some("key-12345".to_string()),
            part_size_mb: 512,
            concurrency_upload_global: 3,
            concurrency_upload_parts: 8,
            concurrency_scan_parts: 6,
            theme: "monokai".to_string(),
            scanner_enabled: false,
            host_metrics_enabled: true,
            local_width_percent: 60,
            history_width: 80,
            log_level: "debug".to_string(),
        };

        save_config_to_db(&conn, &config)?;
        let loaded = load_config_from_db(&conn)?;

        // Verify all fields
        assert_eq!(loaded.quarantine_dir, config.quarantine_dir);
        assert_eq!(loaded.reports_dir, config.reports_dir);
        assert_eq!(loaded.state_dir, config.state_dir);
        assert_eq!(loaded.watch_dir, config.watch_dir);
        assert_eq!(
            loaded.delete_source_after_upload,
            config.delete_source_after_upload
        );
        assert_eq!(loaded.scan_chunk_size_mb, config.scan_chunk_size_mb);
        assert_eq!(loaded.max_scan_size_mb, config.max_scan_size_mb);
        assert_eq!(loaded.clamd_host, config.clamd_host);
        assert_eq!(loaded.clamd_port, config.clamd_port);
        assert_eq!(loaded.clamd_socket, config.clamd_socket);
        assert_eq!(loaded.s3_bucket, config.s3_bucket);
        assert_eq!(loaded.s3_prefix, config.s3_prefix);
        assert_eq!(loaded.s3_region, config.s3_region);
        assert_eq!(loaded.s3_endpoint, config.s3_endpoint);
        assert_eq!(loaded.s3_access_key, config.s3_access_key);
        assert_eq!(loaded.s3_secret_key, config.s3_secret_key);
        assert_eq!(loaded.s3_key_template, config.s3_key_template);
        assert_eq!(loaded.kms_key_id, config.kms_key_id);
        assert_eq!(loaded.part_size_mb, config.part_size_mb);
        assert_eq!(
            loaded.concurrency_upload_global,
            config.concurrency_upload_global
        );
        assert_eq!(
            loaded.concurrency_upload_parts,
            config.concurrency_upload_parts
        );
        assert_eq!(loaded.concurrency_scan_parts, config.concurrency_scan_parts);
        assert_eq!(loaded.scanner_enabled, config.scanner_enabled);
        assert_eq!(loaded.host_metrics_enabled, config.host_metrics_enabled);
        assert_eq!(loaded.local_width_percent, config.local_width_percent);
        assert_eq!(loaded.history_width, config.history_width);
        assert_eq!(loaded.log_level, config.log_level);

        Ok(())
    }
}
