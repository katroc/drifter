use crate::core::config::{Config, StagingMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsCategory {
    S3,
    Scanner,
    Performance,
    Theme,
}

pub struct SettingsState {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub clamd_host: String,
    pub clamd_port: String,
    pub scan_chunk_size: String,
    pub part_size: String,
    pub concurrency_global: String,
    pub concurrency_upload_parts: String,
    pub concurrency_scan_parts: String,
    pub active_category: SettingsCategory,
    pub selected_field: usize,
    pub editing: bool,
    pub theme: String,
    pub original_theme: Option<String>,
    pub scanner_enabled: bool,
    pub host_metrics_enabled: bool,
    pub staging_mode_direct: bool, // true = Direct mode, false = Copy mode
    pub delete_source_after_upload: bool,
}

impl SettingsState {
    pub fn from_config(cfg: &Config) -> Self {
        Self {
            endpoint: cfg.s3_endpoint.clone().unwrap_or_default(),
            bucket: cfg.s3_bucket.clone().unwrap_or_default(),
            region: cfg.s3_region.clone().unwrap_or_default(),
            prefix: cfg.s3_prefix.clone().unwrap_or_default(),
            access_key: cfg.s3_access_key.clone().unwrap_or_default(),
            secret_key: cfg.s3_secret_key.clone().unwrap_or_default(),
            clamd_host: cfg.clamd_host.clone(),
            clamd_port: cfg.clamd_port.to_string(),
            scan_chunk_size: cfg.scan_chunk_size_mb.to_string(),
            part_size: cfg.part_size_mb.to_string(),
            concurrency_global: cfg.concurrency_upload_global.to_string(),
            concurrency_upload_parts: cfg.concurrency_upload_parts.to_string(),
            concurrency_scan_parts: cfg.concurrency_scan_parts.to_string(),
            active_category: SettingsCategory::S3,
            selected_field: 0,
            editing: false,
            theme: cfg.theme.clone(),
            original_theme: None,
            scanner_enabled: cfg.scanner_enabled,
            host_metrics_enabled: cfg.host_metrics_enabled,
            staging_mode_direct: cfg.staging_mode == StagingMode::Direct,
            delete_source_after_upload: cfg.delete_source_after_upload,
        }
    }

    pub fn apply_to_config(&self, cfg: &mut Config) {
        cfg.s3_endpoint = if self.endpoint.trim().is_empty() {
            None
        } else {
            Some(self.endpoint.trim().to_string())
        };
        cfg.s3_bucket = if self.bucket.trim().is_empty() {
            None
        } else {
            Some(self.bucket.trim().to_string())
        };
        cfg.s3_region = if self.region.trim().is_empty() {
            None
        } else {
            Some(self.region.trim().to_string())
        };
        cfg.s3_prefix = if self.prefix.trim().is_empty() {
            None
        } else {
            Some(self.prefix.trim().to_string())
        };
        cfg.s3_access_key = if self.access_key.trim().is_empty() {
            None
        } else {
            Some(self.access_key.trim().to_string())
        };
        cfg.s3_secret_key = if self.secret_key.trim().is_empty() {
            None
        } else {
            Some(self.secret_key.trim().to_string())
        };
        cfg.clamd_host = self.clamd_host.trim().to_string();
        if let Ok(p) = self.clamd_port.trim().parse() {
            cfg.clamd_port = p;
        }
        if let Ok(v) = self.scan_chunk_size.trim().parse() {
            cfg.scan_chunk_size_mb = v;
        }
        if let Ok(v) = self.part_size.trim().parse() {
            cfg.part_size_mb = v;
        }
        if let Ok(v) = self.concurrency_global.trim().parse() {
            cfg.concurrency_upload_global = v;
        }
        if let Ok(v) = self.concurrency_upload_parts.trim().parse() {
            cfg.concurrency_upload_parts = v;
        }
        if let Ok(v) = self.concurrency_scan_parts.trim().parse() {
            cfg.concurrency_scan_parts = v;
        }
        cfg.theme = self.theme.clone();
        cfg.scanner_enabled = self.scanner_enabled;
        cfg.host_metrics_enabled = self.host_metrics_enabled;
        cfg.staging_mode = if self.staging_mode_direct {
            StagingMode::Direct
        } else {
            StagingMode::Copy
        };
        cfg.delete_source_after_upload = self.delete_source_after_upload;
    }
}
