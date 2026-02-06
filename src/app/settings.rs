use crate::core::config::Config;
use crate::core::transfer::EndpointKind;
use crate::db;
use anyhow::Result;
use rusqlite::Connection;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsCategory {
    S3,
    Scanner,
    Performance,
    Theme,
}

impl SettingsCategory {
    pub fn field_count(&self) -> usize {
        match self {
            SettingsCategory::S3 => 7,
            SettingsCategory::Scanner => 4,
            SettingsCategory::Performance => 7,
            SettingsCategory::Theme => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3ProfileSelection {
    Primary,
    Secondary,
}

pub struct SettingsState {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub secondary_endpoint: String,
    pub secondary_bucket: String,
    pub secondary_region: String,
    pub secondary_prefix: String,
    pub secondary_access_key: String,
    pub secondary_secret_key: String,
    pub s3_profile_selection: S3ProfileSelection,
    pub clamd_host: String,
    pub clamd_port: String,
    pub scan_chunk_size: String,
    pub part_size: String,
    pub concurrency_global: String,
    pub concurrency_scan_global: String,
    pub concurrency_upload_parts: String,
    pub concurrency_scan_parts: String,
    pub active_category: SettingsCategory,
    pub selected_field: usize,
    pub editing: bool,
    pub theme: String,
    pub original_theme: Option<String>,
    pub scanner_enabled: bool,
    pub host_metrics_enabled: bool,
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
            secondary_endpoint: String::new(),
            secondary_bucket: String::new(),
            secondary_region: String::new(),
            secondary_prefix: String::new(),
            secondary_access_key: String::new(),
            secondary_secret_key: String::new(),
            s3_profile_selection: S3ProfileSelection::Primary,
            clamd_host: cfg.clamd_host.clone(),
            clamd_port: cfg.clamd_port.to_string(),
            scan_chunk_size: cfg.scan_chunk_size_mb.to_string(),
            part_size: cfg.part_size_mb.to_string(),
            concurrency_global: cfg.concurrency_upload_global.to_string(),
            concurrency_scan_global: cfg.concurrency_scan_global.to_string(),
            concurrency_upload_parts: cfg.concurrency_upload_parts.to_string(),
            concurrency_scan_parts: cfg.concurrency_scan_parts.to_string(),
            active_category: SettingsCategory::S3,
            selected_field: 0,
            editing: false,
            theme: cfg.theme.clone(),
            original_theme: None,
            scanner_enabled: cfg.scanner_enabled,
            host_metrics_enabled: cfg.host_metrics_enabled,
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
        if let Ok(v) = self.concurrency_scan_global.trim().parse() {
            cfg.concurrency_scan_global = v;
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
        cfg.delete_source_after_upload = self.delete_source_after_upload;
    }

    pub fn selected_s3_profile_label(&self) -> &'static str {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => "Primary",
            S3ProfileSelection::Secondary => "Secondary",
        }
    }

    pub fn cycle_s3_profile_selection(&mut self) {
        self.s3_profile_selection = match self.s3_profile_selection {
            S3ProfileSelection::Primary => S3ProfileSelection::Secondary,
            S3ProfileSelection::Secondary => S3ProfileSelection::Primary,
        };
    }

    pub fn selected_s3_endpoint_mut(&mut self) -> &mut String {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => &mut self.endpoint,
            S3ProfileSelection::Secondary => &mut self.secondary_endpoint,
        }
    }

    pub fn selected_s3_bucket_mut(&mut self) -> &mut String {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => &mut self.bucket,
            S3ProfileSelection::Secondary => &mut self.secondary_bucket,
        }
    }

    pub fn selected_s3_region_mut(&mut self) -> &mut String {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => &mut self.region,
            S3ProfileSelection::Secondary => &mut self.secondary_region,
        }
    }

    pub fn selected_s3_prefix_mut(&mut self) -> &mut String {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => &mut self.prefix,
            S3ProfileSelection::Secondary => &mut self.secondary_prefix,
        }
    }

    pub fn selected_s3_access_key_mut(&mut self) -> &mut String {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => &mut self.access_key,
            S3ProfileSelection::Secondary => &mut self.secondary_access_key,
        }
    }

    pub fn selected_s3_secret_key_mut(&mut self) -> &mut String {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => &mut self.secret_key,
            S3ProfileSelection::Secondary => &mut self.secondary_secret_key,
        }
    }

    pub fn selected_s3_endpoint(&self) -> &str {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => self.endpoint.as_str(),
            S3ProfileSelection::Secondary => self.secondary_endpoint.as_str(),
        }
    }

    pub fn selected_s3_bucket(&self) -> &str {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => self.bucket.as_str(),
            S3ProfileSelection::Secondary => self.secondary_bucket.as_str(),
        }
    }

    pub fn selected_s3_region(&self) -> &str {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => self.region.as_str(),
            S3ProfileSelection::Secondary => self.secondary_region.as_str(),
        }
    }

    pub fn selected_s3_prefix(&self) -> &str {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => self.prefix.as_str(),
            S3ProfileSelection::Secondary => self.secondary_prefix.as_str(),
        }
    }

    pub fn selected_s3_access_key(&self) -> &str {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => self.access_key.as_str(),
            S3ProfileSelection::Secondary => self.secondary_access_key.as_str(),
        }
    }

    pub fn selected_s3_secret_key_display(&self, reveal: bool) -> &str {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => {
                if reveal {
                    self.secret_key.as_str()
                } else {
                    "*******"
                }
            }
            S3ProfileSelection::Secondary => {
                if reveal {
                    self.secondary_secret_key.as_str()
                } else {
                    "*******"
                }
            }
        }
    }

    pub fn apply_selected_s3_profile_to_config(&self, cfg: &mut Config) {
        match self.s3_profile_selection {
            S3ProfileSelection::Primary => {
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
            }
            S3ProfileSelection::Secondary => {
                cfg.s3_endpoint = if self.secondary_endpoint.trim().is_empty() {
                    None
                } else {
                    Some(self.secondary_endpoint.trim().to_string())
                };
                cfg.s3_bucket = if self.secondary_bucket.trim().is_empty() {
                    None
                } else {
                    Some(self.secondary_bucket.trim().to_string())
                };
                cfg.s3_region = if self.secondary_region.trim().is_empty() {
                    None
                } else {
                    Some(self.secondary_region.trim().to_string())
                };
                cfg.s3_prefix = if self.secondary_prefix.trim().is_empty() {
                    None
                } else {
                    Some(self.secondary_prefix.trim().to_string())
                };
                cfg.s3_access_key = if self.secondary_access_key.trim().is_empty() {
                    None
                } else {
                    Some(self.secondary_access_key.trim().to_string())
                };
                cfg.s3_secret_key = if self.secondary_secret_key.trim().is_empty() {
                    None
                } else {
                    Some(self.secondary_secret_key.trim().to_string())
                };
            }
        }
    }

    pub fn apply_secondary_s3_profile_to_config(&self, cfg: &mut Config) {
        cfg.s3_endpoint = if self.secondary_endpoint.trim().is_empty() {
            None
        } else {
            Some(self.secondary_endpoint.trim().to_string())
        };
        cfg.s3_bucket = if self.secondary_bucket.trim().is_empty() {
            None
        } else {
            Some(self.secondary_bucket.trim().to_string())
        };
        cfg.s3_region = if self.secondary_region.trim().is_empty() {
            None
        } else {
            Some(self.secondary_region.trim().to_string())
        };
        cfg.s3_prefix = if self.secondary_prefix.trim().is_empty() {
            None
        } else {
            Some(self.secondary_prefix.trim().to_string())
        };
        cfg.s3_access_key = if self.secondary_access_key.trim().is_empty() {
            None
        } else {
            Some(self.secondary_access_key.trim().to_string())
        };
        cfg.s3_secret_key = if self.secondary_secret_key.trim().is_empty() {
            None
        } else {
            Some(self.secondary_secret_key.trim().to_string())
        };
    }

    pub fn load_secondary_profile_from_db(&mut self, conn: &Connection) -> Result<()> {
        let profile = db::get_endpoint_profile_by_name(conn, "Secondary S3")?;
        let Some(profile) = profile else {
            return Ok(());
        };
        if profile.kind != EndpointKind::S3 {
            return Ok(());
        }

        let read_config = |key: &str| -> String {
            profile
                .config
                .get(key)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .unwrap_or("")
                .to_string()
        };

        self.secondary_bucket = read_config("bucket");
        self.secondary_region = read_config("region");
        self.secondary_endpoint = read_config("endpoint");
        self.secondary_prefix = read_config("prefix");
        self.secondary_access_key = read_config("access_key");
        self.secondary_secret_key = if let Some(secret_ref) = profile.credential_ref.as_deref() {
            db::get_secret(conn, secret_ref)?.unwrap_or_default()
        } else {
            String::new()
        };

        Ok(())
    }

    pub fn save_secondary_profile_to_db(&self, conn: &Connection) -> Result<()> {
        let trim_non_empty = |value: &str| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        };

        let bucket = trim_non_empty(&self.secondary_bucket);
        let region = trim_non_empty(&self.secondary_region);
        let endpoint = trim_non_empty(&self.secondary_endpoint);
        let prefix = trim_non_empty(&self.secondary_prefix);
        let access_key = trim_non_empty(&self.secondary_access_key);
        let secret = trim_non_empty(&self.secondary_secret_key);

        let has_any = bucket.is_some()
            || region.is_some()
            || endpoint.is_some()
            || prefix.is_some()
            || access_key.is_some()
            || secret.is_some();
        if !has_any {
            return Ok(());
        }

        if let Some(ref secret_value) = secret {
            db::set_secret(conn, "s3_secret_secondary", secret_value)?;
        }

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

        let credential_ref = if secret.is_some() {
            Some("s3_secret_secondary")
        } else {
            None
        };

        if let Some(existing) = db::get_endpoint_profile_by_name(conn, "Secondary S3")? {
            db::update_endpoint_profile(
                conn,
                existing.id,
                &Value::Object(config),
                credential_ref,
                false,
                false,
            )?;
        } else {
            db::create_endpoint_profile(
                conn,
                &db::NewEndpointProfile {
                    name: "Secondary S3".to_string(),
                    kind: EndpointKind::S3,
                    config: Value::Object(config),
                    credential_ref: credential_ref.map(std::string::ToString::to_string),
                    is_default_source: false,
                    is_default_destination: false,
                },
            )?;
        }

        Ok(())
    }
}
