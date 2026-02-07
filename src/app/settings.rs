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
            SettingsCategory::S3 => 8,
            SettingsCategory::Scanner => 4,
            SettingsCategory::Performance => 7,
            SettingsCategory::Theme => 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct S3ProfileEntry {
    pub id: i64,
    pub name: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub is_default_source: bool,
    pub is_default_destination: bool,
}

pub struct SettingsState {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub s3_profile_id: Option<i64>,
    pub s3_profile_name: String,
    pub s3_profile_is_default_source: bool,
    pub s3_profile_is_default_destination: bool,
    pub s3_profiles: Vec<S3ProfileEntry>,
    pub s3_profile_index: usize,
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
            s3_profile_id: None,
            s3_profile_name: "Unconfigured".to_string(),
            s3_profile_is_default_source: false,
            s3_profile_is_default_destination: false,
            s3_profiles: Vec::new(),
            s3_profile_index: 0,
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
        // Keep legacy config S3 fields in sync only when editing the default destination profile.
        if self.s3_profile_is_default_destination {
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

    fn trim_non_empty(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    fn read_config(profile: &db::EndpointProfileRow, key: &str) -> String {
        profile
            .config
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or("")
            .to_string()
    }

    fn profile_entry_from_row(
        conn: &Connection,
        profile: db::EndpointProfileRow,
    ) -> Result<S3ProfileEntry> {
        let secret_key = if let Some(secret_ref) = profile.credential_ref.as_deref() {
            db::get_secret(conn, secret_ref)?.unwrap_or_default()
        } else {
            String::new()
        };

        Ok(S3ProfileEntry {
            id: profile.id,
            name: profile.name.clone(),
            endpoint: Self::read_config(&profile, "endpoint"),
            bucket: Self::read_config(&profile, "bucket"),
            region: Self::read_config(&profile, "region"),
            prefix: Self::read_config(&profile, "prefix"),
            access_key: Self::read_config(&profile, "access_key"),
            secret_key,
            is_default_source: profile.is_default_source,
            is_default_destination: profile.is_default_destination,
        })
    }

    fn apply_profile_index(&mut self, index: usize) {
        if self.s3_profiles.is_empty() || index >= self.s3_profiles.len() {
            self.s3_profile_id = None;
            self.s3_profile_name = "Unconfigured".to_string();
            self.s3_profile_is_default_source = false;
            self.s3_profile_is_default_destination = false;
            self.endpoint.clear();
            self.bucket.clear();
            self.region.clear();
            self.prefix.clear();
            self.access_key.clear();
            self.secret_key.clear();
            self.s3_profile_index = 0;
            return;
        }

        let profile = self.s3_profiles[index].clone();
        self.s3_profile_id = Some(profile.id);
        self.s3_profile_name = profile.name;
        self.s3_profile_is_default_source = profile.is_default_source;
        self.s3_profile_is_default_destination = profile.is_default_destination;
        self.endpoint = profile.endpoint;
        self.bucket = profile.bucket;
        self.region = profile.region;
        self.prefix = profile.prefix;
        self.access_key = profile.access_key;
        self.secret_key = profile.secret_key;
        self.s3_profile_index = index;
    }

    pub fn selected_s3_profile_label(&self) -> &str {
        self.s3_profile_name.as_str()
    }

    pub fn selected_s3_profile_name(&self) -> &str {
        self.s3_profile_name.as_str()
    }

    pub fn selected_s3_profile_name_mut(&mut self) -> &mut String {
        &mut self.s3_profile_name
    }

    pub fn cycle_s3_profile_selection(&mut self) {
        if self.s3_profiles.is_empty() {
            return;
        }
        let next = (self.s3_profile_index + 1) % self.s3_profiles.len();
        self.apply_profile_index(next);
    }

    pub fn cycle_s3_profile_selection_prev(&mut self) {
        if self.s3_profiles.is_empty() {
            return;
        }
        let next = (self.s3_profile_index + self.s3_profiles.len() - 1) % self.s3_profiles.len();
        self.apply_profile_index(next);
    }

    pub fn selected_s3_endpoint_mut(&mut self) -> &mut String {
        &mut self.endpoint
    }

    pub fn selected_s3_bucket_mut(&mut self) -> &mut String {
        &mut self.bucket
    }

    pub fn selected_s3_region_mut(&mut self) -> &mut String {
        &mut self.region
    }

    pub fn selected_s3_prefix_mut(&mut self) -> &mut String {
        &mut self.prefix
    }

    pub fn selected_s3_access_key_mut(&mut self) -> &mut String {
        &mut self.access_key
    }

    pub fn selected_s3_secret_key_mut(&mut self) -> &mut String {
        &mut self.secret_key
    }

    pub fn selected_s3_endpoint(&self) -> &str {
        self.endpoint.as_str()
    }

    pub fn selected_s3_bucket(&self) -> &str {
        self.bucket.as_str()
    }

    pub fn selected_s3_region(&self) -> &str {
        self.region.as_str()
    }

    pub fn selected_s3_prefix(&self) -> &str {
        self.prefix.as_str()
    }

    pub fn selected_s3_access_key(&self) -> &str {
        self.access_key.as_str()
    }

    pub fn selected_s3_secret_key_display(&self, reveal: bool) -> &str {
        if reveal {
            self.secret_key.as_str()
        } else {
            "*******"
        }
    }

    pub fn apply_selected_s3_profile_to_config(&self, cfg: &mut Config) {
        cfg.s3_endpoint = Self::trim_non_empty(&self.endpoint);
        cfg.s3_bucket = Self::trim_non_empty(&self.bucket);
        cfg.s3_region = Self::trim_non_empty(&self.region);
        cfg.s3_prefix = Self::trim_non_empty(&self.prefix);
        cfg.s3_access_key = Self::trim_non_empty(&self.access_key);
        cfg.s3_secret_key = Self::trim_non_empty(&self.secret_key);
    }

    pub fn load_secondary_profile_from_db(&mut self, conn: &Connection) -> Result<()> {
        let target_profile_id = self.s3_profile_id;
        let profiles = db::list_endpoint_profiles(conn)?
            .into_iter()
            .filter(|profile| profile.kind == EndpointKind::S3)
            .map(|profile| Self::profile_entry_from_row(conn, profile))
            .collect::<Result<Vec<_>>>()?;

        self.s3_profiles = profiles;

        if self.s3_profiles.is_empty() {
            self.apply_profile_index(usize::MAX);
            return Ok(());
        }

        let selected = if let Some(id) = target_profile_id {
            self.s3_profiles.iter().position(|profile| profile.id == id)
        } else {
            None
        }
        .or_else(|| {
            self.s3_profiles
                .iter()
                .position(|profile| profile.is_default_destination)
        })
        .unwrap_or(0);

        self.apply_profile_index(selected);
        Ok(())
    }

    fn next_profile_name(&self) -> String {
        let mut n = self.s3_profiles.len() + 1;
        loop {
            let candidate = format!("S3 Profile {}", n);
            if !self
                .s3_profiles
                .iter()
                .any(|profile| profile.name == candidate)
            {
                return candidate;
            }
            n += 1;
        }
    }

    fn ensure_unique_profile_name(&self, requested: &str, exclude_id: Option<i64>) -> String {
        if requested.trim().is_empty() {
            return self.next_profile_name();
        }
        let base = requested.trim().to_string();
        if !self.s3_profiles.iter().any(|profile| {
            if Some(profile.id) == exclude_id {
                return false;
            }
            profile.name == base
        }) {
            return base;
        }
        let mut n = 2usize;
        loop {
            let candidate = format!("{} ({})", base, n);
            if !self.s3_profiles.iter().any(|profile| {
                if Some(profile.id) == exclude_id {
                    return false;
                }
                profile.name == candidate
            }) {
                return candidate;
            }
            n += 1;
        }
    }

    pub fn create_s3_profile(&mut self, conn: &Connection) -> Result<()> {
        let name = self.next_profile_name();
        let new_id = db::create_endpoint_profile(
            conn,
            &db::NewEndpointProfile {
                name,
                kind: EndpointKind::S3,
                config: Value::Object(Map::new()),
                credential_ref: None,
                is_default_source: false,
                is_default_destination: false,
            },
        )?;
        self.s3_profile_id = Some(new_id);
        self.load_secondary_profile_from_db(conn)?;
        Ok(())
    }

    pub fn delete_current_s3_profile(&mut self, conn: &Connection) -> Result<bool> {
        let Some(profile_id) = self.s3_profile_id else {
            return Ok(false);
        };
        db::delete_endpoint_profile(conn, profile_id)?;
        self.s3_profile_id = None;
        self.load_secondary_profile_from_db(conn)?;
        Ok(true)
    }

    pub fn save_secondary_profile_to_db(&self, conn: &Connection) -> Result<()> {
        let profile_id = if let Some(id) = self.s3_profile_id {
            id
        } else {
            return Ok(());
        };

        let profile_name = self.ensure_unique_profile_name(&self.s3_profile_name, Some(profile_id));
        db::rename_endpoint_profile(conn, profile_id, &profile_name)?;

        let mut config = Map::new();
        if let Some(v) = Self::trim_non_empty(&self.bucket) {
            config.insert("bucket".to_string(), Value::String(v));
        }
        if let Some(v) = Self::trim_non_empty(&self.region) {
            config.insert("region".to_string(), Value::String(v));
        }
        if let Some(v) = Self::trim_non_empty(&self.endpoint) {
            config.insert("endpoint".to_string(), Value::String(v));
        }
        if let Some(v) = Self::trim_non_empty(&self.prefix) {
            config.insert("prefix".to_string(), Value::String(v));
        }
        if let Some(v) = Self::trim_non_empty(&self.access_key) {
            config.insert("access_key".to_string(), Value::String(v));
        }

        let secret_ref = format!("s3_secret_profile_{}", profile_id);
        let credential_ref = if let Some(secret) = Self::trim_non_empty(&self.secret_key) {
            db::set_secret(conn, &secret_ref, &secret)?;
            Some(secret_ref)
        } else {
            None
        };

        db::update_endpoint_profile(
            conn,
            profile_id,
            &Value::Object(config),
            credential_ref.as_deref(),
            self.s3_profile_is_default_source,
            self.s3_profile_is_default_destination,
        )?;

        Ok(())
    }
}
