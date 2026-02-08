use crate::core::config::{Config, DEFAULT_S3_REGION, KNOWN_S3_REGIONS};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WizardStep {
    Paths,
    Scanner,
    S3,
    Performance,
    Done,
}

#[derive(Debug, Clone)]
pub struct WizardState {
    pub step: WizardStep,
    pub field: usize,
    pub editing: bool,
    // Paths
    pub quarantine_dir: String,
    pub reports_dir: String,
    pub state_dir: String,
    // Scanner
    pub scanner_enabled: bool,
    pub clamd_host: String,
    pub clamd_port: String,
    pub scan_chunk_size: String,
    // S3
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub s3_region_custom: bool,
    pub s3_region_filter: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    // Performance
    pub part_size: String,
    pub concurrency_upload_global: String,
    pub concurrency_scan_global: String,
    pub concurrency_upload_parts: String,
    pub concurrency_scan_parts: String,
    pub delete_source_after_upload: bool,
    pub host_metrics_enabled: bool,
}

impl Default for WizardState {
    fn default() -> Self {
        Self::new()
    }
}

impl WizardState {
    pub const S3_REGION_OTHER_LABEL: &'static str = "Custom";

    pub fn known_s3_regions() -> &'static [&'static str] {
        &KNOWN_S3_REGIONS
    }

    fn is_known_s3_region(value: &str) -> bool {
        let trimmed = value.trim();
        !trimmed.is_empty() && KNOWN_S3_REGIONS.contains(&trimmed)
    }

    fn s3_region_custom_for_value(value: &str) -> bool {
        !Self::is_known_s3_region(value)
    }

    pub fn s3_region_other_index(&self) -> usize {
        KNOWN_S3_REGIONS.len()
    }

    pub fn selected_s3_region_known_index(&self) -> Option<usize> {
        if self.s3_region_custom {
            return None;
        }
        let region = self.region.trim();
        if region.is_empty() {
            return None;
        }
        KNOWN_S3_REGIONS.iter().position(|value| *value == region)
    }

    pub fn selected_s3_region_selector_index(&self) -> usize {
        self.selected_s3_region_known_index()
            .unwrap_or(self.s3_region_other_index())
    }

    pub fn is_s3_region_other_selected(&self) -> bool {
        self.s3_region_custom
    }

    pub fn set_s3_region_selector_index(&mut self, index: usize) {
        if index < KNOWN_S3_REGIONS.len() {
            self.region = KNOWN_S3_REGIONS[index].to_string();
            self.s3_region_custom = false;
        } else {
            if !self.s3_region_custom {
                self.region.clear();
            }
            self.s3_region_custom = true;
            self.s3_region_filter.clear();
        }
    }

    pub fn cycle_s3_region_selection(&mut self) {
        let selector_indices = self.filtered_s3_region_selector_indices();
        if selector_indices.is_empty() {
            return;
        }
        let selected_idx = self.selected_s3_region_selector_index();
        let next_pos = if let Some(current_pos) =
            selector_indices.iter().position(|idx| *idx == selected_idx)
        {
            (current_pos + 1) % selector_indices.len()
        } else {
            0
        };
        self.set_s3_region_selector_index(selector_indices[next_pos]);
    }

    pub fn cycle_s3_region_selection_prev(&mut self) {
        let selector_indices = self.filtered_s3_region_selector_indices();
        if selector_indices.is_empty() {
            return;
        }
        let selected_idx = self.selected_s3_region_selector_index();
        let prev_pos = if let Some(current_pos) =
            selector_indices.iter().position(|idx| *idx == selected_idx)
        {
            (current_pos + selector_indices.len().saturating_sub(1)) % selector_indices.len()
        } else {
            selector_indices.len().saturating_sub(1)
        };
        self.set_s3_region_selector_index(selector_indices[prev_pos]);
    }

    pub fn clear_s3_region_filter(&mut self) {
        self.s3_region_filter.clear();
    }

    pub fn s3_region_filter(&self) -> &str {
        self.s3_region_filter.as_str()
    }

    pub fn filtered_s3_region_selector_indices(&self) -> Vec<usize> {
        let filter = self.s3_region_filter.trim().to_ascii_lowercase();
        let mut indices: Vec<usize> = KNOWN_S3_REGIONS
            .iter()
            .enumerate()
            .filter_map(|(idx, region)| {
                if filter.is_empty() || region.to_ascii_lowercase().contains(&filter) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();

        indices.push(self.s3_region_other_index());
        indices
    }

    fn sync_s3_region_selection_to_filter(&mut self) {
        if self.s3_region_custom {
            return;
        }
        let filter = self.s3_region_filter.trim().to_ascii_lowercase();
        if filter.is_empty() {
            return;
        }
        if let Some((idx, _)) = KNOWN_S3_REGIONS
            .iter()
            .enumerate()
            .find(|(_, region)| region.to_ascii_lowercase().contains(&filter))
        {
            self.region = KNOWN_S3_REGIONS[idx].to_string();
        }
    }

    pub fn push_char_to_s3_region_filter(&mut self, c: char) {
        self.s3_region_filter.push(c);
        self.sync_s3_region_selection_to_filter();
    }

    pub fn pop_char_from_s3_region_filter(&mut self) {
        self.s3_region_filter.pop();
        self.sync_s3_region_selection_to_filter();
    }

    pub fn new() -> Self {
        let defaults = Config::default();
        let region = defaults
            .s3_region
            .unwrap_or_else(|| DEFAULT_S3_REGION.to_string());
        Self {
            step: WizardStep::Paths,
            field: 0,
            editing: false,
            quarantine_dir: defaults.quarantine_dir,
            reports_dir: defaults.reports_dir,
            state_dir: defaults.state_dir,
            scanner_enabled: defaults.scanner_enabled,
            clamd_host: defaults.clamd_host,
            clamd_port: defaults.clamd_port.to_string(),
            scan_chunk_size: defaults.scan_chunk_size_mb.to_string(),
            bucket: defaults.s3_bucket.unwrap_or_default(),
            prefix: defaults.s3_prefix.unwrap_or_default(),
            s3_region_custom: Self::s3_region_custom_for_value(&region),
            s3_region_filter: String::new(),
            region,
            endpoint: defaults.s3_endpoint.unwrap_or_default(),
            access_key: defaults.s3_access_key.unwrap_or_default(),
            secret_key: defaults.s3_secret_key.unwrap_or_default(),
            part_size: defaults.part_size_mb.to_string(),
            concurrency_upload_global: defaults.concurrency_upload_global.to_string(),
            concurrency_scan_global: defaults.concurrency_scan_global.to_string(),
            concurrency_upload_parts: defaults.concurrency_upload_parts.to_string(),
            concurrency_scan_parts: defaults.concurrency_scan_parts.to_string(),
            delete_source_after_upload: defaults.delete_source_after_upload,
            host_metrics_enabled: defaults.host_metrics_enabled,
        }
    }

    pub fn from_config(cfg: &Config) -> Self {
        let region = cfg
            .s3_region
            .clone()
            .unwrap_or_else(|| DEFAULT_S3_REGION.to_string());
        Self {
            step: WizardStep::Paths,
            field: 0,
            editing: false,
            quarantine_dir: cfg.quarantine_dir.clone(),
            reports_dir: cfg.reports_dir.clone(),
            state_dir: cfg.state_dir.clone(),
            scanner_enabled: cfg.scanner_enabled,
            clamd_host: cfg.clamd_host.clone(),
            clamd_port: cfg.clamd_port.to_string(),
            scan_chunk_size: cfg.scan_chunk_size_mb.to_string(),
            bucket: cfg.s3_bucket.clone().unwrap_or_default(),
            prefix: cfg.s3_prefix.clone().unwrap_or_default(),
            s3_region_custom: Self::s3_region_custom_for_value(&region),
            s3_region_filter: String::new(),
            region,
            endpoint: cfg.s3_endpoint.clone().unwrap_or_default(),
            access_key: cfg.s3_access_key.clone().unwrap_or_default(),
            secret_key: cfg.s3_secret_key.clone().unwrap_or_default(),
            part_size: cfg.part_size_mb.to_string(),
            concurrency_upload_global: cfg.concurrency_upload_global.to_string(),
            concurrency_scan_global: cfg.concurrency_scan_global.to_string(),
            concurrency_upload_parts: cfg.concurrency_upload_parts.to_string(),
            concurrency_scan_parts: cfg.concurrency_scan_parts.to_string(),
            delete_source_after_upload: cfg.delete_source_after_upload,
            host_metrics_enabled: cfg.host_metrics_enabled,
        }
    }

    pub fn field_count(&self) -> usize {
        match self.step {
            WizardStep::Paths => 3,
            WizardStep::Scanner => 4,
            WizardStep::S3 => 6,
            WizardStep::Performance => 7,
            WizardStep::Done => 0,
        }
    }

    pub fn get_field_mut(&mut self) -> Option<&mut String> {
        match (self.step, self.field) {
            (WizardStep::Paths, 0) => Some(&mut self.quarantine_dir),
            (WizardStep::Paths, 1) => Some(&mut self.reports_dir),
            (WizardStep::Paths, 2) => Some(&mut self.state_dir),
            (WizardStep::Scanner, 1) => Some(&mut self.clamd_host),
            (WizardStep::Scanner, 2) => Some(&mut self.clamd_port),
            (WizardStep::Scanner, 3) => Some(&mut self.scan_chunk_size),
            (WizardStep::S3, 0) => Some(&mut self.bucket),
            (WizardStep::S3, 1) => Some(&mut self.prefix),
            (WizardStep::S3, 2) => Some(&mut self.region),
            (WizardStep::S3, 3) => Some(&mut self.endpoint),
            (WizardStep::S3, 4) => Some(&mut self.access_key),
            (WizardStep::S3, 5) => Some(&mut self.secret_key),
            (WizardStep::Performance, 0) => Some(&mut self.part_size),
            (WizardStep::Performance, 1) => Some(&mut self.concurrency_upload_global),
            (WizardStep::Performance, 2) => Some(&mut self.concurrency_scan_global),
            (WizardStep::Performance, 3) => Some(&mut self.concurrency_upload_parts),
            (WizardStep::Performance, 4) => Some(&mut self.concurrency_scan_parts),
            _ => None,
        }
    }

    pub fn is_toggle_field(&self) -> bool {
        matches!(
            (self.step, self.field),
            (WizardStep::Scanner, 0) | (WizardStep::Performance, 5) | (WizardStep::Performance, 6)
        )
    }

    pub fn toggle_current_field(&mut self) -> bool {
        match (self.step, self.field) {
            (WizardStep::Scanner, 0) => {
                self.scanner_enabled = !self.scanner_enabled;
                true
            }
            (WizardStep::Performance, 5) => {
                self.delete_source_after_upload = !self.delete_source_after_upload;
                true
            }
            (WizardStep::Performance, 6) => {
                self.host_metrics_enabled = !self.host_metrics_enabled;
                true
            }
            _ => false,
        }
    }

    pub fn next_step(&mut self) {
        self.step = match self.step {
            WizardStep::Paths => WizardStep::Scanner,
            WizardStep::Scanner => WizardStep::S3,
            WizardStep::S3 => WizardStep::Performance,
            WizardStep::Performance => WizardStep::Done,
            WizardStep::Done => WizardStep::Done,
        };
        self.field = 0;
    }

    pub fn prev_step(&mut self) {
        self.step = match self.step {
            WizardStep::Paths => WizardStep::Paths,
            WizardStep::Scanner => WizardStep::Paths,
            WizardStep::S3 => WizardStep::Scanner,
            WizardStep::Performance => WizardStep::S3,
            WizardStep::Done => WizardStep::Performance,
        };
        self.field = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_region_from_config_starts_non_custom() {
        let cfg = Config {
            s3_region: Some("eu-west-1".to_string()),
            ..Config::default()
        };
        let state = WizardState::from_config(&cfg);

        assert_eq!(state.region, "eu-west-1");
        assert!(!state.is_s3_region_other_selected());
        assert!(state.selected_s3_region_known_index().is_some());
    }

    #[test]
    fn unknown_region_from_config_starts_custom() {
        let cfg = Config {
            s3_region: Some("provider-private-9".to_string()),
            ..Config::default()
        };
        let state = WizardState::from_config(&cfg);

        assert_eq!(state.region, "provider-private-9");
        assert!(state.is_s3_region_other_selected());
    }

    #[test]
    fn filtered_cycle_selects_matching_region() {
        let mut state = WizardState::new();
        state.set_s3_region_selector_index(state.s3_region_other_index());

        for c in "us-east-1".chars() {
            state.push_char_to_s3_region_filter(c);
        }

        state.cycle_s3_region_selection();
        assert_eq!(state.region, "us-east-1");
        assert!(!state.is_s3_region_other_selected());
    }

    #[test]
    fn selecting_custom_switches_to_custom_input_mode() {
        let mut state = WizardState::new();
        state.set_s3_region_selector_index(state.s3_region_other_index());

        assert!(state.is_s3_region_other_selected());
        assert!(state.region.is_empty());

        state.region.push_str("custom-region-2");
        assert_eq!(state.region, "custom-region-2");
        assert!(state.is_s3_region_other_selected());
    }
}
