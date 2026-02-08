use anyhow::Result;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, atomic::AtomicBool};
use std::time::{Duration, Instant};

use crate::app::settings::SettingsState;
use crate::components::file_picker::FilePicker;
use crate::components::wizard::WizardState;
use crate::core::config::Config;
use crate::core::metrics::{HostMetricsSnapshot, MetricsCollector};
use crate::core::transfer::EndpointKind;
use crate::core::transfer::TransferDirection;
use crate::db::{
    EndpointProfileRow, JobRow, JobTransferMetadata, get_default_destination_endpoint_profile,
    get_default_source_endpoint_profile, get_job_transfer_metadata, list_active_jobs,
    list_endpoint_profiles, list_history_jobs, list_quarantined_jobs,
};
use crate::services::uploader::S3Object;
use crate::utils::lock_mutex;

use crate::services::watch::Watcher;
use crate::ui::theme::Theme;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppTab {
    Transfers,
    Quarantine,
    Logs,
    Settings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppFocus {
    Rail,
    Browser, // Only in Transfers tab
    Queue,   // Only in Transfers tab
    History,
    Quarantine,       // Only in Quarantine tab
    Remote,           // Remote panel in Transfers tab
    Logs,             // Only in Logs tab
    SettingsCategory, // Switch categories
    SettingsFields,   // Edit fields
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModalAction {
    None,
    ClearHistory,
    CancelJob(i64),
    DeleteRemoteObject(String, String, bool, RemoteTarget), // key, current_path, is_dir, target
    QuitApp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteTarget {
    Primary,
    Secondary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewMode {
    Flat,
    Tree,
}

#[derive(Debug, Clone)]
pub struct VisualItem {
    pub text: String,
    pub index_in_jobs: Option<usize>, // Index in original flat list
    pub is_folder: bool,
    pub depth: usize,
    pub first_job_index: Option<usize>, // For details view when folder is selected
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Browsing,           // In File Picker
    Filter,             // In File Picker Filter
    LogSearch,          // In Log Viewer Search
    Confirmation,       // Modal
    LayoutAdjust,       // Popout
    QueueSearch,        // In Transfer Queue
    HistorySearch,      // In Job History
    RemoteBrowsing,     // For navigating S3 directories
    RemoteFolderCreate, // Modal for creating new folder in Remote view
    EndpointSelect,     // Endpoint profile selector popover in Transfers
}

#[derive(Debug)]
pub enum AppEvent {
    Notification(String),
    RemoteFileList(Option<i64>, String, Vec<S3Object>), // (endpoint_id, path, objects)
    RemoteFileListSecondary(Option<i64>, String, Vec<S3Object>), // (endpoint_id, path, objects)
    LogLine(String),
    RefreshRemote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutTarget {
    Local,
    Queue,
    History,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryFilter {
    All,
    Complete,
    Quarantined,
}

impl HistoryFilter {
    pub fn next(&self) -> Self {
        match self {
            HistoryFilter::All => HistoryFilter::Complete,
            HistoryFilter::Complete => HistoryFilter::Quarantined,
            HistoryFilter::Quarantined => HistoryFilter::All,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            HistoryFilter::All => "All",
            HistoryFilter::Complete => "Complete",
            HistoryFilter::Quarantined => "Quarantined",
        }
    }
}

// ProgressInfo struct definition needed? It's used in Arc<Mutex<HashMap<i64, ProgressInfo>>>.
// Assuming it's public in a module or defined here?
// The user imports `crate::state::ProgressInfo`?
// The original `tui.rs` imported `crate::state::ProgressInfo`.
// I should import it here too.

use crate::coordinator::ProgressInfo;

pub struct App {
    pub jobs: Vec<JobRow>,
    pub history: Vec<JobRow>,
    pub job_transfer_metadata: HashMap<i64, JobTransferMetadata>,
    pub selected: usize,
    pub selected_history: usize,
    pub quarantine: Vec<JobRow>,
    pub selected_quarantine: usize,

    pub s3_objects: Vec<S3Object>,
    pub selected_remote: usize,
    pub remote_current_path: String,
    pub remote_cache: HashMap<String, (Vec<S3Object>, Instant)>, // path -> (objects, fetched_at)
    pub remote_request_pending: Option<String>,                  // path currently being fetched
    pub remote_loading: bool,
    pub selected_remote_items: HashMap<String, S3Object>,
    pub selected_remote_items_secondary: HashMap<String, S3Object>,
    pub s3_objects_secondary: Vec<S3Object>,
    pub selected_remote_secondary: usize,
    pub remote_secondary_current_path: String,
    pub remote_secondary_cache: HashMap<String, (Vec<S3Object>, Instant)>,
    pub remote_secondary_request_pending: Option<String>,
    pub remote_secondary_loading: bool,
    pub transfer_direction: TransferDirection,
    pub s3_profiles: Vec<EndpointProfileRow>,
    pub transfer_source_endpoint_id: Option<i64>,
    pub transfer_destination_endpoint_id: Option<i64>,
    pub transfer_endpoint_select_target: Option<RemoteTarget>,
    pub transfer_endpoint_select_index: usize,

    pub last_refresh: Instant,
    pub status_message: String,
    pub status_message_at: Option<Instant>,
    pub input_mode: InputMode,
    pub input_buffer: String,
    pub history_filter: HistoryFilter,
    pub picker: FilePicker,
    pub watch_enabled: bool,
    pub _watch_path: Option<PathBuf>,
    pub _watch_seen: HashSet<PathBuf>,
    pub last_watch_scan: Instant,
    pub _watcher: Watcher,
    pub current_tab: AppTab,
    pub focus: AppFocus,
    pub settings: SettingsState,
    pub config: Arc<AsyncMutex<Config>>,
    pub cached_config: Config,
    pub clamav_status: Arc<Mutex<String>>,
    pub progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>>,
    pub cached_progress: HashMap<i64, ProgressInfo>,
    pub cancellation_tokens: Arc<AsyncMutex<HashMap<i64, Arc<AtomicBool>>>>,
    pub conn: Arc<Mutex<Connection>>,

    // Logs
    pub logs: VecDeque<String>,
    pub logs_scroll: usize,
    pub logs_scroll_x: usize,
    pub logs_stick_to_bottom: bool,
    pub log_search_active: bool,
    pub log_search_query: String,
    pub log_search_results: Vec<usize>, // Indicies of matching lines
    pub log_search_current: usize,      // Index in log_search_results
    pub log_handle: Option<crate::logging::LogHandle>,
    pub current_log_level: String,

    // Queue & History Search
    pub queue_search_query: String,
    pub history_search_query: String,

    // Visualization
    pub view_mode: ViewMode,
    pub visual_jobs: Vec<VisualItem>,
    pub visual_history: Vec<VisualItem>,

    // Async feedback channel
    pub async_rx: mpsc::Receiver<AppEvent>,
    pub async_tx: mpsc::Sender<AppEvent>,
    // Wizard
    pub show_wizard: bool,
    pub wizard: WizardState,
    pub wizard_from_settings: bool,
    pub theme: Theme,
    pub theme_names: Vec<&'static str>,
    // Modal State
    pub pending_action: ModalAction,
    pub confirmation_msg: String,
    pub confirmation_return_mode: Option<InputMode>,

    // Metrics
    pub metrics: MetricsCollector,
    pub last_metrics: HostMetricsSnapshot,
    pub last_metrics_refresh: Instant,

    // Mouse Interaction
    pub last_click_time: Option<Instant>,
    pub last_click_pos: Option<(u16, u16)>,
    pub hover_pos: Option<(u16, u16)>,

    // Layout Adjustment
    pub layout_adjust_target: Option<LayoutTarget>,
    pub layout_adjust_message: String,

    // Remote Folder Creation
    pub creating_folder_name: String,
}

impl App {
    pub async fn new(
        conn: Arc<Mutex<Connection>>,
        config: Arc<AsyncMutex<Config>>,
        progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>>,
        cancellation_tokens: Arc<AsyncMutex<HashMap<i64, Arc<AtomicBool>>>>,
        log_handle: crate::logging::LogHandle,
        async_tx: mpsc::Sender<AppEvent>,
        async_rx: mpsc::Receiver<AppEvent>,
    ) -> Result<Self> {
        let cfg_guard = config.lock().await;
        let cached_config = cfg_guard.clone();
        let settings = SettingsState::from_config(&cfg_guard);
        let theme = Theme::from_name(&cfg_guard.theme);
        let initial_log_level = cfg_guard.log_level.clone();
        drop(cfg_guard);

        let (tx, rx) = (async_tx, async_rx);

        let mut app = Self {
            jobs: Vec::new(),
            history: Vec::new(),
            job_transfer_metadata: HashMap::new(),
            quarantine: Vec::new(),
            selected: 0,
            selected_history: 0,
            selected_quarantine: 0,

            s3_objects: Vec::new(),
            selected_remote: 0,
            remote_current_path: String::new(),
            remote_cache: HashMap::new(),
            remote_request_pending: None,
            remote_loading: false,
            selected_remote_items: HashMap::new(),
            selected_remote_items_secondary: HashMap::new(),
            s3_objects_secondary: Vec::new(),
            selected_remote_secondary: 0,
            remote_secondary_current_path: String::new(),
            remote_secondary_cache: HashMap::new(),
            remote_secondary_request_pending: None,
            remote_secondary_loading: false,
            transfer_direction: TransferDirection::LocalToS3,
            s3_profiles: Vec::new(),
            transfer_source_endpoint_id: None,
            transfer_destination_endpoint_id: None,
            transfer_endpoint_select_target: None,
            transfer_endpoint_select_index: 0,

            last_refresh: Instant::now() - Duration::from_secs(5),
            status_message: "Ready".to_string(),
            status_message_at: None,
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            history_filter: HistoryFilter::All,
            picker: FilePicker::new(),
            watch_enabled: false,
            _watch_path: None,
            _watch_seen: HashSet::new(),
            last_watch_scan: Instant::now() - Duration::from_secs(10),
            _watcher: Watcher::new(conn.clone()),
            current_tab: AppTab::Transfers,
            focus: AppFocus::Browser,
            settings,
            config: config.clone(),
            cached_config,
            clamav_status: Arc::new(Mutex::new("Checking...".to_string())),
            progress: progress.clone(),
            cached_progress: HashMap::new(),
            async_rx: rx,
            async_tx: tx,
            show_wizard: false,
            wizard: WizardState::new(),
            wizard_from_settings: false,
            theme,
            theme_names: Theme::list_names(),
            pending_action: ModalAction::None,
            confirmation_msg: String::new(),
            confirmation_return_mode: None,

            metrics: MetricsCollector::new(),
            last_metrics: HostMetricsSnapshot::default(),
            last_metrics_refresh: Instant::now(),
            cancellation_tokens,
            view_mode: ViewMode::Flat,
            visual_jobs: Vec::new(),
            visual_history: Vec::new(),
            last_click_time: None,
            last_click_pos: None,
            hover_pos: None,
            layout_adjust_target: None,
            layout_adjust_message: String::new(),

            logs: VecDeque::new(),
            logs_scroll: 0,
            logs_scroll_x: 0,
            logs_stick_to_bottom: true,
            log_search_active: false,
            log_search_query: String::new(),
            log_search_results: Vec::new(),
            log_search_current: 0,
            log_handle: Some(log_handle),
            current_log_level: initial_log_level,

            conn: conn.clone(),
            queue_search_query: String::new(),
            history_search_query: String::new(),

            // Remote Folder Creation
            creating_folder_name: String::new(),
        };

        if let Ok(conn_guard) = lock_mutex(&conn) {
            let _ = app.settings.load_secondary_profile_from_db(&conn_guard);
            let _ = app.refresh_s3_profiles(&conn_guard);
        }

        if let Some(watch_dir) = app
            .cached_config
            .watch_dir
            .clone()
            .filter(|w| !w.trim().is_empty())
        {
            let watch_path = PathBuf::from(&watch_dir);
            if watch_path.exists() {
                app._watcher.start(watch_path.clone());
                app._watch_path = Some(watch_path);
                app.watch_enabled = true;
                app.status_message = format!("Watching {}", watch_dir);
            } else {
                app.status_message = format!("Watch path not found: {}", watch_dir);
            }
        }

        // ClamAV checker task (Tokio)
        let status_clone = app.clamav_status.clone();
        let config_clone = config.clone();
        tokio::spawn(async move {
            loop {
                // Read config for host/port
                let (host, port) = {
                    let cfg = config_clone.lock().await;
                    (cfg.clamd_host.clone(), cfg.clamd_port)
                };
                let addr = format!("{}:{}", host, port);
                let status = if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                    "Connected"
                } else {
                    "Disconnected"
                };

                if let Ok(mut s) = lock_mutex(&status_clone) {
                    *s = status.to_string();
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        app.picker.refresh();

        Ok(app)
    }

    pub fn set_status<S: Into<String>>(&mut self, msg: S) {
        self.status_message = msg.into();
        self.status_message_at = Some(Instant::now());
    }

    pub fn toggle_transfer_direction(&mut self) {
        self.transfer_direction = match self.transfer_direction {
            TransferDirection::LocalToS3 => TransferDirection::S3ToLocal,
            TransferDirection::S3ToLocal => TransferDirection::S3ToS3,
            TransferDirection::S3ToS3 => TransferDirection::LocalToS3,
        };
    }

    fn contains_s3_profile_id(&self, id: Option<i64>) -> bool {
        let Some(id) = id else {
            return false;
        };
        self.s3_profiles.iter().any(|profile| profile.id == id)
    }

    pub fn refresh_s3_profiles(&mut self, conn: &Connection) -> Result<()> {
        self.s3_profiles = list_endpoint_profiles(conn)?
            .into_iter()
            .filter(|profile| profile.kind == EndpointKind::S3)
            .collect();

        let first_s3 = self.s3_profiles.first().map(|profile| profile.id);
        let default_source_s3 = get_default_source_endpoint_profile(conn)?
            .filter(|profile| profile.kind == EndpointKind::S3)
            .map(|profile| profile.id);
        let default_destination_s3 = get_default_destination_endpoint_profile(conn)?
            .filter(|profile| profile.kind == EndpointKind::S3)
            .map(|profile| profile.id);

        if !self.contains_s3_profile_id(self.transfer_source_endpoint_id) {
            self.transfer_source_endpoint_id =
                default_source_s3.or(default_destination_s3).or(first_s3);
        }
        if !self.contains_s3_profile_id(self.transfer_destination_endpoint_id) {
            self.transfer_destination_endpoint_id =
                default_destination_s3.or(default_source_s3).or(first_s3);
        }

        if self.s3_profiles.is_empty() {
            self.transfer_source_endpoint_id = None;
            self.transfer_destination_endpoint_id = None;
        }

        Ok(())
    }

    pub fn endpoint_profile_name(&self, endpoint_id: Option<i64>) -> String {
        let Some(endpoint_id) = endpoint_id else {
            return "Unconfigured".to_string();
        };
        self.s3_profiles
            .iter()
            .find(|profile| profile.id == endpoint_id)
            .map(|profile| profile.name.clone())
            .unwrap_or_else(|| format!("Profile #{endpoint_id}"))
    }

    pub fn primary_remote_endpoint_id(&self) -> Option<i64> {
        match self.transfer_direction {
            TransferDirection::LocalToS3 => self.transfer_destination_endpoint_id,
            TransferDirection::S3ToLocal | TransferDirection::S3ToS3 => {
                self.transfer_source_endpoint_id
            }
        }
    }

    pub fn secondary_remote_endpoint_id(&self) -> Option<i64> {
        if self.transfer_direction == TransferDirection::S3ToS3 {
            self.transfer_destination_endpoint_id
        } else {
            None
        }
    }

    pub fn refresh_jobs(&mut self, conn: &Connection) -> Result<()> {
        // Use trace! to avoid spamming logs every tick
        // tracing::trace!("Refreshing jobs");
        self.jobs = list_active_jobs(conn, 100)?;
        let filter_str = if self.history_filter == HistoryFilter::All {
            None
        } else {
            Some(self.history_filter.as_str())
        };
        self.history = list_history_jobs(conn, 100, filter_str)?;
        self.quarantine = list_quarantined_jobs(conn, 100)?;
        self.refresh_transfer_metadata(conn);

        let prev_visual_jobs_len = self.visual_jobs.len();
        self.rebuild_visual_lists();

        if self.visual_jobs.len() != prev_visual_jobs_len {
            // Log when list size changes significantly?
            // tracing::debug!("Job list updated: {} jobs visible", self.visual_jobs.len());
        }

        // Auto-fix selected if out of bounds (using visual list size)
        if self.selected >= self.visual_jobs.len() {
            self.selected = self.visual_jobs.len().saturating_sub(1);
        }
        if self.selected_history >= self.visual_history.len() {
            self.selected_history = self.visual_history.len().saturating_sub(1);
        }
        if self.selected_quarantine >= self.quarantine.len() {
            self.selected_quarantine = self.quarantine.len().saturating_sub(1);
        }

        self.last_refresh = Instant::now();
        Ok(())
    }

    fn refresh_transfer_metadata(&mut self, conn: &Connection) {
        self.job_transfer_metadata.clear();

        let mut job_ids = HashSet::new();
        for job in self
            .jobs
            .iter()
            .chain(self.history.iter())
            .chain(self.quarantine.iter())
        {
            job_ids.insert(job.id);
        }

        for job_id in job_ids {
            match get_job_transfer_metadata(conn, job_id) {
                Ok(Some(metadata)) => {
                    self.job_transfer_metadata.insert(job_id, metadata);
                }
                Ok(None) => {}
                Err(e)
                    if e.to_string().contains("no such column")
                        || e.to_string().contains("no such table") =>
                {
                    self.job_transfer_metadata.clear();
                    return;
                }
                Err(_) => {}
            }
        }
    }

    pub fn transfer_metadata_for_job(&self, job_id: i64) -> Option<&JobTransferMetadata> {
        self.job_transfer_metadata.get(&job_id)
    }

    pub fn transfer_direction_for_job(&self, job_id: i64) -> Option<&str> {
        self.transfer_metadata_for_job(job_id)
            .and_then(|metadata| metadata.transfer_direction.as_deref())
    }

    pub fn rebuild_visual_lists(&mut self) {
        let q_query = self.queue_search_query.to_lowercase();
        self.visual_jobs = self.build_visual_list(&self.jobs, &q_query);
        let h_query = self.history_search_query.to_lowercase();
        self.visual_history = self.build_visual_list(&self.history, &h_query);
    }

    pub fn build_visual_list(&self, jobs: &[JobRow], filter_query: &str) -> Vec<VisualItem> {
        fn display_leaf_name(path: &str) -> String {
            let trimmed = path.trim_end_matches(['/', '\\']);
            if trimmed.is_empty() {
                return path.to_string();
            }
            Path::new(trimmed)
                .file_name()
                .and_then(|n| {
                    let name = n.to_string_lossy().to_string();
                    if name.is_empty() { None } else { Some(name) }
                })
                .or_else(|| {
                    trimmed
                        .rsplit(['/', '\\'])
                        .find(|part| !part.is_empty())
                        .map(std::string::ToString::to_string)
                })
                .unwrap_or_else(|| path.to_string())
        }

        let filtered_jobs: Vec<(usize, &JobRow)> = if filter_query.is_empty() {
            jobs.iter().enumerate().collect()
        } else {
            jobs.iter()
                .enumerate()
                .filter(|(_, job)| {
                    job.source_path.to_lowercase().contains(filter_query)
                        || Path::new(&job.source_path)
                            .file_name()
                            .map(|n| n.to_string_lossy().to_lowercase().contains(filter_query))
                            .unwrap_or(false)
                })
                .collect()
        };

        let (jobs_to_process, original_indices): (Vec<&JobRow>, Vec<usize>) =
            filtered_jobs.iter().map(|(idx, job)| (*job, *idx)).unzip();

        match self.view_mode {
            ViewMode::Flat => {
                filtered_jobs
                    .into_iter()
                    .map(|(i, job)| {
                        // Extract filename for display
                        let name = display_leaf_name(&job.source_path);

                        VisualItem {
                            text: name,
                            index_in_jobs: Some(i),
                            is_folder: false,
                            depth: 0,
                            first_job_index: None,
                        }
                    })
                    .collect()
            }
            ViewMode::Tree => {
                if jobs_to_process.is_empty() {
                    return Vec::new();
                }

                // 1. Calculate Common Prefix
                let paths: Vec<&Path> = jobs_to_process
                    .iter()
                    .map(|j| Path::new(&j.source_path))
                    .collect();
                let absolute_paths: Vec<&Path> =
                    paths.iter().copied().filter(|p| p.is_absolute()).collect();
                let common_candidate_paths: Vec<&Path> = if absolute_paths.len() >= 2 {
                    absolute_paths.clone()
                } else {
                    paths.clone()
                };

                let p0 = common_candidate_paths[0];
                let mut common_components: Vec<_> = p0.components().collect();

                for p in &common_candidate_paths[1..] {
                    let comps: Vec<_> = p.components().collect();
                    let min_len = std::cmp::min(common_components.len(), comps.len());
                    let mut match_len = 0;
                    while match_len < min_len && common_components[match_len] == comps[match_len] {
                        match_len += 1;
                    }
                    common_components.truncate(match_len);
                    if common_components.is_empty() {
                        break;
                    }
                }

                let mut common_path_buf: std::path::PathBuf = common_components.iter().collect();
                if common_path_buf.as_os_str().is_empty()
                    && !absolute_paths.is_empty()
                    && let Ok(cwd) = std::env::current_dir()
                    && absolute_paths.iter().any(|p| p.starts_with(&cwd))
                {
                    common_path_buf = cwd;
                }

                // 2. Heuristic for Base Path

                // If common prefix is exactly a file path (e.g. single file in list),
                // we should start "view" at its parent folder to show structure.
                let common_is_file = common_candidate_paths.contains(&common_path_buf.as_path());

                let effective_common_path = if common_is_file {
                    common_path_buf
                        .parent()
                        .unwrap_or(&common_path_buf)
                        .to_path_buf()
                } else {
                    common_path_buf.clone()
                };

                // Check depths relative to effective common prefix
                let depth_reference_paths: Vec<&Path> = paths
                    .iter()
                    .copied()
                    .filter(|p| p.strip_prefix(&effective_common_path).is_ok())
                    .collect();
                let depth_paths: Vec<&Path> = if depth_reference_paths.is_empty() {
                    paths.clone()
                } else {
                    depth_reference_paths
                };

                let max_depth = depth_paths
                    .iter()
                    .map(|p| {
                        p.strip_prefix(&effective_common_path)
                            .unwrap_or(p)
                            .components()
                            .count()
                    })
                    .max()
                    .unwrap_or(0);

                // If max_depth <= 1, it means all items are immediate children.
                // We show parent container context effectively.
                let base_path_buf = if max_depth <= 1 && !common_path_buf.as_os_str().is_empty() {
                    effective_common_path
                        .parent()
                        .map(|p| p.to_path_buf())
                        .unwrap_or(effective_common_path)
                } else {
                    effective_common_path
                };

                // 3. Build Tree (Order Preserving)
                struct TreeNode {
                    name: String,
                    children: Vec<TreeNode>,
                    files: Vec<usize>, // Indicies in original 'jobs' slice
                }

                let mut root = TreeNode {
                    name: String::new(),
                    children: Vec::new(),
                    files: Vec::new(),
                };

                for (it_idx, job) in jobs_to_process.iter().enumerate() {
                    let _orig_idx = original_indices[it_idx];
                    let full_path = Path::new(&job.source_path);
                    let rel_path = full_path.strip_prefix(&base_path_buf).unwrap_or(full_path);

                    // Components of the relative path
                    let components: Vec<String> = rel_path
                        .components()
                        .filter_map(|c| match c {
                            std::path::Component::Normal(part) => {
                                Some(part.to_string_lossy().to_string())
                            }
                            std::path::Component::ParentDir => Some("..".to_string()),
                            std::path::Component::CurDir
                            | std::path::Component::RootDir
                            | std::path::Component::Prefix(_) => None,
                        })
                        .collect();

                    let mut current = &mut root;

                    // Traverse folders (components excluding the last one, which is the file)
                    if components.len() > 1 {
                        for i in 0..components.len() - 1 {
                            let comp = &components[i];
                            let remaining_for_file = &components[i + 1..]; // This includes the filename at end

                            // Sequential Merge Check:
                            // Check ONLY the last child to preserve time order.
                            // And check for collisions.
                            let mut should_merge = false;

                            if let Some(last) = current.children.last()
                                && last.name == *comp
                            {
                                // Check collision: Does `last` already contain the file we are about to add?
                                // We need to verify if `last` + `remaining_for_file` hits an existing file.
                                // Helper closure for collision
                                fn check_collision(
                                    node: &TreeNode,
                                    path: &[String],
                                    jobs_to_process: &[&JobRow],
                                ) -> bool {
                                    if path.len() == 1 {
                                        // Path is just [filename]. Check node.files.
                                        let filename = &path[0];
                                        for &f_idx in &node.files {
                                            // index_in_jobs logic: f_idx is a relative index in the filtered list if Tree traversal uses it
                                            // Actually, current implementation pushes `idx` from loop over `jobs_to_process`
                                            // which is an index into `jobs_to_process`.
                                            let f_path = &jobs_to_process[f_idx].source_path;
                                            let f_name = display_leaf_name(f_path);
                                            if f_name == *filename {
                                                return true;
                                            }
                                        }
                                        return false;
                                    }
                                    // Path is [dir, ..., filename]
                                    let next_dir = &path[0];
                                    if let Some(child) =
                                        node.children.iter().find(|c| c.name == *next_dir)
                                    {
                                        return check_collision(child, &path[1..], jobs_to_process);
                                    }
                                    false
                                }

                                if !check_collision(last, remaining_for_file, &jobs_to_process) {
                                    should_merge = true;
                                }
                            }

                            if should_merge {
                                current = current
                                    .children
                                    .last_mut()
                                    .expect("Merge logic guarantees child exists");
                            } else {
                                let new_node = TreeNode {
                                    name: comp.clone(),
                                    children: Vec::new(),
                                    files: Vec::new(),
                                };
                                current.children.push(new_node);
                                current = current.children.last_mut().expect("Just pushed child");
                            }
                        }
                    }

                    current.files.push(it_idx);
                }

                // 4. Flatten Tree
                let mut items = Vec::new();
                fn flatten(
                    node: &TreeNode,
                    depth: usize,
                    items: &mut Vec<VisualItem>,
                    jobs_to_process: &[&JobRow],
                    original_indices: &[usize],
                ) {
                    // Helper to find first file in a node
                    fn find_any_file(node: &TreeNode) -> Option<usize> {
                        if let Some(&idx) = node.files.first() {
                            return Some(idx);
                        }
                        for child in &node.children {
                            if let Some(idx) = find_any_file(child) {
                                return Some(idx);
                            }
                        }
                        None
                    }

                    // Folders
                    for child in &node.children {
                        let first_job = find_any_file(child);
                        items.push(VisualItem {
                            text: format!("{}/", child.name),
                            index_in_jobs: None,
                            is_folder: true,
                            depth,
                            first_job_index: first_job,
                        });
                        flatten(child, depth + 1, items, jobs_to_process, original_indices);
                    }

                    // Files (Relative to jobs_to_process)
                    for &it_idx in &node.files {
                        let job = jobs_to_process[it_idx];
                        let orig_idx = original_indices[it_idx];
                        let filename = display_leaf_name(&job.source_path);

                        items.push(VisualItem {
                            text: filename,
                            index_in_jobs: Some(orig_idx),
                            is_folder: false,
                            depth,
                            first_job_index: None,
                        });
                    }
                }

                flatten(&root, 0, &mut items, &jobs_to_process, &original_indices);
                items
            }
        }
    }

    pub fn browser_move_down(&mut self) {
        let filter = if self.input_mode == InputMode::Filter {
            self.input_buffer.to_lowercase()
        } else {
            String::new()
        };

        if filter.is_empty() {
            self.picker.move_down();
            return;
        }

        let filtered_indices: Vec<usize> = self
            .picker
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                if e.is_parent {
                    return true;
                }
                let matches_name = e.name.to_lowercase().contains(&filter);
                if self.picker.search_recursive {
                    let rel_path = e
                        .path
                        .strip_prefix(&self.picker.cwd)
                        .map(|p| p.to_string_lossy().to_lowercase())
                        .unwrap_or_else(|_| e.name.to_lowercase());
                    matches_name || rel_path.contains(&filter)
                } else {
                    matches_name
                }
            })
            .map(|(i, _)| i)
            .collect();

        if filtered_indices.is_empty() {
            return;
        }

        let current_pos = filtered_indices
            .iter()
            .position(|i| *i == self.picker.selected)
            .unwrap_or(0);
        if current_pos + 1 < filtered_indices.len() {
            self.picker.selected = filtered_indices[current_pos + 1];
        }
    }

    pub fn browser_move_up(&mut self) {
        let filter = if self.input_mode == InputMode::Filter {
            self.input_buffer.to_lowercase()
        } else {
            String::new()
        };

        if filter.is_empty() {
            self.picker.move_up();
            return;
        }

        let filtered_indices: Vec<usize> = self
            .picker
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                if e.is_parent {
                    return true;
                }
                let matches_name = e.name.to_lowercase().contains(&filter);
                if self.picker.search_recursive {
                    let rel_path = e
                        .path
                        .strip_prefix(&self.picker.cwd)
                        .map(|p| p.to_string_lossy().to_lowercase())
                        .unwrap_or_else(|_| e.name.to_lowercase());
                    matches_name || rel_path.contains(&filter)
                } else {
                    matches_name
                }
            })
            .map(|(i, _)| i)
            .collect();

        if filtered_indices.is_empty() {
            return;
        }

        let current_pos = filtered_indices
            .iter()
            .position(|i| *i == self.picker.selected)
            .unwrap_or(0);
        if current_pos > 0 {
            self.picker.selected = filtered_indices[current_pos - 1];
        } else {
            self.picker.selected = filtered_indices[0];
        }
    }

    pub fn recalibrate_picker_selection(&mut self) {
        let filter = if self.input_mode == InputMode::Filter {
            self.input_buffer.to_lowercase()
        } else {
            return;
        };
        if filter.is_empty() {
            return;
        }

        let filtered_indices: Vec<usize> = self
            .picker
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                if e.is_parent {
                    return true;
                }
                let matches_name = e.name.to_lowercase().contains(&filter);
                if self.picker.search_recursive {
                    let rel_path = e
                        .path
                        .strip_prefix(&self.picker.cwd)
                        .map(|p| p.to_string_lossy().to_lowercase())
                        .unwrap_or_else(|_| e.name.to_lowercase());
                    matches_name || rel_path.contains(&filter)
                } else {
                    matches_name
                }
            })
            .map(|(i, _)| i)
            .collect();

        if !filtered_indices.is_empty() && !filtered_indices.contains(&self.picker.selected) {
            self.picker.selected = filtered_indices[0];
        }
    }

    pub async fn set_log_level(&mut self, level: &str) {
        if let Some(handle) = &self.log_handle {
            use tracing_subscriber::EnvFilter;
            let new_filter = EnvFilter::new(level);
            if let Err(e) = handle.reload(new_filter) {
                self.status_message = format!("Failed to set log level: {}", e);
            } else {
                self.current_log_level = level.to_string();
                self.status_message = format!("Log level set to: {}", level);

                // Persist to config and DB
                {
                    let mut cfg = self.config.lock().await;
                    cfg.log_level = level.to_string();
                    if let Ok(conn) = self.conn.lock()
                        && let Err(e) = crate::core::config::save_config_to_db(&conn, &cfg)
                    {
                        tracing::error!("Failed to save log level to DB: {}", e);
                    }
                }
            }
        }
    }

    pub async fn toggle_pause_selected_job(&mut self) {
        let job_id = if self.view_mode == ViewMode::Flat {
            if self.selected < self.jobs.len() {
                self.jobs[self.selected].id
            } else {
                return;
            }
        } else {
            // Tree mode logic: find job ID from visual item
            // Assuming simple mapping for now or finding from visual item
            // VisualItem has index_in_jobs, which is index into self.jobs (if flattened first?)
            // Actually self.visual_jobs[self.selected].index_in_jobs -> self.jobs[idx].id
            if self.selected < self.visual_jobs.len() {
                if let Some(idx) = self.visual_jobs[self.selected].index_in_jobs {
                    if idx < self.jobs.len() {
                        self.jobs[idx].id
                    } else {
                        return;
                    }
                } else {
                    return; // Folder selected?
                }
            } else {
                return;
            }
        };

        // Get current status
        let status = {
            let conn = if let Ok(c) = self.conn.lock() {
                c
            } else {
                return;
            };
            if let Ok(Some(job)) = crate::db::get_job(&conn, job_id) {
                job.status
            } else {
                return;
            }
        };

        if status == "paused" {
            let conn = if let Ok(c) = self.conn.lock() {
                c
            } else {
                return;
            };
            if let Err(e) = crate::db::resume_job(&conn, job_id) {
                self.status_message = format!("Failed to resume: {}", e);
            } else {
                self.status_message = format!("Resumed job {}", job_id);
            }
        } else if status == "uploading"
            || status == "transferring"
            || status == "scanning"
            || status == "queued"
            || status == "staged"
            || status == "ready"
        {
            let res = {
                let conn = if let Ok(c) = self.conn.lock() {
                    c
                } else {
                    return;
                };
                crate::db::pause_job(&conn, job_id)
            };

            if let Err(e) = res {
                self.status_message = format!("Failed to pause: {}", e);
            } else {
                self.status_message = format!("Paused job {}", job_id);
                // Trigger cancellation if running
                {
                    let tokens = self.cancellation_tokens.lock().await;
                    if let Some(token) = tokens.get(&job_id) {
                        token.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        } else {
            self.status_message = "Cannot pause/resume this job (invalid state)".to_string();
        }
    }

    pub fn change_selected_job_priority(&mut self, delta: i64) {
        let job_id = if self.view_mode == ViewMode::Flat {
            if self.selected < self.jobs.len() {
                self.jobs[self.selected].id
            } else {
                return;
            }
        } else if self.selected < self.visual_jobs.len() {
            if let Some(idx) = self.visual_jobs[self.selected].index_in_jobs {
                if idx < self.jobs.len() {
                    self.jobs[idx].id
                } else {
                    return;
                }
            } else {
                return;
            }
        } else {
            return;
        };

        let conn_arc = self.conn.clone();
        let conn = if let Ok(c) = conn_arc.lock() {
            c
        } else {
            return;
        };

        if let Ok(Some(job)) = crate::db::get_job(&conn, job_id) {
            let new_priority = job.priority + delta;
            if let Err(e) = crate::db::set_job_priority(&conn, job_id, new_priority) {
                self.status_message = format!("Failed to set priority: {}", e);
            } else {
                self.status_message = format!("Priority set to {}", new_priority);
                if let Err(e) = self.refresh_jobs(&conn) {
                    self.status_message = format!("Failed to refresh jobs: {}", e);
                }

                if let Some(pos) = self.jobs.iter().position(|j| j.id == job_id)
                    && self.view_mode == ViewMode::Flat
                {
                    self.selected = pos;
                }
            }
        }
    }
}
