use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, atomic::AtomicBool};
use std::time::{Duration, Instant};
use std::sync::mpsc;
use anyhow::Result;
use rusqlite::Connection;

use crate::db::{JobRow, list_active_jobs, list_history_jobs, list_quarantined_jobs};
use crate::components::file_picker::{FilePicker};
use crate::components::wizard::WizardState;
use crate::core::config::Config;
use crate::core::metrics::{MetricsCollector, HostMetricsSnapshot};
use crate::app::settings::SettingsState;
use crate::services::uploader::S3Object;
use crate::utils::lock_mutex;
use crate::ui::theme::Theme;
use crate::services::watch::Watcher;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppTab {
    Transfers,
    Quarantine,
    Remote,
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
    Remote,           // Only in Remote tab
    Logs,             // Only in Logs tab
    SettingsCategory, // Switch categories
    SettingsFields,   // Edit fields
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModalAction {
    None,
    ClearHistory,
    CancelJob(i64),
    DeleteRemoteObject(String),
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
    Browsing, // In File Picker
    Filter,   // In File Picker Filter
    LogSearch, // In Log Viewer Search
    Confirmation, // Modal
    LayoutAdjust, // Popout
    QueueSearch,  // In Transfer Queue
    HistorySearch, // In Job History
}

#[derive(Debug)]
pub enum AppEvent {
    Notification(String),
    RemoteFileList(Vec<S3Object>),
    LogLine(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutTarget {
    Hopper,
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
    pub selected: usize,
    pub selected_history: usize,
    pub quarantine: Vec<JobRow>,
    pub selected_quarantine: usize,
    
    pub s3_objects: Vec<S3Object>,
    pub selected_remote: usize,

    pub last_refresh: Instant,
    pub status_message: String,
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
    pub config: Arc<Mutex<Config>>,
    pub clamav_status: Arc<Mutex<String>>,
    pub progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>,
    pub cancellation_tokens: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>,

    // Logs
    pub logs: VecDeque<String>,
    pub logs_scroll: usize,
    pub logs_stick_to_bottom: bool,
    pub log_search_active: bool,
    pub log_search_query: String,
    pub log_search_results: Vec<usize>, // Indicies of matching lines
    pub log_search_current: usize, // Index in log_search_results

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
    pub theme: Theme,
    pub theme_names: Vec<&'static str>,
    // Modal State
    pub pending_action: ModalAction,
    pub confirmation_msg: String,

    // Metrics
    pub metrics: MetricsCollector,
    pub last_metrics: HostMetricsSnapshot,
    pub last_metrics_refresh: Instant,

    // Mouse Interaction
    pub last_click_time: Option<Instant>,
    pub last_click_pos: Option<(u16, u16)>,

    // Layout Adjustment
    pub layout_adjust_target: Option<LayoutTarget>,
    pub layout_adjust_message: String,
}

impl App {
    pub fn new(
        conn: Arc<Mutex<Connection>>,
        config: Arc<Mutex<Config>>,
        progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>,
        cancellation_tokens: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>,
    ) -> Result<Self> {
        let cfg_guard = lock_mutex(&config)?;
        let watcher_cfg = cfg_guard.clone();
        let settings = SettingsState::from_config(&cfg_guard);
        let theme = Theme::from_name(&cfg_guard.theme);
        drop(cfg_guard);

        let (tx, rx) = mpsc::channel();

        // Need access to conn for Watcher
        // The original code passed `conn` (Arc<Mutex>) to Watcher::new.
        // `Watcher::new` takes `Arc<Mutex<Connection>>`.
        
        let mut app = Self {
            jobs: Vec::new(),
            history: Vec::new(),
            quarantine: Vec::new(),
            selected: 0,
            selected_history: 0,
            selected_quarantine: 0,
            
            s3_objects: Vec::new(),
            selected_remote: 0,
            
            last_refresh: Instant::now() - Duration::from_secs(5),
            status_message: "Ready".to_string(),
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            history_filter: HistoryFilter::All,
            picker: FilePicker::new(),
            watch_enabled: false,
            _watch_path: None,
            _watch_seen: HashSet::new(),
            last_watch_scan: Instant::now() - Duration::from_secs(10),
            _watcher: Watcher::new(conn.clone(), watcher_cfg),
            current_tab: AppTab::Transfers,
            focus: AppFocus::Browser,
            settings,
            config: config.clone(),
            clamav_status: Arc::new(Mutex::new("Checking...".to_string())),
            progress: progress.clone(),
            async_rx: rx,
            async_tx: tx,
            show_wizard: false,
            wizard: WizardState::new(),
            theme,
            theme_names: Theme::list_names(),
            pending_action: ModalAction::None,
            confirmation_msg: String::new(),

            metrics: MetricsCollector::new(),
            last_metrics: HostMetricsSnapshot::default(),
            last_metrics_refresh: Instant::now(),
            cancellation_tokens,
            view_mode: ViewMode::Flat,
            visual_jobs: Vec::new(),
            visual_history: Vec::new(),
            last_click_time: None,
            last_click_pos: None,
            layout_adjust_target: None,
            layout_adjust_message: String::new(),

            logs: VecDeque::new(),
            logs_scroll: 0,
            logs_stick_to_bottom: true,
            log_search_active: false,
            log_search_query: String::new(),
            log_search_results: Vec::new(),
            log_search_current: 0,

            queue_search_query: String::new(),
            history_search_query: String::new(),
        };

        // ClamAV checker thread
        let status_clone = app.clamav_status.clone();
        let config_clone = config.clone();
        std::thread::spawn(move || {
            loop {
                // Read config for host/port
                let (host, port) = if let Ok(cfg) = lock_mutex(&config_clone) {
                    (cfg.clamd_host.clone(), cfg.clamd_port)
                } else {
                    ("localhost".into(), 3310)
                };
                let addr = format!("{}:{}", host, port);
                let status = if std::net::TcpStream::connect_timeout(
                    &addr.parse().unwrap_or_else(|_| "127.0.0.1:3310".parse().expect("Default address is valid")),
                    Duration::from_secs(1),
                )
                .is_ok()
                {
                    "Connected"
                } else {
                    "Disconnected"
                };

                if let Ok(mut s) = lock_mutex(&status_clone) {
                    *s = status.to_string();
                }
                std::thread::sleep(Duration::from_secs(5));
            }
        });

        app.picker.refresh();

        Ok(app)
    }

    pub fn refresh_jobs(&mut self, conn: &Connection) -> Result<()> {
        self.jobs = list_active_jobs(conn, 100)?;
        let filter_str = if self.history_filter == HistoryFilter::All {
            None
        } else {
            Some(self.history_filter.as_str())
        };
        self.history = list_history_jobs(conn, 100, filter_str)?;
        self.quarantine = list_quarantined_jobs(conn, 100)?;

        self.rebuild_visual_lists();

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

    pub fn rebuild_visual_lists(&mut self) {
        let q_query = self.queue_search_query.to_lowercase();
        self.visual_jobs = self.build_visual_list(&self.jobs, &q_query);
        let h_query = self.history_search_query.to_lowercase();
        self.visual_history = self.build_visual_list(&self.history, &h_query);
    }

    pub fn build_visual_list(&self, jobs: &[JobRow], filter_query: &str) -> Vec<VisualItem> {
        let filtered_jobs: Vec<(usize, &JobRow)> = if filter_query.is_empty() {
            jobs.iter().enumerate().collect()
        } else {
            jobs.iter()
                .enumerate()
                .filter(|(_, job)| {
                    job.source_path.to_lowercase().contains(filter_query) ||
                    Path::new(&job.source_path)
                        .file_name()
                        .map(|n| n.to_string_lossy().to_lowercase().contains(filter_query))
                        .unwrap_or(false)
                })
                .collect()
        };

        let (jobs_to_process, original_indices): (Vec<&JobRow>, Vec<usize>) = filtered_jobs
            .iter()
            .map(|(idx, job)| (*job, *idx))
            .unzip();

        match self.view_mode {
            ViewMode::Flat => {
                filtered_jobs.into_iter()
                    .map(|(i, job)| {
                        // Extract filename for display
                        let name = Path::new(&job.source_path)
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_else(|| job.source_path.clone());

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
                // We must scan ALL paths since we aren't sorting anymore.
                let paths: Vec<&Path> = jobs_to_process.iter().map(|j| Path::new(&j.source_path)).collect();
                let p0 = paths[0];
                let mut common_components: Vec<_> = p0.components().collect();

                for p in &paths[1..] {
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

                let common_path_buf: std::path::PathBuf = common_components.iter().collect();

                // 2. Heuristic for Base Path

                // If common prefix is exactly a file path (e.g. single file in list),
                // we should start "view" at its parent folder to show structure.
                let common_is_file = jobs_to_process
                    .iter()
                    .any(|j| Path::new(&j.source_path) == common_path_buf);

                let effective_common_path = if common_is_file {
                    common_path_buf
                        .parent()
                        .unwrap_or(&common_path_buf)
                        .to_path_buf()
                } else {
                    common_path_buf.clone()
                };

                // Check depths relative to effective common prefix
                let max_depth = paths
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
                let base_path_buf = if max_depth <= 1 && common_path_buf.components().count() > 0 {
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
                        .map(|c| c.as_os_str().to_string_lossy().to_string())
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
                                                let f_name = std::path::Path::new(f_path)
                                                    .file_name()
                                                    .unwrap_or_default()
                                                    .to_string_lossy();
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
                                current = current.children.last_mut().expect("Merge logic guarantees child exists");
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
                        let filename = std::path::Path::new(&job.source_path)
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_else(|| job.source_path.clone());

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
                if e.is_parent { return true; }
                let matches_name = e.name.to_lowercase().contains(&filter);
                if self.picker.search_recursive {
                    let rel_path = e.path.strip_prefix(&self.picker.cwd)
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
                if e.is_parent { return true; }
                let matches_name = e.name.to_lowercase().contains(&filter);
                if self.picker.search_recursive {
                    let rel_path = e.path.strip_prefix(&self.picker.cwd)
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
                if e.is_parent { return true; }
                let matches_name = e.name.to_lowercase().contains(&filter);
                if self.picker.search_recursive {
                    let rel_path = e.path.strip_prefix(&self.picker.cwd)
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
}
