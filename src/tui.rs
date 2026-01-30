use crate::config::Config;
use crate::db::{JobRow, list_active_jobs, list_history_jobs, list_quarantined_jobs};
use crate::ingest::ingest_path;
use crate::metrics::{HostMetricsSnapshot, MetricsCollector};
use crate::state::ProgressInfo;
use crate::theme::{StatusKind, Theme};
use crate::watch::Watcher;
use anyhow::Result;
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers, MouseButton,
    MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Wrap};
use std::sync::mpsc;

fn centered_rect(
    r: ratatui::layout::Rect,
    percent_x: u16,
    percent_y: u16,
) -> ratatui::layout::Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

use ratatui::Terminal;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettingsCategory {
    S3,
    Scanner,
    Performance,
    Theme,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppTab {
    Transfers,
    Quarantine,
    Settings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WizardStep {
    Paths,
    Scanner,
    S3,
    Performance,
    Done,
}

#[derive(Debug, Clone)]
struct WizardState {
    step: WizardStep,
    field: usize,
    editing: bool,
    // Paths
    staging_dir: String,
    quarantine_dir: String,
    // Scanner
    clamd_host: String,
    clamd_port: String,
    // S3
    bucket: String,
    region: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    // Performance
    part_size: String,
    concurrency: String,
}

impl WizardState {
    fn new() -> Self {
        Self {
            step: WizardStep::Paths,
            field: 0,
            editing: false,
            staging_dir: "./staging".to_string(),
            quarantine_dir: "./quarantine".to_string(),
            clamd_host: "127.0.0.1".to_string(),
            clamd_port: "3310".to_string(),
            bucket: String::new(),
            region: "us-east-1".to_string(),
            endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            part_size: "64".to_string(),
            concurrency: "4".to_string(),
        }
    }

    fn field_count(&self) -> usize {
        match self.step {
            WizardStep::Paths => 2,
            WizardStep::Scanner => 2,
            WizardStep::S3 => 5,
            WizardStep::Performance => 2,
            WizardStep::Done => 0,
        }
    }

    fn get_field_mut(&mut self) -> Option<&mut String> {
        match (self.step, self.field) {
            (WizardStep::Paths, 0) => Some(&mut self.staging_dir),
            (WizardStep::Paths, 1) => Some(&mut self.quarantine_dir),
            (WizardStep::Scanner, 0) => Some(&mut self.clamd_host),
            (WizardStep::Scanner, 1) => Some(&mut self.clamd_port),
            (WizardStep::S3, 0) => Some(&mut self.bucket),
            (WizardStep::S3, 1) => Some(&mut self.region),
            (WizardStep::S3, 2) => Some(&mut self.endpoint),
            (WizardStep::S3, 3) => Some(&mut self.access_key),
            (WizardStep::S3, 4) => Some(&mut self.secret_key),
            (WizardStep::Performance, 0) => Some(&mut self.part_size),
            (WizardStep::Performance, 1) => Some(&mut self.concurrency),
            _ => None,
        }
    }

    fn next_step(&mut self) {
        self.step = match self.step {
            WizardStep::Paths => WizardStep::Scanner,
            WizardStep::Scanner => WizardStep::S3,
            WizardStep::S3 => WizardStep::Performance,
            WizardStep::Performance => WizardStep::Done,
            WizardStep::Done => WizardStep::Done,
        };
        self.field = 0;
    }

    fn prev_step(&mut self) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppFocus {
    Rail,
    Browser, // Only in Transfers tab
    Queue,   // Only in Transfers tab
    History,
    Quarantine,       // Only in Quarantine tab
    SettingsCategory, // Switch categories
    SettingsFields,   // Edit fields
}

struct SettingsState {
    endpoint: String,
    bucket: String,
    region: String,
    prefix: String,
    access_key: String,
    secret_key: String,
    clamd_host: String,
    clamd_port: String,
    scan_chunk_size: String,
    part_size: String,
    concurrency_global: String,
    scan_concurrency: String,
    active_category: SettingsCategory,
    selected_field: usize,
    editing: bool,
    theme: String,
    original_theme: Option<String>,
    scanner_enabled: bool,
    host_metrics_enabled: bool,
    staging_mode_direct: bool, // true = Direct mode, false = Copy mode
    delete_source_after_upload: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalAction {
    None,
    ClearHistory,
    CancelJob(i64),
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
    Browsing, // Actively navigating in Hopper
    Filter,
    Confirmation,
    LayoutAdjust,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PickerView {
    List,
    Tree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayoutTarget {
    Hopper,
    Queue,
    History,
}

#[derive(Clone)]
struct FileEntry {
    name: String,
    path: PathBuf,
    is_dir: bool,
    is_parent: bool,
    depth: usize,
    size: u64,
    modified: Option<SystemTime>,
}

struct FilePicker {
    cwd: PathBuf,
    entries: Vec<FileEntry>,
    selected: usize,
    last_error: Option<String>,
    view: PickerView,
    expanded: HashSet<PathBuf>,
    selected_paths: HashSet<PathBuf>,
    search_recursive: bool,
    is_searching: bool,
}

impl FilePicker {
    fn new() -> Self {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let mut expanded = HashSet::new();
        expanded.insert(cwd.clone());
        Self {
            cwd,
            entries: Vec::new(),
            selected: 0,
            last_error: None,
            view: PickerView::List,
            expanded,
            selected_paths: HashSet::new(),
            search_recursive: false,
            is_searching: false,
        }
    }

    fn refresh(&mut self) {
        if self.is_searching && self.search_recursive {
            match self.load_dir_recursive(&self.cwd) {
                Ok(entries) => {
                    self.entries = entries;
                    self.last_error = None;
                }
                Err(err) => {
                    self.entries.clear();
                    self.last_error = Some(err.to_string());
                }
            }
        } else {
            let result = match self.view {
                PickerView::List => self.load_dir_list(&self.cwd),
                PickerView::Tree => self.load_dir_tree(&self.cwd),
            };
            match result {
                Ok(entries) => {
                    self.entries = entries;
                    self.last_error = None;
                }
                Err(err) => {
                    self.entries.clear();
                    self.last_error = Some(err.to_string());
                }
            }
        }

        if self.selected >= self.entries.len() {
            self.selected = self.entries.len().saturating_sub(1);
        }
        if self.entries.is_empty() {
            self.selected = 0;
        } else if self.selected == 0 && self.entries.len() > 1 && self.entries[0].is_parent {
            // Skip '..' parent on initial load
            self.selected = 1;
        }
    }

    fn try_set_cwd(&mut self, path: PathBuf) {
        match self.load_dir_list(&path) {
            Ok(entries) => {
                self.cwd = path;
                self.entries = entries;
                // Start on first real entry (skip '..') if possible
                self.selected = if self.entries.len() > 1 { 1 } else { 0 };
                self.last_error = None;
                self.expanded.insert(self.cwd.clone());
                if self.view == PickerView::Tree {
                    self.entries = match self.load_dir_tree(&self.cwd) {
                        Ok(tree) => tree,
                        Err(err) => {
                            self.last_error = Some(err.to_string());
                            Vec::new()
                        }
                    };
                }
            }
            Err(err) => {
                self.last_error = Some(err.to_string());
            }
        }
    }

    fn load_dir_list(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let mut entries = Vec::new();
        if let Some(parent) = path.parent() {
            entries.push(FileEntry {
                name: "..".to_string(),
                path: parent.to_path_buf(),
                is_dir: true,
                is_parent: true,
                depth: 0,
                size: 0,
                modified: None,
            });
        }
        for entry in fs::read_dir(path)? {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let metadata = fs::metadata(&path).ok();
            let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
            let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            let modified = metadata.and_then(|m| m.modified().ok());

            entries.push(FileEntry {
                name,
                path,
                is_dir,
                is_parent: false,
                depth: 0,
                size,
                modified,
            });
        }
        entries.sort_by(
            |a, b| match (a.is_parent, b.is_parent, a.is_dir, b.is_dir) {
                (true, false, _, _) => std::cmp::Ordering::Less,
                (false, true, _, _) => std::cmp::Ordering::Greater,
                (_, _, true, false) => std::cmp::Ordering::Less,
                (_, _, false, true) => std::cmp::Ordering::Greater,
                _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
            },
        );
        Ok(entries)
    }

    fn load_dir_tree(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let mut entries = Vec::new();
        if let Some(parent) = path.parent() {
            entries.push(FileEntry {
                name: "..".to_string(),
                path: parent.to_path_buf(),
                is_dir: true,
                is_parent: true,
                depth: 0,
                size: 0,
                modified: None,
            });
        }
        self.build_tree(path, 0, 4, &mut entries)?;
        Ok(entries)
    }

    fn load_dir_recursive(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let mut entries = Vec::new();
        let mut stack = vec![(path.to_path_buf(), 0)];
        let max_files = 5000;
        let max_depth = 10;

        while let Some((curr_path, depth)) = stack.pop() {
            if entries.len() >= max_files || depth > max_depth {
                continue;
            }

            if let Ok(dir_entries) = fs::read_dir(curr_path) {
                for entry in dir_entries {
                    if let Ok(entry) = entry {
                        let p = entry.path();
                        let name = entry.file_name().to_string_lossy().to_string();
                        let metadata = fs::metadata(&p).ok();
                        let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
                        let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
                        let modified = metadata.and_then(|m| m.modified().ok());

                        entries.push(FileEntry {
                            name,
                            path: p.clone(),
                            is_dir,
                            is_parent: false,
                            depth,
                            size,
                            modified,
                        });

                        if is_dir {
                            stack.push((p, depth + 1));
                        }
                    }
                }
            }
        }

        entries.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        Ok(entries)
    }

    fn build_tree(
        &self,
        path: &Path,
        depth: usize,
        max_depth: usize,
        entries: &mut Vec<FileEntry>,
    ) -> Result<()> {
        let dir_entries = match fs::read_dir(path) {
            Ok(e) => e,
            Err(_) => return Ok(()),
        };

        let mut children = Vec::new();
        for entry in dir_entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let metadata = fs::metadata(&path).ok();
            let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
            let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            let modified = metadata.and_then(|m| m.modified().ok());

            children.push(FileEntry {
                name,
                path,
                is_dir,
                is_parent: false,
                depth,
                size,
                modified,
            });
        }
        children.sort_by(|a, b| match (a.is_dir, b.is_dir) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
        });

        for child in children {
            let is_expanded = child.is_dir && self.expanded.contains(&child.path);
            let child_path = child.path.clone();
            entries.push(child);
            if is_expanded && depth + 1 < max_depth {
                let _ = self.build_tree(&child_path, depth + 1, max_depth, entries);
            }
        }
        Ok(())
    }

    fn selected_entry(&self) -> Option<&FileEntry> {
        self.entries.get(self.selected)
    }

    fn move_up(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
        }
    }

    fn move_down(&mut self) {
        if self.selected + 1 < self.entries.len() {
            self.selected += 1;
        }
    }

    fn page_up(&mut self) {
        let step = 10;
        if self.selected >= step {
            self.selected -= step;
        } else {
            self.selected = 0;
        }
    }

    fn page_down(&mut self) {
        let step = 10;
        self.selected = (self.selected + step).min(self.entries.len().saturating_sub(1));
    }

    fn go_parent(&mut self) {
        if let Some(parent) = self.cwd.parent() {
            self.try_set_cwd(parent.to_path_buf());
        }
    }

    fn toggle_view(&mut self) {
        self.view = match self.view {
            PickerView::List => PickerView::Tree,
            PickerView::Tree => PickerView::List,
        };
        self.refresh();
    }

    fn toggle_expand(&mut self) {
        let entry = self.selected_entry().cloned();
        if let Some(entry) = entry {
            if entry.is_dir && !entry.is_parent {
                if self.expanded.contains(&entry.path) {
                    self.expanded.remove(&entry.path);
                } else {
                    self.expanded.insert(entry.path);
                }
                if self.view == PickerView::Tree {
                    self.refresh();
                }
            }
        }
    }

    fn toggle_select(&mut self) {
        let entry = self.selected_entry().cloned();
        if let Some(entry) = entry {
            if entry.is_parent {
                return;
            }
            if self.selected_paths.contains(&entry.path) {
                self.selected_paths.remove(&entry.path);
            } else {
                self.selected_paths.insert(entry.path);
            }
        }
    }

    fn clear_selected(&mut self) {
        self.selected_paths.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryFilter {
    All,
    Complete,
    Quarantined,
}

impl HistoryFilter {
    fn next(&self) -> Self {
        match self {
            HistoryFilter::All => HistoryFilter::Complete,
            HistoryFilter::Complete => HistoryFilter::Quarantined,
            HistoryFilter::Quarantined => HistoryFilter::All,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            HistoryFilter::All => "All",
            HistoryFilter::Complete => "Complete",
            HistoryFilter::Quarantined => "Quarantined",
        }
    }
}

pub struct App {
    pub jobs: Vec<JobRow>,
    pub history: Vec<JobRow>,
    pub selected: usize,
    pub selected_history: usize,
    quarantine: Vec<JobRow>,
    selected_quarantine: usize,
    last_refresh: Instant,
    status_message: String,
    input_mode: InputMode,
    input_buffer: String,
    history_filter: HistoryFilter,
    picker: FilePicker,
    watch_enabled: bool,
    _watch_path: Option<PathBuf>,
    _watch_seen: HashSet<PathBuf>,
    last_watch_scan: Instant,
    _watcher: Watcher,
    current_tab: AppTab,
    focus: AppFocus,
    settings: SettingsState,
    config: Arc<Mutex<Config>>,
    clamav_status: Arc<Mutex<String>>,
    progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>,
    pub cancellation_tokens: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>,

    // Visualization
    pub view_mode: ViewMode,
    pub visual_jobs: Vec<VisualItem>,
    pub visual_history: Vec<VisualItem>,

    // Async feedback channel
    pub async_rx: mpsc::Receiver<String>,
    pub async_tx: mpsc::Sender<String>,
    // Wizard
    show_wizard: bool,
    wizard: WizardState,
    theme: Theme,
    theme_names: Vec<&'static str>,
    // Modal State
    pub pending_action: ModalAction,
    pub confirmation_msg: String,

    // Metrics
    pub metrics: MetricsCollector,
    pub last_metrics: HostMetricsSnapshot,
    pub last_metrics_refresh: Instant,

    // Mouse Interaction
    last_click_time: Option<Instant>,
    last_click_pos: Option<(u16, u16)>,

    // Layout Adjustment
    layout_adjust_target: Option<LayoutTarget>,
    layout_adjust_message: String,
}

fn calculate_list_offset(selected: usize, total: usize, display_height: usize) -> usize {
    if total <= display_height {
        return 0;
    }
    let mut offset = 0;
    if selected >= display_height / 2 {
        offset = (selected + 1).saturating_sub(display_height / 2);
    }
    if offset + display_height > total {
        offset = total.saturating_sub(display_height);
    }
    offset
}

impl App {
    fn new(
        conn: Arc<Mutex<Connection>>,
        config: Arc<Mutex<Config>>,
        progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>,
        cancellation_tokens: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>,
    ) -> Self {
        let cfg_guard = config.lock().unwrap();
        let watcher_cfg = cfg_guard.clone();
        let settings = SettingsState::from_config(&cfg_guard);
        let theme = Theme::from_name(&cfg_guard.theme);
        drop(cfg_guard);

        let (tx, rx) = mpsc::channel();

        let mut app = Self {
            jobs: Vec::new(),
            history: Vec::new(),
            quarantine: Vec::new(),
            selected: 0,
            selected_history: 0,
            selected_quarantine: 0,
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
            _watcher: Watcher::new(conn, watcher_cfg),
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
        };

        let status_clone = app.clamav_status.clone();
        let config_clone = config.clone();
        std::thread::spawn(move || {
            loop {
                // Read config for host/port
                let (host, port) = {
                    let cfg = config_clone.lock().unwrap();
                    (cfg.clamd_host.clone(), cfg.clamd_port)
                };
                let addr = format!("{}:{}", host, port);
                let status = if std::net::TcpStream::connect_timeout(
                    &addr.parse().unwrap(),
                    Duration::from_secs(1),
                )
                .is_ok()
                {
                    "Connected"
                } else {
                    "Disconnected"
                };

                {
                    let mut s = status_clone.lock().unwrap();
                    *s = status.to_string();
                }
                std::thread::sleep(Duration::from_secs(5));
            }
        });

        app.picker.refresh();

        app
    }

    fn refresh_jobs(&mut self, conn: &Connection) -> Result<()> {
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

    fn rebuild_visual_lists(&mut self) {
        self.visual_jobs = self.build_visual_list(&self.jobs);
        self.visual_history = self.build_visual_list(&self.history);
    }

    fn build_visual_list(&self, jobs: &[JobRow]) -> Vec<VisualItem> {
        match self.view_mode {
            ViewMode::Flat => {
                jobs.iter()
                    .enumerate()
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
                if jobs.is_empty() {
                    return Vec::new();
                }

                // 1. Calculate Common Prefix
                // We must scan ALL paths since we aren't sorting anymore.
                let paths: Vec<&Path> = jobs.iter().map(|j| Path::new(&j.source_path)).collect();
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
                let common_is_file = jobs
                    .iter()
                    .any(|j| Path::new(&j.source_path) == &common_path_buf);

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

                for (idx, job) in jobs.iter().enumerate() {
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

                            if let Some(last) = current.children.last() {
                                if last.name == *comp {
                                    // Check collision: Does `last` already contain the file we are about to add?
                                    // We need to verify if `last` + `remaining_for_file` hits an existing file.
                                    // Helper closure for collision
                                    fn check_collision(
                                        node: &TreeNode,
                                        path: &[String],
                                        jobs: &[JobRow],
                                    ) -> bool {
                                        if path.len() == 1 {
                                            // Path is just [filename]. Check node.files.
                                            let filename = &path[0];
                                            for &f_idx in &node.files {
                                                let f_path = &jobs[f_idx].source_path;
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
                                            return check_collision(child, &path[1..], jobs);
                                        }
                                        false
                                    }

                                    if !check_collision(last, remaining_for_file, jobs) {
                                        should_merge = true;
                                    }
                                }
                            }

                            if should_merge {
                                current = current.children.last_mut().unwrap();
                            } else {
                                let new_node = TreeNode {
                                    name: comp.clone(),
                                    children: Vec::new(),
                                    files: Vec::new(),
                                };
                                current.children.push(new_node);
                                current = current.children.last_mut().unwrap();
                            }
                        }
                    }

                    current.files.push(idx);
                }

                // 4. Flatten Tree
                let mut items = Vec::new();
                fn flatten(
                    node: &TreeNode,
                    depth: usize,
                    items: &mut Vec<VisualItem>,
                    jobs: &[JobRow],
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
                        flatten(child, depth + 1, items, jobs);
                    }

                    // Files
                    for &idx in &node.files {
                        let job = &jobs[idx];
                        let filename = std::path::Path::new(&job.source_path)
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_else(|| job.source_path.clone());

                        items.push(VisualItem {
                            text: filename,
                            index_in_jobs: Some(idx),
                            is_folder: false,
                            depth,
                            first_job_index: None,
                        });
                    }
                }

                flatten(&root, 0, &mut items, jobs);
                items
            }
        }
    }

    fn browser_move_down(&mut self) {
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
            .filter(|(_, e)| e.is_parent || e.name.to_lowercase().contains(&filter))
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

    fn browser_move_up(&mut self) {
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
            .filter(|(_, e)| e.is_parent || e.name.to_lowercase().contains(&filter))
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

    fn recalibrate_picker_selection(&mut self) {
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
            .filter(|(_, e)| e.is_parent || e.name.to_lowercase().contains(&filter))
            .map(|(i, _)| i)
            .collect();

        if !filtered_indices.is_empty() && !filtered_indices.contains(&self.picker.selected) {
            self.picker.selected = filtered_indices[0];
        }
    }
}

pub fn run_tui(
    conn_mutex: Arc<Mutex<Connection>>,
    cfg: Arc<Mutex<Config>>,
    progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>,
    cancellation_tokens: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>,
    needs_wizard: bool,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut app = App::new(
        conn_mutex.clone(),
        cfg.clone(),
        progress,
        cancellation_tokens,
    );
    app.show_wizard = needs_wizard;

    {
        let conn = conn_mutex.lock().unwrap();
        app.refresh_jobs(&conn)?;
    }

    let tick_rate = Duration::from_millis(200);
    loop {
        // Refresh metrics periodically (e.g. every 1s) if enabled
        if app.config.lock().unwrap().host_metrics_enabled {
            if app.last_metrics_refresh.elapsed() >= Duration::from_secs(1) {
                app.last_metrics = app.metrics.refresh();
                app.last_metrics_refresh = Instant::now();
            }
        }

        terminal.draw(|f| ui(f, &app))?;

        if app.last_refresh.elapsed() > Duration::from_secs(1) {
            let conn = conn_mutex.lock().unwrap();
            app.refresh_jobs(&conn)?;
        }

        // Check for async messages (e.g. connection tests)
        if let Ok(msg) = app.async_rx.try_recv() {
            app.status_message = msg;
        }

        if app.watch_enabled && app.last_watch_scan.elapsed() > Duration::from_secs(2) {
            let _conn = conn_mutex.lock().unwrap();
            app.last_watch_scan = Instant::now();
        }

        if event::poll(tick_rate)? {
            let event = event::read()?;
            match event {
                Event::Key(key) => {
                    // Handle Layout Adjustment Mode
                    if app.input_mode == InputMode::LayoutAdjust {
                        match key.code {
                            KeyCode::Char('1') => {
                                app.layout_adjust_target = Some(LayoutTarget::Hopper);
                                update_layout_message(&mut app, LayoutTarget::Hopper);
                            }
                            KeyCode::Char('2') => {
                                app.layout_adjust_target = Some(LayoutTarget::Queue);
                                update_layout_message(&mut app, LayoutTarget::Queue);
                            }
                            KeyCode::Char('3') => {
                                app.layout_adjust_target = Some(LayoutTarget::History);
                                update_layout_message(&mut app, LayoutTarget::History);
                            }
                            KeyCode::Char('+') | KeyCode::Char('=') => {
                                if let Some(target) = app.layout_adjust_target {
                                    adjust_layout_dimension(&mut app.config, target, 1);
                                    update_layout_message(&mut app, target);
                                }
                            }
                            KeyCode::Char('-') | KeyCode::Char('_') => {
                                if let Some(target) = app.layout_adjust_target {
                                    adjust_layout_dimension(&mut app.config, target, -1);
                                    update_layout_message(&mut app, target);
                                }
                            }
                            KeyCode::Char('r') => {
                                if let Some(target) = app.layout_adjust_target {
                                    reset_layout_dimension(&mut app.config, target);
                                    update_layout_message(&mut app, target);
                                }
                            }
                            KeyCode::Char('R') => {
                                reset_all_layout_dimensions(&mut app.config);
                                app.layout_adjust_message =
                                    "All dimensions reset to defaults".to_string();
                            }
                            KeyCode::Char('s') => {
                                let conn = conn_mutex.lock().unwrap();
                                if let Err(e) = crate::config::save_config_to_db(
                                    &conn,
                                    &app.config.lock().unwrap(),
                                ) {
                                    app.status_message = format!("Save error: {}", e);
                                } else {
                                    app.status_message = "Layout saved".to_string();
                                }
                                app.input_mode = InputMode::Normal;
                                app.layout_adjust_target = None;
                            }
                            KeyCode::Esc | KeyCode::Char('q') => {
                                let conn = conn_mutex.lock().unwrap();
                                if let Ok(loaded_cfg) = crate::config::load_config_from_db(&conn) {
                                    *app.config.lock().unwrap() = loaded_cfg;
                                    app.status_message = "Layout changes discarded".to_string();
                                }
                                app.input_mode = InputMode::Normal;
                                app.layout_adjust_target = None;
                            }
                            _ => {}
                        }
                        continue;
                    }

                    // Handle Confirmation Mode
                    if app.input_mode == InputMode::Confirmation {
                        match key.code {
                            KeyCode::Char('y') | KeyCode::Enter => {
                                // Execute Action
                                match app.pending_action {
                                    ModalAction::ClearHistory => {
                                        let conn = conn_mutex.lock().unwrap();
                                        let filter_str = if app.history_filter == HistoryFilter::All
                                        {
                                            None
                                        } else {
                                            Some(app.history_filter.as_str())
                                        };
                                        if let Err(e) = crate::db::clear_history(&conn, filter_str)
                                        {
                                            app.status_message =
                                                format!("Error clearing history: {}", e);
                                        } else {
                                            app.selected_history = 0;
                                            app.status_message = format!(
                                                "History cleared ({})",
                                                app.history_filter.as_str()
                                            );
                                            let _ = app.refresh_jobs(&conn);
                                        }
                                    }
                                    ModalAction::CancelJob(id) => {
                                        // Trigger cancellation token
                                        if let Some(token) =
                                            app.cancellation_tokens.lock().unwrap().get(&id)
                                        {
                                            token.store(true, Ordering::Relaxed);
                                        }

                                        // Update DB status (Soft Delete / Cancel)
                                        let conn = conn_mutex.lock().unwrap();
                                        let _ = crate::db::cancel_job(&conn, id);
                                        app.status_message = format!("Cancelled job {}", id);

                                        // Refresh to update list
                                        let _ = app.refresh_jobs(&conn);
                                    }
                                    _ => {}
                                }

                                // Reset
                                app.input_mode = InputMode::Normal;
                                app.pending_action = ModalAction::None;
                                app.confirmation_msg.clear();
                            }
                            KeyCode::Char('n') | KeyCode::Esc => {
                                // Cancel
                                app.input_mode = InputMode::Normal;
                                app.pending_action = ModalAction::None;
                                app.confirmation_msg.clear();
                                app.status_message = "Action cancelled".to_string();
                            }
                            _ => {}
                        }
                        continue;
                    }

                    // Handle wizard mode separately
                    if app.show_wizard {
                        match key.code {
                            KeyCode::Char('q') if !app.wizard.editing => break,
                            KeyCode::Esc if app.wizard.editing => {
                                app.wizard.editing = false;
                            }
                            KeyCode::Enter => {
                                if app.wizard.step == WizardStep::Done {
                                    // Save config and close wizard
                                    let conn = conn_mutex.lock().unwrap();
                                    let mut cfg = Config::default();
                                    cfg.staging_dir = app.wizard.staging_dir.clone();
                                    cfg.quarantine_dir = app.wizard.quarantine_dir.clone();
                                    cfg.clamd_host = app.wizard.clamd_host.clone();
                                    cfg.clamd_port = app.wizard.clamd_port.parse().unwrap_or(3310);
                                    cfg.s3_bucket = if app.wizard.bucket.is_empty() {
                                        None
                                    } else {
                                        Some(app.wizard.bucket.clone())
                                    };
                                    cfg.s3_region = if app.wizard.region.is_empty() {
                                        None
                                    } else {
                                        Some(app.wizard.region.clone())
                                    };
                                    cfg.s3_endpoint = if app.wizard.endpoint.is_empty() {
                                        None
                                    } else {
                                        Some(app.wizard.endpoint.clone())
                                    };
                                    cfg.s3_access_key = if app.wizard.access_key.is_empty() {
                                        None
                                    } else {
                                        Some(app.wizard.access_key.clone())
                                    };
                                    cfg.s3_secret_key = if app.wizard.secret_key.is_empty() {
                                        None
                                    } else {
                                        Some(app.wizard.secret_key.clone())
                                    };
                                    cfg.part_size_mb = app.wizard.part_size.parse().unwrap_or(64);
                                    cfg.concurrency_upload_global =
                                        app.wizard.concurrency.parse().unwrap_or(4);
                                    cfg.concurrency_parts_per_file = 4;

                                    let _ = crate::config::save_config_to_db(&conn, &cfg);
                                    drop(conn);

                                    // Create directories if they don't exist
                                    let _ = std::fs::create_dir_all(&cfg.staging_dir);
                                    let _ = std::fs::create_dir_all(&cfg.quarantine_dir);
                                    let _ = std::fs::create_dir_all(&cfg.state_dir);

                                    // Update shared config
                                    let mut shared_cfg = app.config.lock().unwrap();
                                    *shared_cfg = cfg;
                                    drop(shared_cfg);

                                    // Reload settings state
                                    let cfg_guard = app.config.lock().unwrap();
                                    app.settings = SettingsState::from_config(&cfg_guard);

                                    app.show_wizard = false;
                                    app.status_message = "Setup complete!".to_string();
                                } else {
                                    app.wizard.editing = !app.wizard.editing;
                                }
                            }
                            KeyCode::Tab if !app.wizard.editing => {
                                app.wizard.next_step();
                            }
                            KeyCode::BackTab if !app.wizard.editing => {
                                app.wizard.prev_step();
                            }
                            KeyCode::Up if !app.wizard.editing => {
                                if app.wizard.field > 0 {
                                    app.wizard.field -= 1;
                                }
                            }
                            KeyCode::Down if !app.wizard.editing => {
                                if app.wizard.field + 1 < app.wizard.field_count() {
                                    app.wizard.field += 1;
                                }
                            }
                            KeyCode::Char(c) if app.wizard.editing => {
                                if let Some(field) = app.wizard.get_field_mut() {
                                    field.push(c);
                                }
                            }
                            KeyCode::Backspace if app.wizard.editing => {
                                if let Some(field) = app.wizard.get_field_mut() {
                                    field.pop();
                                }
                            }
                            _ => {}
                        }
                        continue;
                    }

                    // Determine if we are capturing input (e.g. editing settings, browsing, or in search)
                    let capturing_input = match app.focus {
                        AppFocus::SettingsFields => app.settings.editing,
                        AppFocus::Browser => {
                            app.input_mode == InputMode::Filter
                                || app.input_mode == InputMode::Browsing
                        }
                        _ => false,
                    };

                    // Meta keys (Quit, Tab)
                    if !capturing_input {
                        let old_focus = app.focus;
                        match key.code {
                            KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                                app.input_mode = InputMode::LayoutAdjust;
                                app.layout_adjust_target = None;
                                app.layout_adjust_message = "Layout Adjustment: 1=Hopper 2=Queue 3=History | +/- adjust | r reset | R reset-all | s save | q cancel".to_string();
                                continue;
                            }
                            KeyCode::Char('q') => break,
                            KeyCode::Tab => {
                                app.focus = match app.focus {
                                    AppFocus::Rail => match app.current_tab {
                                        AppTab::Transfers => AppFocus::Browser,
                                        AppTab::Quarantine => AppFocus::Quarantine,
                                        AppTab::Settings => AppFocus::SettingsCategory,
                                    },
                                    AppFocus::Browser => AppFocus::Queue,
                                    AppFocus::Queue => AppFocus::History,
                                    AppFocus::SettingsCategory => AppFocus::SettingsFields,
                                    AppFocus::History
                                    | AppFocus::Quarantine
                                    | AppFocus::SettingsFields => AppFocus::Rail,
                                };
                            }
                            KeyCode::BackTab => {
                                app.focus = match app.focus {
                                    AppFocus::Rail => match app.current_tab {
                                        AppTab::Transfers => AppFocus::History,
                                        AppTab::Quarantine => AppFocus::Quarantine,
                                        AppTab::Settings => AppFocus::SettingsFields,
                                    },
                                    AppFocus::History => AppFocus::Queue,
                                    AppFocus::Queue => AppFocus::Browser,
                                    AppFocus::SettingsFields => AppFocus::SettingsCategory,
                                    AppFocus::Browser
                                    | AppFocus::Quarantine
                                    | AppFocus::SettingsCategory => AppFocus::Rail,
                                };
                            }
                            KeyCode::Right => {
                                app.focus = match app.focus {
                                    AppFocus::Rail => match app.current_tab {
                                        AppTab::Transfers => AppFocus::Browser,
                                        AppTab::Quarantine => AppFocus::Quarantine,
                                        AppTab::Settings => AppFocus::SettingsCategory,
                                    },
                                    AppFocus::Browser => AppFocus::Queue,
                                    AppFocus::Queue => AppFocus::History,
                                    AppFocus::SettingsCategory => AppFocus::SettingsFields,
                                    _ => app.focus,
                                };
                            }
                            KeyCode::Left => {
                                app.focus = match app.focus {
                                    AppFocus::History => AppFocus::Queue,
                                    AppFocus::Queue => AppFocus::Browser,
                                    AppFocus::Browser
                                    | AppFocus::Quarantine
                                    | AppFocus::SettingsCategory => AppFocus::Rail,
                                    AppFocus::SettingsFields => AppFocus::SettingsCategory,
                                    _ => app.focus,
                                };
                            }
                            _ => {}
                        }
                        // If focus changed, don't also process this key in focus-specific handling
                        if app.focus != old_focus {
                            continue;
                        }
                    }

                    // Focus-specific handling
                    match app.focus {
                        AppFocus::Rail => match key.code {
                            KeyCode::Up | KeyCode::Char('k') => {
                                app.current_tab = match app.current_tab {
                                    AppTab::Transfers => AppTab::Settings,
                                    AppTab::Quarantine => AppTab::Transfers,
                                    AppTab::Settings => AppTab::Quarantine,
                                };
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                app.current_tab = match app.current_tab {
                                    AppTab::Transfers => AppTab::Quarantine,
                                    AppTab::Quarantine => AppTab::Settings,
                                    AppTab::Settings => AppTab::Transfers,
                                };
                            }
                            _ => {}
                        },
                        AppFocus::Browser => {
                            match app.input_mode {
                                InputMode::Normal => {
                                    // Browser is focused but not actively navigating
                                    match key.code {
                                        KeyCode::Char('a') | KeyCode::Enter => {
                                            app.input_mode = InputMode::Browsing;
                                            app.status_message = "Browsing files...".to_string();
                                        }
                                        KeyCode::Up | KeyCode::Char('k') => app.browser_move_up(),
                                        KeyCode::Down | KeyCode::Char('j') => {
                                            app.browser_move_down()
                                        }
                                        _ => {}
                                    }
                                }
                                InputMode::Browsing => {
                                    // Actively navigating directories
                                    match key.code {
                                        KeyCode::Esc | KeyCode::Char('q') => {
                                            app.input_mode = InputMode::Normal;
                                            app.status_message = "Ready".to_string();
                                        }
                                        KeyCode::Up | KeyCode::Char('k') => app.browser_move_up(),
                                        KeyCode::Down | KeyCode::Char('j') => {
                                            app.browser_move_down()
                                        }
                                        KeyCode::PageUp => app.picker.page_up(),
                                        KeyCode::PageDown => app.picker.page_down(),
                                        KeyCode::Left | KeyCode::Char('h') | KeyCode::Backspace => {
                                            app.picker.go_parent()
                                        }
                                        KeyCode::Right | KeyCode::Char('l') | KeyCode::Enter => {
                                            if let Some(entry) = app.picker.selected_entry() {
                                                if entry.is_dir {
                                                    if app.picker.view == PickerView::Tree
                                                        && key.code == KeyCode::Right
                                                    {
                                                        app.picker.toggle_expand();
                                                    } else {
                                                        app.picker.try_set_cwd(entry.path.clone());
                                                        app.input_buffer.clear();
                                                        app.picker.is_searching = false;
                                                    }
                                                } else if key.code == KeyCode::Enter
                                                    || key.code == KeyCode::Char('l')
                                                {
                                                    // Only queue files on Enter or 'l', not Right arrow
                                                    let conn = conn_mutex.lock().unwrap();
                                                    let cfg_guard = cfg.lock().unwrap();
                                                    let staging = cfg_guard.staging_dir.clone();
                                                    let staging_mode =
                                                        cfg_guard.staging_mode.clone();
                                                    drop(cfg_guard);
                                                    let _ = ingest_path(
                                                        &conn,
                                                        &staging,
                                                        &staging_mode,
                                                        &entry.path.to_string_lossy(),
                                                    );
                                                    app.status_message =
                                                        "File added to queue".to_string();
                                                }
                                                // Right arrow on a file does nothing
                                            }
                                        }
                                        KeyCode::Char('/') => {
                                            app.input_mode = InputMode::Filter;
                                            app.picker.search_recursive = true;
                                            app.picker.is_searching = true;
                                            app.picker.refresh();
                                            app.status_message = "Search (recursive)".to_string();
                                        }
                                        KeyCode::Char(' ') => app.picker.toggle_select(),
                                        KeyCode::Char('t') => app.picker.toggle_view(),
                                        KeyCode::Char('f') => {
                                            app.picker.search_recursive =
                                                !app.picker.search_recursive;
                                            app.picker.refresh();
                                            app.status_message = if app.picker.search_recursive {
                                                "Recursive search enabled"
                                            } else {
                                                "Recursive search disabled"
                                            }
                                            .to_string();
                                        }
                                        KeyCode::Char('c') => app.picker.clear_selected(),
                                        KeyCode::Char('s') => {
                                            let paths: Vec<PathBuf> =
                                                app.picker.selected_paths.iter().cloned().collect();
                                            if !paths.is_empty() {
                                                let conn = conn_mutex.lock().unwrap();
                                                let cfg_guard = cfg.lock().unwrap();
                                                let staging = cfg_guard.staging_dir.clone();
                                                let staging_mode = cfg_guard.staging_mode.clone();
                                                drop(cfg_guard);
                                                let mut total = 0;
                                                for path in paths {
                                                    if let Ok(count) = ingest_path(
                                                        &conn,
                                                        &staging,
                                                        &staging_mode,
                                                        &path.to_string_lossy(),
                                                    ) {
                                                        total += count;
                                                    }
                                                }
                                                app.status_message =
                                                    format!("Ingested {} items", total);
                                                app.picker.clear_selected();
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                InputMode::Filter => match key.code {
                                    KeyCode::Esc => {
                                        app.input_mode = InputMode::Browsing;
                                        app.input_buffer.clear();
                                        app.picker.is_searching = false;
                                        app.picker.search_recursive = false;
                                        app.picker.refresh();
                                    }
                                    KeyCode::Enter => {
                                        app.input_mode = InputMode::Browsing;
                                    }
                                    KeyCode::Up => app.browser_move_up(),
                                    KeyCode::Down => app.browser_move_down(),
                                    KeyCode::Backspace => {
                                        app.input_buffer.pop();
                                        app.picker.is_searching = !app.input_buffer.is_empty();
                                        if app.picker.search_recursive {
                                            app.picker.refresh();
                                        }
                                        app.recalibrate_picker_selection();
                                    }
                                    KeyCode::Char(c) => {
                                        app.input_buffer.push(c);
                                        app.picker.is_searching = true;
                                        if app.picker.search_recursive {
                                            app.picker.refresh();
                                        }
                                        app.recalibrate_picker_selection();
                                    }
                                    _ => {}
                                },
                                InputMode::Confirmation => {}
                                InputMode::LayoutAdjust => {}
                            }
                        }
                        AppFocus::Queue => {
                            match key.code {
                                KeyCode::Up | KeyCode::Char('k') => {
                                    if app.selected > 0 {
                                        app.selected -= 1;
                                    }
                                }
                                KeyCode::Down | KeyCode::Char('j') => {
                                    if app.selected + 1 < app.visual_jobs.len() {
                                        app.selected += 1;
                                    }
                                }
                                KeyCode::Char('t') => {
                                    app.view_mode = match app.view_mode {
                                        ViewMode::Flat => ViewMode::Tree,
                                        ViewMode::Tree => ViewMode::Flat,
                                    };
                                    app.status_message =
                                        format!("Switched to {:?} View", app.view_mode);
                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = app.refresh_jobs(&conn);
                                }
                                KeyCode::Char('R') => {
                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = app.refresh_jobs(&conn);
                                }
                                KeyCode::Char('r') => {
                                    if !app.visual_jobs.is_empty()
                                        && app.selected < app.visual_jobs.len()
                                    {
                                        if let Some(idx) =
                                            app.visual_jobs[app.selected].index_in_jobs
                                        {
                                            let id = app.jobs[idx].id;
                                            let conn = conn_mutex.lock().unwrap();
                                            let _ = crate::db::retry_job(&conn, id);
                                            app.status_message = format!("Retried job {}", id);
                                        }
                                    }
                                }
                                KeyCode::Char('d') | KeyCode::Delete => {
                                    if !app.visual_jobs.is_empty()
                                        && app.selected < app.visual_jobs.len()
                                    {
                                        if let Some(idx) =
                                            app.visual_jobs[app.selected].index_in_jobs
                                        {
                                            let job = &app.jobs[idx];
                                            let id = job.id;
                                            let is_active = job.status == "uploading"
                                                || job.status == "scanning"
                                                || job.status == "pending"
                                                || job.status == "queued";

                                            if is_active {
                                                // Require confirmation
                                                app.pending_action = ModalAction::CancelJob(id);
                                                app.confirmation_msg =
                                                    format!("Cancel active job #{}? (y/n)", id);
                                                app.input_mode = InputMode::Confirmation;
                                            } else {
                                                // Just do it (Soft delete/Cancel)
                                                let conn = conn_mutex.lock().unwrap();
                                                let _ = crate::db::cancel_job(&conn, id);
                                                app.status_message = format!("Removed job {}", id);
                                                let _ = app.refresh_jobs(&conn);
                                                // Selection adjustment happens in refresh_jobs (auto-fix) or we can decrement if at end
                                                // But standard behavior is fine.
                                            }
                                        }
                                    }
                                }
                                KeyCode::Char('c') => {
                                    // Clear all completed jobs
                                    let conn = conn_mutex.lock().unwrap();
                                    let ids_to_delete: Vec<i64> = app
                                        .jobs
                                        .iter()
                                        .filter(|j| j.status == "complete")
                                        .map(|j| j.id)
                                        .collect();
                                    let count = ids_to_delete.len();
                                    for id in ids_to_delete {
                                        let _ = crate::db::delete_job(&conn, id);
                                    }
                                    if count > 0 {
                                        app.status_message =
                                            format!("Cleared {} completed jobs", count);
                                        let _ = app.refresh_jobs(&conn);
                                    }
                                }
                                _ => {}
                            }
                        }
                        AppFocus::History => {
                            match key.code {
                                KeyCode::Up | KeyCode::Char('k') => {
                                    if app.selected_history > 0 {
                                        app.selected_history -= 1;
                                    }
                                }
                                KeyCode::Down | KeyCode::Char('j') => {
                                    if app.selected_history + 1 < app.visual_history.len() {
                                        app.selected_history += 1;
                                    }
                                }
                                KeyCode::Char('t') => {
                                    app.view_mode = match app.view_mode {
                                        ViewMode::Flat => ViewMode::Tree,
                                        ViewMode::Tree => ViewMode::Flat,
                                    };
                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = app.refresh_jobs(&conn);
                                }
                                KeyCode::Char('f') => {
                                    app.history_filter = app.history_filter.next();
                                    app.selected_history = 0; // Reset selection
                                    app.status_message =
                                        format!("History Filter: {}", app.history_filter.as_str());
                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = app.refresh_jobs(&conn);
                                }
                                KeyCode::Char('c') => {
                                    // Trigger confirmation logic for History
                                    app.input_mode = InputMode::Confirmation;
                                    app.pending_action = ModalAction::ClearHistory;
                                    app.confirmation_msg = format!(
                                        "Clear {} history entries? This cannot be undone.",
                                        match app.history_filter {
                                            HistoryFilter::All => "ALL",
                                            HistoryFilter::Complete => "completed",
                                            HistoryFilter::Quarantined => "quarantined",
                                        }
                                    );
                                }
                                KeyCode::Char('d') | KeyCode::Delete => {
                                    if !app.visual_history.is_empty()
                                        && app.selected_history < app.visual_history.len()
                                    {
                                        if let Some(idx) =
                                            app.visual_history[app.selected_history].index_in_jobs
                                        {
                                            let job = &app.history[idx];
                                            let id = job.id;
                                            let conn = conn_mutex.lock().unwrap();
                                            let _ = crate::db::delete_job(&conn, id);
                                            let _ = app.refresh_jobs(&conn);
                                            // Auto-fix happens in refresh_jobs
                                        }
                                    }
                                }
                                KeyCode::Char('R') => {
                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = app.refresh_jobs(&conn);
                                }
                                _ => {}
                            }
                        }
                        AppFocus::Quarantine => match key.code {
                            KeyCode::Up | KeyCode::Char('k') => {
                                if app.selected_quarantine > 0 {
                                    app.selected_quarantine -= 1;
                                }
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                if app.selected_quarantine + 1 < app.quarantine.len() {
                                    app.selected_quarantine += 1;
                                }
                            }
                            KeyCode::Char('d') | KeyCode::Delete => {
                                if !app.quarantine.is_empty()
                                    && app.selected_quarantine < app.quarantine.len()
                                {
                                    let job = &app.quarantine[app.selected_quarantine];
                                    let id = job.id;
                                    let q_path = job.staged_path.clone();

                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = crate::db::update_scan_status(
                                        &conn,
                                        id,
                                        "removed",
                                        "quarantined_removed",
                                    );

                                    if let Some(p) = q_path {
                                        let _ = std::fs::remove_file(p);
                                    }

                                    app.status_message = format!(
                                        "Threat neutralized: File '{}' deleted",
                                        job.source_path
                                    );

                                    if app.selected_quarantine >= app.quarantine.len()
                                        && app.selected_quarantine > 0
                                    {
                                        app.selected_quarantine -= 1;
                                    }
                                }
                            }
                            KeyCode::Char('R') => {
                                let conn = conn_mutex.lock().unwrap();
                                let _ = app.refresh_jobs(&conn);
                            }
                            _ => {}
                        },
                        AppFocus::SettingsCategory => {
                            match key.code {
                                KeyCode::Up | KeyCode::Char('k') => {
                                    app.settings.active_category = match app
                                        .settings
                                        .active_category
                                    {
                                        SettingsCategory::S3 => SettingsCategory::Theme,
                                        SettingsCategory::Scanner => SettingsCategory::S3,
                                        SettingsCategory::Performance => SettingsCategory::Scanner,
                                        SettingsCategory::Theme => SettingsCategory::Performance,
                                    };
                                    app.settings.selected_field = 0;
                                }
                                KeyCode::Down | KeyCode::Char('j') => {
                                    app.settings.active_category = match app
                                        .settings
                                        .active_category
                                    {
                                        SettingsCategory::S3 => SettingsCategory::Scanner,
                                        SettingsCategory::Scanner => SettingsCategory::Performance,
                                        SettingsCategory::Performance => SettingsCategory::Theme,
                                        SettingsCategory::Theme => SettingsCategory::S3,
                                    };
                                    app.settings.selected_field = 0;
                                }
                                KeyCode::Right | KeyCode::Enter => {
                                    app.focus = AppFocus::SettingsFields;
                                }
                                KeyCode::Char('s') => {
                                    let mut cfg = app.config.lock().unwrap();
                                    app.settings.apply_to_config(&mut cfg);

                                    // Apply theme immediately
                                    app.theme = Theme::from_name(&cfg.theme);

                                    // Save entire config to database
                                    let conn = conn_mutex.lock().unwrap();
                                    if let Err(e) = crate::config::save_config_to_db(&conn, &cfg) {
                                        app.status_message = format!("Save error: {}", e);
                                    } else {
                                        app.status_message = "Configuration saved".to_string();
                                    }
                                }
                                _ => {}
                            }
                        }
                        AppFocus::SettingsFields => {
                            let field_count = match app.settings.active_category {
                                SettingsCategory::S3 => 6,
                                SettingsCategory::Scanner => 4,
                                SettingsCategory::Performance => 6,
                                SettingsCategory::Theme => 1,
                            };
                            match key.code {
                                KeyCode::Enter => {
                                    // Special handling for Boolean Toggles
                                    let is_toggle = (app.settings.active_category
                                        == SettingsCategory::Scanner
                                        && app.settings.selected_field == 3)
                                        || (app.settings.active_category
                                            == SettingsCategory::Performance
                                            && app.settings.selected_field >= 3);

                                    if is_toggle {
                                        // Handle toggle based on category and field
                                        match (
                                            app.settings.active_category,
                                            app.settings.selected_field,
                                        ) {
                                            (SettingsCategory::Scanner, 3) => {
                                                app.settings.scanner_enabled =
                                                    !app.settings.scanner_enabled;
                                                app.status_message = format!(
                                                    "Scanner {}",
                                                    if app.settings.scanner_enabled {
                                                        "Enabled"
                                                    } else {
                                                        "Disabled"
                                                    }
                                                );
                                            }
                                            (SettingsCategory::Performance, 3) => {
                                                app.settings.staging_mode_direct =
                                                    !app.settings.staging_mode_direct;
                                                app.status_message = format!(
                                                    "Staging Mode: {}",
                                                    if app.settings.staging_mode_direct {
                                                        "Direct (no copy)"
                                                    } else {
                                                        "Copy (default)"
                                                    }
                                                );
                                            }
                                            (SettingsCategory::Performance, 4) => {
                                                app.settings.delete_source_after_upload =
                                                    !app.settings.delete_source_after_upload;
                                                app.status_message = format!(
                                                    "Delete Source: {}",
                                                    if app.settings.delete_source_after_upload {
                                                        "Enabled"
                                                    } else {
                                                        "Disabled"
                                                    }
                                                );
                                            }
                                            (SettingsCategory::Performance, 5) => {
                                                app.settings.host_metrics_enabled =
                                                    !app.settings.host_metrics_enabled;
                                                app.status_message = format!(
                                                    "Metrics: {}",
                                                    if app.settings.host_metrics_enabled {
                                                        "Enabled"
                                                    } else {
                                                        "Disabled"
                                                    }
                                                );
                                            }
                                            _ => {}
                                        }

                                        // Trigger Auto-Save logic immediately
                                        let mut cfg = app.config.lock().unwrap();
                                        app.settings.apply_to_config(&mut cfg);
                                        let conn = conn_mutex.lock().unwrap();
                                        if let Err(e) =
                                            crate::config::save_config_to_db(&conn, &cfg)
                                        {
                                            app.status_message = format!("Save error: {}", e);
                                        }
                                    } else {
                                        app.settings.editing = !app.settings.editing;
                                    }

                                    if app.settings.editing {
                                        // Entering edit mode
                                        if app.settings.active_category == SettingsCategory::Theme {
                                            app.settings.original_theme =
                                                Some(app.settings.theme.clone());
                                        }
                                    } else {
                                        // Exiting edit mode (Confirming) - Trigger Auto-Save
                                        if app.settings.active_category == SettingsCategory::Theme {
                                            app.settings.original_theme = None;
                                        }

                                        let mut cfg = app.config.lock().unwrap();
                                        app.settings.apply_to_config(&mut cfg);

                                        // Apply theme immediately
                                        app.theme = Theme::from_name(&cfg.theme);

                                        // Save entire config to database
                                        let conn = conn_mutex.lock().unwrap();
                                        if let Err(e) =
                                            crate::config::save_config_to_db(&conn, &cfg)
                                        {
                                            app.status_message = format!("Save error: {}", e);
                                        } else {
                                            let field_name = match app.settings.active_category {
                                                SettingsCategory::S3 => {
                                                    match app.settings.selected_field {
                                                        0 => "S3 Endpoint",
                                                        1 => "S3 Bucket",
                                                        2 => "S3 Region",
                                                        3 => "Prefix",
                                                        4 => "Access Key",
                                                        5 => "Secret Key",
                                                        _ => "Settings",
                                                    }
                                                }
                                                SettingsCategory::Scanner => {
                                                    match app.settings.selected_field {
                                                        0 => "ClamAV Host",
                                                        1 => "ClamAV Port",
                                                        2 => "Chunk Size",
                                                        3 => "Enable Scanner",
                                                        _ => "Scanner",
                                                    }
                                                }
                                                SettingsCategory::Performance => "Performance",
                                                SettingsCategory::Theme => "Theme",
                                            };
                                            app.status_message = format!("Saved: {}", field_name);
                                        }
                                    }
                                }
                                KeyCode::Esc => {
                                    app.settings.editing = false;
                                    // Revert theme if cancelled
                                    if app.settings.active_category == SettingsCategory::Theme {
                                        if let Some(orig) = &app.settings.original_theme {
                                            app.settings.theme = orig.clone();
                                            app.theme = Theme::from_name(orig);
                                        }
                                        app.settings.original_theme = None;
                                    }
                                }
                                // Theme cycle handling (Left/Right OR Up/Down when editing)
                                KeyCode::Right | KeyCode::Down
                                    if app.settings.editing
                                        && app.settings.active_category
                                            == SettingsCategory::Theme =>
                                {
                                    let current = app.settings.theme.as_str();
                                    if let Some(idx) =
                                        app.theme_names.iter().position(|&n| n == current)
                                    {
                                        let next_idx = (idx + 1) % app.theme_names.len();
                                        app.settings.theme = app.theme_names[next_idx].to_string();
                                        // Live preview
                                        app.theme = Theme::from_name(&app.settings.theme);
                                    }
                                }
                                KeyCode::Left | KeyCode::Up
                                    if app.settings.editing
                                        && app.settings.active_category
                                            == SettingsCategory::Theme =>
                                {
                                    let current = app.settings.theme.as_str();
                                    if let Some(idx) =
                                        app.theme_names.iter().position(|&n| n == current)
                                    {
                                        let prev_idx = (idx + app.theme_names.len() - 1)
                                            % app.theme_names.len();
                                        app.settings.theme = app.theme_names[prev_idx].to_string();
                                        // Live preview
                                        app.theme = Theme::from_name(&app.settings.theme);
                                    }
                                }
                                // When editing, handle ALL characters including j/k/s
                                KeyCode::Char(c) if app.settings.editing => {
                                    match (
                                        app.settings.active_category,
                                        app.settings.selected_field,
                                    ) {
                                        (SettingsCategory::Theme, _) => {
                                            // Specific handling for j/k in theme selection
                                            if c == 'j' {
                                                let current = app.settings.theme.as_str();
                                                if let Some(idx) = app
                                                    .theme_names
                                                    .iter()
                                                    .position(|&n| n == current)
                                                {
                                                    let next_idx =
                                                        (idx + 1) % app.theme_names.len();
                                                    app.settings.theme =
                                                        app.theme_names[next_idx].to_string();
                                                    app.theme =
                                                        Theme::from_name(&app.settings.theme);
                                                }
                                            } else if c == 'k' {
                                                let current = app.settings.theme.as_str();
                                                if let Some(idx) = app
                                                    .theme_names
                                                    .iter()
                                                    .position(|&n| n == current)
                                                {
                                                    let prev_idx = (idx + app.theme_names.len()
                                                        - 1)
                                                        % app.theme_names.len();
                                                    app.settings.theme =
                                                        app.theme_names[prev_idx].to_string();
                                                    app.theme =
                                                        Theme::from_name(&app.settings.theme);
                                                }
                                            }
                                        }
                                        (SettingsCategory::S3, 0) => app.settings.endpoint.push(c),
                                        (SettingsCategory::S3, 1) => app.settings.bucket.push(c),
                                        (SettingsCategory::S3, 2) => app.settings.region.push(c),
                                        (SettingsCategory::S3, 3) => app.settings.prefix.push(c),
                                        (SettingsCategory::S3, 4) => {
                                            app.settings.access_key.push(c)
                                        }
                                        (SettingsCategory::S3, 5) => {
                                            app.settings.secret_key.push(c)
                                        }
                                        (SettingsCategory::Scanner, 0) => {
                                            app.settings.clamd_host.push(c)
                                        }
                                        (SettingsCategory::Scanner, 1) => {
                                            app.settings.clamd_port.push(c)
                                        }
                                        (SettingsCategory::Scanner, 2) => {
                                            app.settings.scan_chunk_size.push(c)
                                        }
                                        (SettingsCategory::Performance, 0) => {
                                            app.settings.part_size.push(c)
                                        }
                                        (SettingsCategory::Performance, 1) => {
                                            app.settings.concurrency_global.push(c)
                                        }
                                        (SettingsCategory::Performance, 2) => {
                                            app.settings.scan_concurrency.push(c)
                                        }
                                        _ => {}
                                    }
                                }
                                KeyCode::Backspace if app.settings.editing => {
                                    match (
                                        app.settings.active_category,
                                        app.settings.selected_field,
                                    ) {
                                        (SettingsCategory::S3, 0) => {
                                            app.settings.endpoint.pop();
                                        }
                                        (SettingsCategory::S3, 1) => {
                                            app.settings.bucket.pop();
                                        }
                                        (SettingsCategory::S3, 2) => {
                                            app.settings.region.pop();
                                        }
                                        (SettingsCategory::S3, 3) => {
                                            app.settings.prefix.pop();
                                        }
                                        (SettingsCategory::S3, 4) => {
                                            app.settings.access_key.pop();
                                        }
                                        (SettingsCategory::S3, 5) => {
                                            app.settings.secret_key.pop();
                                        }
                                        (SettingsCategory::Scanner, 0) => {
                                            app.settings.clamd_host.pop();
                                        }
                                        (SettingsCategory::Scanner, 1) => {
                                            app.settings.clamd_port.pop();
                                        }
                                        (SettingsCategory::Scanner, 2) => {
                                            app.settings.scan_chunk_size.pop();
                                        }
                                        (SettingsCategory::Performance, 0) => {
                                            app.settings.part_size.pop();
                                        }
                                        (SettingsCategory::Performance, 1) => {
                                            app.settings.concurrency_global.pop();
                                        }
                                        (SettingsCategory::Performance, 2) => {
                                            app.settings.scan_concurrency.pop();
                                        }
                                        _ => {}
                                    }
                                }
                                // Navigation only when NOT editing
                                KeyCode::Up | KeyCode::Char('k') if !app.settings.editing => {
                                    if app.settings.selected_field > 0 {
                                        app.settings.selected_field -= 1;
                                    }
                                }
                                KeyCode::Down | KeyCode::Char('j') if !app.settings.editing => {
                                    if app.settings.selected_field + 1 < field_count {
                                        app.settings.selected_field += 1;
                                    }
                                }
                                KeyCode::Char('s')
                                    if !app.settings.editing
                                        || (app.settings.editing
                                            && app.settings.active_category
                                                == SettingsCategory::Theme) =>
                                {
                                    // If saving while editing theme, exit edit mode and commit
                                    if app.settings.editing
                                        && app.settings.active_category == SettingsCategory::Theme
                                    {
                                        app.settings.editing = false;
                                        app.settings.original_theme = None;
                                    }

                                    let mut cfg = app.config.lock().unwrap();
                                    app.settings.apply_to_config(&mut cfg);

                                    // Apply theme immediately
                                    app.theme = Theme::from_name(&cfg.theme);

                                    // Save entire config to database
                                    let conn = conn_mutex.lock().unwrap();
                                    if let Err(e) = crate::config::save_config_to_db(&conn, &cfg) {
                                        app.status_message = format!("Save error: {}", e);
                                    } else {
                                        app.status_message = format!(
                                            "Configuration saved. Bucket: {}",
                                            cfg.s3_bucket.as_deref().unwrap_or("None")
                                        );
                                    }
                                }
                                KeyCode::Char('w') if !app.settings.editing => {
                                    // Launch setup wizard
                                    app.wizard = WizardState::new();
                                    app.show_wizard = true;
                                }
                                KeyCode::Char('t') if !app.settings.editing => {
                                    let cat = app.settings.active_category;
                                    if cat == SettingsCategory::S3
                                        || cat == SettingsCategory::Scanner
                                    {
                                        app.status_message = "Testing connection...".to_string();
                                        let tx = app.async_tx.clone();
                                        let config_clone = app.config.lock().unwrap().clone();

                                        std::thread::spawn(move || {
                                            let rt = tokio::runtime::Runtime::new().unwrap();
                                            let res = rt.block_on(async {
                                                if cat == SettingsCategory::S3 {
                                                    crate::uploader::Uploader::check_connection(
                                                        &config_clone,
                                                    )
                                                    .await
                                                } else {
                                                    crate::scanner::Scanner::new(&config_clone)
                                                        .check_connection()
                                                        .await
                                                }
                                            });

                                            let msg = match res {
                                                Ok(s) => s,
                                                Err(e) => format!("Connection Failed: {}", e),
                                            };
                                            let _ = tx.send(msg);
                                        });
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Event::Mouse(mouse) => {
                    if mouse.kind == MouseEventKind::Down(MouseButton::Left) {
                        if let Ok(size) = terminal.size() {
                            let root = Layout::default()
                                .direction(Direction::Vertical)
                                .constraints([
                                    Constraint::Min(0),
                                    Constraint::Length(2),
                                    Constraint::Length(1),
                                ])
                                .split(size);

                            let main_layout = Layout::default()
                                .direction(Direction::Horizontal)
                                .constraints([
                                    Constraint::Length(14),
                                    Constraint::Min(0),
                                    Constraint::Length(60),
                                ])
                                .split(root[0]);

                            let rail = main_layout[0];
                            let center = main_layout[1];
                            let right = main_layout[2];
                            let x = mouse.column;
                            let y = mouse.row;

                            if x >= rail.x
                                && x < rail.x + rail.width
                                && y >= rail.y
                                && y < rail.y + rail.height
                            {
                                // Rail Click
                                let rel_ry = y.saturating_sub(rail.y);
                                if rel_ry == 1 {
                                    app.current_tab = AppTab::Transfers;
                                    app.focus = AppFocus::Rail;
                                } else if rel_ry == 2 {
                                    app.current_tab = AppTab::Quarantine;
                                    app.focus = AppFocus::Rail;
                                } else if rel_ry == 3 {
                                    app.current_tab = AppTab::Settings;
                                    app.focus = AppFocus::Rail;
                                }
                            } else if x >= center.x
                                && x < center.x + center.width
                                && y >= center.y
                                && y < center.y + center.height
                            {
                                // Center Click (Transfers / Settings)
                                match app.current_tab {
                                    AppTab::Transfers => {
                                        let chunks = Layout::default()
                                            .direction(Direction::Horizontal)
                                            .constraints([
                                                Constraint::Percentage(50),
                                                Constraint::Percentage(50),
                                            ])
                                            .split(center);

                                        // Browser Panel Interaction
                                        if x >= chunks[0].x && x < chunks[0].x + chunks[0].width {
                                            app.focus = AppFocus::Browser;

                                            // Handle Double Click (Global for Panel)
                                            let now = Instant::now();
                                            let is_double_click =
                                                if let (Some(last_time), Some(last_pos)) =
                                                    (app.last_click_time, app.last_click_pos)
                                                {
                                                    now.duration_since(last_time)
                                                        < Duration::from_millis(500)
                                                        && last_pos == (x, y)
                                                } else {
                                                    false
                                                };

                                            if is_double_click {
                                                // Reset click timer
                                                app.last_click_time = None;

                                                // If Normal mode, switch to Browsing (Global Action)
                                                if app.input_mode == InputMode::Normal {
                                                    app.input_mode = InputMode::Browsing;
                                                    app.status_message =
                                                        "Browsing files...".to_string();
                                                } else if app.input_mode == InputMode::Browsing {
                                                    // If already Browsing, check if we double-clicked a specific item to Enter
                                                    let has_filter = !app.input_buffer.is_empty()
                                                        || app.input_mode == InputMode::Filter;
                                                    let data_start_y = chunks[0].y
                                                        + 1
                                                        + 1
                                                        + if has_filter { 1 } else { 0 }
                                                        + 1;

                                                    if y >= data_start_y {
                                                        let relative_row =
                                                            (y - data_start_y) as usize;
                                                        let display_height =
                                                            chunks[0].height.saturating_sub(
                                                                if has_filter { 4 } else { 3 },
                                                            )
                                                                as usize;

                                                        let filter = if app.input_mode
                                                            == InputMode::Filter
                                                        {
                                                            app.input_buffer.clone()
                                                        } else {
                                                            app.input_buffer.clone()
                                                        };
                                                        let filtered_entries: Vec<usize> = app
                                                            .picker
                                                            .entries
                                                            .iter()
                                                            .enumerate()
                                                            .filter(|(_, e)| {
                                                                e.is_parent
                                                                    || filter.is_empty()
                                                                    || fuzzy_match(&filter, &e.name)
                                                            })
                                                            .map(|(i, _)| i)
                                                            .collect();

                                                        let total_rows = filtered_entries.len();
                                                        let current_filtered_selected =
                                                            filtered_entries
                                                                .iter()
                                                                .position(|&i| {
                                                                    i == app.picker.selected
                                                                })
                                                                .unwrap_or(0);
                                                        let offset = calculate_list_offset(
                                                            current_filtered_selected,
                                                            total_rows,
                                                            display_height,
                                                        );

                                                        if relative_row < display_height
                                                            && offset + relative_row < total_rows
                                                        {
                                                            // Valid item double-clicked
                                                            let new_filtered_idx =
                                                                offset + relative_row;
                                                            let new_real_idx =
                                                                filtered_entries[new_filtered_idx];
                                                            app.picker.selected = new_real_idx;

                                                            let entry = app.picker.entries
                                                                [app.picker.selected]
                                                                .clone();
                                                            if entry.is_dir {
                                                                if entry.is_parent {
                                                                    app.picker.go_parent();
                                                                } else {
                                                                    app.picker
                                                                        .try_set_cwd(entry.path);
                                                                }
                                                            } else {
                                                                if let Ok(conn) = conn_mutex.lock()
                                                                {
                                                                    if let Ok(cfg_guard) =
                                                                        cfg.lock()
                                                                    {
                                                                        let _ = ingest_path(
                                                                            &conn,
                                                                            &cfg_guard.staging_dir,
                                                                            &cfg_guard.staging_mode,
                                                                            &entry
                                                                                .path
                                                                                .to_string_lossy(),
                                                                        );
                                                                        drop(cfg_guard);
                                                                        drop(conn);
                                                                        app.status_message =
                                                                            "File added to queue"
                                                                                .to_string();
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            } else {
                                                // Single Click Logic
                                                app.last_click_time = Some(now);
                                                app.last_click_pos = Some((x, y));

                                                // Handle selection
                                                let has_filter = !app.input_buffer.is_empty()
                                                    || app.input_mode == InputMode::Filter;
                                                let data_start_y = chunks[0].y
                                                    + 1
                                                    + 1
                                                    + if has_filter { 1 } else { 0 }
                                                    + 1;

                                                if y >= data_start_y {
                                                    let relative_row = (y - data_start_y) as usize;
                                                    let display_height =
                                                        chunks[0].height.saturating_sub(
                                                            if has_filter { 4 } else { 3 },
                                                        )
                                                            as usize;

                                                    let filter =
                                                        if app.input_mode == InputMode::Filter {
                                                            app.input_buffer.clone()
                                                        } else {
                                                            app.input_buffer.clone()
                                                        };
                                                    let filtered_entries: Vec<usize> = app
                                                        .picker
                                                        .entries
                                                        .iter()
                                                        .enumerate()
                                                        .filter(|(_, e)| {
                                                            e.is_parent
                                                                || filter.is_empty()
                                                                || fuzzy_match(&filter, &e.name)
                                                        })
                                                        .map(|(i, _)| i)
                                                        .collect();

                                                    let total_rows = filtered_entries.len();

                                                    let current_filtered_selected =
                                                        filtered_entries
                                                            .iter()
                                                            .position(|&i| i == app.picker.selected)
                                                            .unwrap_or(0);
                                                    let offset = calculate_list_offset(
                                                        current_filtered_selected,
                                                        total_rows,
                                                        display_height,
                                                    );

                                                    if relative_row < display_height
                                                        && offset + relative_row < total_rows
                                                    {
                                                        let new_filtered_idx =
                                                            offset + relative_row;
                                                        let new_real_idx =
                                                            filtered_entries[new_filtered_idx];
                                                        app.picker.selected = new_real_idx;
                                                    }
                                                }
                                            }
                                        }
                                        // Queue Panel Interaction
                                        else if x >= chunks[1].x
                                            && x < chunks[1].x + chunks[1].width
                                        {
                                            app.focus = AppFocus::Queue;
                                            let inner_y = chunks[1].y + 1; // Header
                                            if y > inner_y {
                                                let relative_row = (y - inner_y - 1) as usize; // -1 because row 0 is header
                                                let display_height =
                                                    chunks[1].height.saturating_sub(2) as usize; // Borders
                                                let total_rows = app.visual_jobs.len();
                                                let offset = calculate_list_offset(
                                                    app.selected,
                                                    total_rows,
                                                    display_height,
                                                );

                                                if relative_row < display_height
                                                    && offset + relative_row < total_rows
                                                {
                                                    app.selected = offset + relative_row;
                                                }
                                            }
                                        }
                                    }
                                    AppTab::Quarantine => {
                                        app.focus = AppFocus::Quarantine;
                                        let inner_y = center.y + 1; // Header
                                        if y > inner_y {
                                            let relative_row = (y - inner_y - 1) as usize;
                                            let display_height =
                                                center.height.saturating_sub(4) as usize;
                                            let total_rows = app.quarantine.len();
                                            let offset = calculate_list_offset(
                                                app.selected_quarantine,
                                                total_rows,
                                                display_height,
                                            );

                                            if relative_row < display_height
                                                && offset + relative_row < total_rows
                                            {
                                                app.selected_quarantine = offset + relative_row;
                                            }
                                        }
                                    }
                                    AppTab::Settings => {
                                        // Calculate layout exactly as in draw_settings
                                        let outer_block = Block::default().borders(Borders::ALL);
                                        let inner_area = outer_block.inner(center);

                                        let chunks = Layout::default()
                                            .direction(Direction::Horizontal)
                                            .constraints([
                                                Constraint::Length(16),
                                                Constraint::Min(0),
                                            ])
                                            .split(inner_area);

                                        if x >= chunks[0].x && x < chunks[0].x + chunks[0].width {
                                            app.focus = AppFocus::SettingsCategory;

                                            // Handle Category Selection
                                            // List items start at chunks[0].y
                                            if y >= chunks[0].y {
                                                let rel_y = (y - chunks[0].y) as usize;
                                                // Categories: S3, Scanner, Performance, Theme
                                                if rel_y < 4 {
                                                    let new_cat = match rel_y {
                                                        0 => SettingsCategory::S3,
                                                        1 => SettingsCategory::Scanner,
                                                        2 => SettingsCategory::Performance,
                                                        3 => SettingsCategory::Theme,
                                                        _ => SettingsCategory::S3,
                                                    };

                                                    if app.settings.active_category != new_cat {
                                                        app.settings.active_category = new_cat;
                                                        app.settings.selected_field = 0; // Reset field on category switch
                                                    }
                                                }
                                            }
                                        } else if x >= chunks[1].x
                                            && x < chunks[1].x + chunks[1].width
                                        {
                                            app.focus = AppFocus::SettingsFields;

                                            // Handle Field Selection
                                            // Special Case: Theme Selection Popup
                                            if app.settings.editing
                                                && app.settings.active_category
                                                    == SettingsCategory::Theme
                                            {
                                                let max_width = chunks[1].width.saturating_sub(2);
                                                let max_height = chunks[1].height.saturating_sub(2);
                                                if max_width >= 22 && max_height >= 8 {
                                                    let width = max_width.min(62);
                                                    let height = max_height.min(14).max(8);
                                                    let theme_area = Rect {
                                                        x: chunks[1].x + 1,
                                                        y: chunks[1].y + 1,
                                                        width,
                                                        height,
                                                    };
                                                    let show_preview = theme_area.width >= 54;
                                                    let list_width = if show_preview {
                                                        24
                                                    } else {
                                                        theme_area.width
                                                    };
                                                    let list_area = Rect {
                                                        x: theme_area.x,
                                                        y: theme_area.y,
                                                        width: list_width,
                                                        height: theme_area.height,
                                                    };

                                                    if x >= list_area.x
                                                        && x < list_area.x + list_area.width
                                                        && y >= list_area.y
                                                        && y < list_area.y + list_area.height
                                                    {
                                                        // Click inside theme list
                                                        let rel_ly = (y
                                                            .saturating_sub(list_area.y + 1))
                                                            as usize; // +1 for border
                                                        let max_visible =
                                                            list_area.height.saturating_sub(2)
                                                                as usize;

                                                        if rel_ly < max_visible {
                                                            let total = app.theme_names.len();
                                                            let current_theme =
                                                                app.settings.theme.as_str();

                                                            // Calculate offset as in rendering
                                                            let mut offset = 0;
                                                            if let Some(idx) = app
                                                                .theme_names
                                                                .iter()
                                                                .position(|&n| n == current_theme)
                                                            {
                                                                if max_visible > 0
                                                                    && total > max_visible
                                                                {
                                                                    offset = idx.saturating_sub(
                                                                        max_visible / 2,
                                                                    );
                                                                    if offset + max_visible > total
                                                                    {
                                                                        offset = total
                                                                            .saturating_sub(
                                                                                max_visible,
                                                                            );
                                                                    }
                                                                }
                                                            }

                                                            let target_idx = offset + rel_ly;
                                                            if target_idx < total {
                                                                let now = Instant::now();
                                                                let is_double_click = if let (
                                                                    Some(last_time),
                                                                    Some(last_pos),
                                                                ) = (
                                                                    app.last_click_time,
                                                                    app.last_click_pos,
                                                                ) {
                                                                    now.duration_since(last_time)
                                                                        < Duration::from_millis(500)
                                                                        && last_pos == (x, y)
                                                                } else {
                                                                    false
                                                                };

                                                                app.settings.theme = app
                                                                    .theme_names[target_idx]
                                                                    .to_string();
                                                                // Live preview
                                                                app.theme = Theme::from_name(
                                                                    &app.settings.theme,
                                                                );

                                                                if is_double_click {
                                                                    // Commit and Exit on Double Click
                                                                    app.settings.editing = false;
                                                                    app.settings.original_theme =
                                                                        None;
                                                                    let mut cfg =
                                                                        app.config.lock().unwrap();
                                                                    app.settings
                                                                        .apply_to_config(&mut cfg);
                                                                    let conn =
                                                                        conn_mutex.lock().unwrap();
                                                                    let _ = crate::config::save_config_to_db(&conn, &cfg);
                                                                    app.status_message =
                                                                        "Theme saved".to_string();
                                                                    app.last_click_time = None;
                                                                } else {
                                                                    app.last_click_time = Some(now);
                                                                    app.last_click_pos =
                                                                        Some((x, y));
                                                                }
                                                            }
                                                        }
                                                        continue; // Don't process other field clicks
                                                    } else {
                                                        // Click-off: Exit and Commit
                                                        app.settings.editing = false;
                                                        app.settings.original_theme = None;
                                                        let mut cfg = app.config.lock().unwrap();
                                                        app.settings.apply_to_config(&mut cfg);
                                                        let conn = conn_mutex.lock().unwrap();
                                                        let _ = crate::config::save_config_to_db(
                                                            &conn, &cfg,
                                                        );
                                                        app.status_message =
                                                            "Theme selection closed".to_string();
                                                        continue;
                                                    }
                                                }
                                            }

                                            // Default Field Selection
                                            if y >= chunks[1].y {
                                                let rel_y = (y - chunks[1].y) as usize;
                                                let clicked_view_idx = rel_y / 3;

                                                // Calculate Context
                                                let count = match app.settings.active_category {
                                                    SettingsCategory::S3 => 6,
                                                    SettingsCategory::Scanner => 4,
                                                    SettingsCategory::Performance => 6,
                                                    SettingsCategory::Theme => 1,
                                                };

                                                let display_height = chunks[1].height as usize;
                                                let fields_per_view = display_height / 3;

                                                // Replicate render offset logic
                                                let mut offset = 0;
                                                if app.settings.selected_field >= fields_per_view {
                                                    offset = app
                                                        .settings
                                                        .selected_field
                                                        .saturating_sub(fields_per_view / 2);
                                                }
                                                // Safety check for offset
                                                if count > fields_per_view
                                                    && offset + fields_per_view > count
                                                {
                                                    offset = count - fields_per_view;
                                                }

                                                let target_idx = offset + clicked_view_idx;

                                                if target_idx < count {
                                                    // Handle Boolean Toggles on Click
                                                    let is_toggle = (app.settings.active_category
                                                        == SettingsCategory::Scanner
                                                        && target_idx == 3)
                                                        || (app.settings.active_category
                                                            == SettingsCategory::Performance
                                                            && target_idx >= 3);

                                                    if is_toggle {
                                                        match (
                                                            app.settings.active_category,
                                                            target_idx,
                                                        ) {
                                                            (SettingsCategory::Scanner, 3) => {
                                                                app.settings.scanner_enabled =
                                                                    !app.settings.scanner_enabled;
                                                                app.status_message = format!(
                                                                    "Scanner {}",
                                                                    if app.settings.scanner_enabled
                                                                    {
                                                                        "Enabled"
                                                                    } else {
                                                                        "Disabled"
                                                                    }
                                                                );
                                                            }
                                                            (SettingsCategory::Performance, 3) => {
                                                                app.settings.staging_mode_direct =
                                                                    !app.settings
                                                                        .staging_mode_direct;
                                                                app.status_message = format!(
                                                                    "Staging Mode: {}",
                                                                    if app
                                                                        .settings
                                                                        .staging_mode_direct
                                                                    {
                                                                        "Direct (no copy)"
                                                                    } else {
                                                                        "Copy (default)"
                                                                    }
                                                                );
                                                            }
                                                            (SettingsCategory::Performance, 4) => {
                                                                app.settings
                                                                    .delete_source_after_upload =
                                                                    !app.settings
                                                                        .delete_source_after_upload;
                                                                app.status_message = format!(
                                                                    "Delete Source: {}",
                                                                    if app
                                                                        .settings
                                                                        .delete_source_after_upload
                                                                    {
                                                                        "Enabled"
                                                                    } else {
                                                                        "Disabled"
                                                                    }
                                                                );
                                                            }
                                                            (SettingsCategory::Performance, 5) => {
                                                                app.settings.host_metrics_enabled =
                                                                    !app.settings
                                                                        .host_metrics_enabled;
                                                                app.status_message = format!(
                                                                    "Metrics: {}",
                                                                    if app
                                                                        .settings
                                                                        .host_metrics_enabled
                                                                    {
                                                                        "Enabled"
                                                                    } else {
                                                                        "Disabled"
                                                                    }
                                                                );
                                                            }
                                                            _ => {}
                                                        }

                                                        // Auto-Save logic for toggles
                                                        let mut cfg = app.config.lock().unwrap();
                                                        app.settings.apply_to_config(&mut cfg);
                                                        let conn = conn_mutex.lock().unwrap();
                                                        let _ = crate::config::save_config_to_db(
                                                            &conn, &cfg,
                                                        );

                                                        app.settings.selected_field = target_idx;
                                                        continue;
                                                    }

                                                    // Handle Double Click for Editing
                                                    let now = Instant::now();
                                                    let is_double_click = if let (
                                                        Some(last_time),
                                                        Some(last_pos),
                                                    ) =
                                                        (app.last_click_time, app.last_click_pos)
                                                    {
                                                        now.duration_since(last_time)
                                                            < Duration::from_millis(500)
                                                            && last_pos == (x, y)
                                                    } else {
                                                        false
                                                    };

                                                    app.settings.selected_field = target_idx;

                                                    // Request: expand theme dropdown when we click it
                                                    if app.settings.active_category
                                                        == SettingsCategory::Theme
                                                    {
                                                        app.settings.editing = true;
                                                    } else if is_double_click {
                                                        app.settings.editing = true;
                                                        app.last_click_time = None;
                                                    } else {
                                                        app.last_click_time = Some(now);
                                                        app.last_click_pos = Some((x, y));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            } else if x >= right.x
                                && x < right.x + right.width
                                && y >= right.y
                                && y < right.y + right.height
                            {
                                // Right Panel Click (History)
                                app.focus = AppFocus::History;
                                let inner_y = right.y + 1; // Header
                                if y > inner_y {
                                    let relative_row = (y - inner_y - 1) as usize;
                                    let display_height = right.height.saturating_sub(4) as usize; // approx overhead
                                    let total_rows = app.visual_history.len();
                                    let offset = calculate_list_offset(
                                        app.selected_history,
                                        total_rows,
                                        display_height,
                                    );

                                    if relative_row < display_height
                                        && offset + relative_row < total_rows
                                    {
                                        app.selected_history = offset + relative_row;
                                    }
                                }
                            }
                        }
                    } else if let MouseEventKind::ScrollDown = mouse.kind {
                        match app.focus {
                            AppFocus::Browser => app.picker.move_down(),
                            AppFocus::Queue => {
                                if app.selected + 1 < app.visual_jobs.len() {
                                    app.selected += 1;
                                }
                            }
                            AppFocus::History => {
                                if app.selected_history + 1 < app.visual_history.len() {
                                    app.selected_history += 1;
                                }
                            }
                            AppFocus::Quarantine => {
                                if app.selected_quarantine + 1 < app.quarantine.len() {
                                    app.selected_quarantine += 1;
                                }
                            }
                            AppFocus::SettingsCategory => {
                                app.settings.active_category = match app.settings.active_category {
                                    SettingsCategory::S3 => SettingsCategory::Scanner,
                                    SettingsCategory::Scanner => SettingsCategory::Performance,
                                    SettingsCategory::Performance => SettingsCategory::Theme,
                                    SettingsCategory::Theme => SettingsCategory::S3,
                                };
                                app.settings.selected_field = 0;
                            }
                            AppFocus::SettingsFields => {
                                let count = match app.settings.active_category {
                                    SettingsCategory::S3 => 6,
                                    SettingsCategory::Scanner => 4,
                                    SettingsCategory::Performance => 6,
                                    SettingsCategory::Theme => 1,
                                };
                                if app.settings.selected_field + 1 < count {
                                    app.settings.selected_field += 1;
                                }
                            }
                            _ => {}
                        }
                    } else if let MouseEventKind::ScrollUp = mouse.kind {
                        match app.focus {
                            AppFocus::Browser => app.picker.move_up(),
                            AppFocus::Queue => {
                                if app.selected > 0 {
                                    app.selected -= 1;
                                }
                            }
                            AppFocus::History => {
                                if app.selected_history > 0 {
                                    app.selected_history -= 1;
                                }
                            }
                            AppFocus::Quarantine => {
                                if app.selected_quarantine > 0 {
                                    app.selected_quarantine -= 1;
                                }
                            }
                            AppFocus::SettingsCategory => {
                                app.settings.active_category = match app.settings.active_category {
                                    SettingsCategory::S3 => SettingsCategory::Theme,
                                    SettingsCategory::Scanner => SettingsCategory::S3,
                                    SettingsCategory::Performance => SettingsCategory::Scanner,
                                    SettingsCategory::Theme => SettingsCategory::Performance,
                                };
                                app.settings.selected_field = 0;
                            }
                            AppFocus::SettingsFields => {
                                if app.settings.selected_field > 0 {
                                    app.settings.selected_field -= 1;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    }

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    Ok(())
}

impl SettingsState {
    fn from_config(cfg: &Config) -> Self {
        use crate::config::StagingMode;
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
            scan_concurrency: cfg.concurrency_parts_per_file.to_string(),
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

    fn apply_to_config(&self, cfg: &mut Config) {
        use crate::config::StagingMode;
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
        if let Ok(v) = self.scan_concurrency.trim().parse() {
            cfg.concurrency_parts_per_file = v;
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

fn draw_rail(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::Rail {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let items = vec![" Transfers ", " Quarantine ", " Settings  "];
    let rows: Vec<Row> = items
        .iter()
        .enumerate()
        .map(|(id, name)| {
            let style = if app.current_tab as usize == id {
                app.theme.selection_style()
            } else {
                app.theme.text_style()
            };
            Row::new(vec![Cell::from(name.to_string())]).style(style)
        })
        .collect();

    let table = Table::new(rows, [Constraint::Length(12)]).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(app.theme.border_type)
            .title("Nav")
            .border_style(focus_style)
            .style(app.theme.panel_style()),
    );
    f.render_widget(table, area);
}

fn draw_job_details(f: &mut ratatui::Frame, app: &App, area: Rect, job: &crate::db::JobRow) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .title(format!(" Job #{} Details ", job.id))
        .border_style(app.theme.border_style())
        .style(app.theme.panel_style());

    let scan_status = job.scan_status.as_deref().unwrap_or("none");
    let error = job.error.as_deref().unwrap_or("none");

    let mut text = vec![
        Line::from(vec![
            Span::styled("Status: ", app.theme.highlight_style()),
            Span::styled(
                job.status.clone(),
                app.theme.status_style(status_kind(job.status.as_str())),
            ),
        ]),
        Line::from(vec![
            Span::styled("Path:   ", app.theme.highlight_style()),
            Span::styled(job.source_path.clone(), app.theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("Size:   ", app.theme.highlight_style()),
            Span::styled(format_bytes(job.size_bytes as u64), app.theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("Scan:   ", app.theme.highlight_style()),
            Span::styled(scan_status, app.theme.text_style()),
        ]),
    ];

    // Show multipart upload progress if job is uploading
    if job.status == "uploading" {
        let progress_map = app.progress.lock().unwrap();
        if let Some(info) = progress_map.get(&job.id) {
            if info.parts_total > 0 {
                // Clone values to avoid borrow issues
                let parts_done = info.parts_done;
                let parts_total = info.parts_total;
                let details = info.details.clone();
                drop(progress_map);

                text.push(Line::from(""));
                text.push(Line::from(Span::styled(
                    "MULTIPART UPLOAD:",
                    app.theme
                        .highlight_style()
                        .add_modifier(Modifier::UNDERLINED),
                )));

                // Show parts progress bar
                let bar_width = 20;
                let filled = (parts_done * bar_width) / parts_total.max(1);
                let bar = format!(
                    "[{}{}] {}/{}",
                    "█".repeat(filled),
                    "░".repeat(bar_width - filled),
                    parts_done,
                    parts_total
                );
                text.push(Line::from(vec![
                    Span::styled("Parts:  ", app.theme.highlight_style()),
                    Span::styled(bar, app.theme.progress_style()),
                ]));
                text.push(Line::from(vec![
                    Span::styled("Detail: ", app.theme.highlight_style()),
                    Span::styled(details, app.theme.text_style()),
                ]));
            }
        }
    }

    text.push(Line::from(""));
    text.push(Line::from(Span::styled(
        "DETAILS / ERRORS:",
        app.theme
            .highlight_style()
            .add_modifier(Modifier::UNDERLINED),
    )));
    text.push(Line::from(Span::styled(
        error,
        if error != "none" {
            app.theme.status_style(StatusKind::Error)
        } else {
            app.theme.text_style()
        },
    )));

    let p = Paragraph::new(text).block(block).wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

fn draw_transfers(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let cfg = app.config.lock().unwrap();
    let hopper_percent = cfg.hopper_width_percent;
    drop(cfg);
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(hopper_percent),
            Constraint::Percentage(100 - hopper_percent),
        ])
        .split(area);

    draw_browser(f, app, chunks[0]);
    draw_jobs(f, app, chunks[1]);
}

fn draw_history(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::History {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    // Header similar to Queue but without Progress
    let header_cells = ["File", "Status", "Size", "Time"]
        .iter()
        .map(|h| Cell::from(*h).style(app.theme.header_style()));
    let header = Row::new(header_cells)
        .style(app.theme.header_style())
        .height(1);

    let total_rows = app.visual_history.len();
    let display_height = area.height.saturating_sub(4) as usize; // Border + Header
    let offset = calculate_list_offset(app.selected_history, total_rows, display_height);

    let rows = app
        .visual_history
        .iter()
        .enumerate()
        .skip(offset)
        .take(display_height)
        .map(|(idx, item)| {
            let is_selected = (idx == app.selected_history) && (app.focus == AppFocus::History);
            let row_style = if is_selected {
                app.theme.selection_style()
            } else {
                app.theme.row_style(idx % 2 != 0)
            };

            let indent = " ".repeat(item.depth * 2);
            let text = if item.text.len() > 30 {
                format!("{}…", &item.text[..29])
            } else {
                item.text.clone()
            };

            let display_name = if item.is_folder {
                format!("{}📁 {}", indent, text)
            } else {
                format!("{}📄 {}", indent, text)
            };

            if let Some(job_idx) = item.index_in_jobs {
                let job = &app.history[job_idx];

                // Status text
                let status_str = match job.status.as_str() {
                    "quarantined" => "Threat Detected",
                    "quarantined_removed" => "Threat Removed",
                    "complete" => "Done",
                    s => s,
                };

                // Relative time
                let time_str = format_relative_time(&job.created_at);

                let status_style = if is_selected {
                    app.theme.selection_style()
                } else {
                    match job.status.as_str() {
                        "quarantined" | "quarantined_removed" => {
                            app.theme.status_style(StatusKind::Error)
                        }
                        "complete" => app.theme.status_style(StatusKind::Success),
                        _ => app.theme.text_style(),
                    }
                };

                Row::new(vec![
                    Cell::from(display_name),
                    Cell::from(status_str).style(status_style),
                    Cell::from(format_bytes(job.size_bytes as u64)),
                    Cell::from(time_str),
                ])
                .style(row_style)
            } else {
                Row::new(vec![
                    Cell::from(display_name),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                ])
                .style(row_style)
            }
        });

    let title = format!(" History [{}] ", app.history_filter.as_str());

    let table = Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(16),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(app.theme.border_type)
            .title(title)
            .border_style(focus_style)
            .style(app.theme.panel_style()),
    );

    f.render_widget(table, area);
}

fn draw_footer(f: &mut ratatui::Frame, app: &App, area: Rect) {
    // Helper to create styled key-action pairs
    let sep = || Span::styled("  │  ".to_string(), app.theme.muted_style());
    let key = |k: &str| {
        Span::styled(
            k.to_string(),
            app.theme.accent_style().add_modifier(Modifier::BOLD),
        )
    };
    let act = |a: &str| Span::styled(format!(" {}", a), app.theme.muted_style());

    let footer_spans = match app.input_mode {
        InputMode::Filter => vec![
            key("Type"),
            act("to filter"),
            sep(),
            key("↑/↓"),
            act("Select"),
            sep(),
            key("Enter"),
            act("Confirm"),
            sep(),
            key("Esc"),
            act("Back"),
        ],
        InputMode::Browsing => vec![
            key("←/→"),
            act("Navigate"),
            sep(),
            key("↑/↓"),
            act("Select"),
            sep(),
            key("/"),
            act("Filter"),
            sep(),
            key("t"),
            act("Tree"),
            sep(),
            key("Space"),
            act("Select"),
            sep(),
            key("s"),
            act("Stage"),
            sep(),
            key("Esc"),
            act("Exit"),
        ],
        InputMode::Confirmation => vec![Span::styled(
            "Confirmation Required",
            app.theme.muted_style(),
        )],
        InputMode::LayoutAdjust => vec![
            key("1-3"),
            act("Select Panel"),
            sep(),
            key("+/-"),
            act("Adjust"),
            sep(),
            key("r/R"),
            act("Reset"),
            sep(),
            key("s"),
            act("Save"),
            sep(),
            key("q"),
            act("Cancel"),
        ],
        InputMode::Normal => match app.focus {
            AppFocus::Rail => vec![
                key("Tab/→"),
                act("Content"),
                sep(),
                key("↑/↓"),
                act("Switch Tab"),
                sep(),
                key("q"),
                act("Quit"),
            ],
            AppFocus::Browser => vec![
                key("↑/↓"),
                act("Select"),
                sep(),
                key("a"),
                act("Browse"),
                sep(),
                key("Tab"),
                act("Next Panel"),
            ],
            AppFocus::Queue => vec![
                key("↑/↓"),
                act("Select"),
                sep(),
                key("Enter"),
                act("Details"),
                sep(),
                key("c"),
                act("Clear Done"),
                sep(),
                key("r"),
                act("Retry"),
                sep(),
                key("d"),
                act("Delete"),
            ],
            AppFocus::History => vec![key("Tab"), act("Rail"), sep(), key("←"), act("Queue")],
            AppFocus::Quarantine => vec![
                key("Tab"),
                act("Rail"),
                sep(),
                key("←"),
                act("Rail"),
                sep(),
                key("d"),
                act("Clear"),
                sep(),
                key("R"),
                act("Refresh"),
            ],
            AppFocus::SettingsCategory => match app.settings.active_category {
                SettingsCategory::S3 | SettingsCategory::Scanner => vec![
                    key("Tab/→"),
                    act("Fields"),
                    sep(),
                    key("←"),
                    act("Rail"),
                    sep(),
                    key("↑/↓"),
                    act("Category"),
                    sep(),
                    key("s"),
                    act("Save"),
                    sep(),
                    key("t"),
                    act("Test"),
                ],
                _ => vec![
                    key("Tab/→"),
                    act("Fields"),
                    sep(),
                    key("←"),
                    act("Rail"),
                    sep(),
                    key("↑/↓"),
                    act("Category"),
                    sep(),
                    key("s"),
                    act("Save"),
                ],
            },
            AppFocus::SettingsFields => match app.settings.active_category {
                SettingsCategory::S3 | SettingsCategory::Scanner => vec![
                    key("Tab"),
                    act("Rail"),
                    sep(),
                    key("←"),
                    act("Category"),
                    sep(),
                    key("Enter"),
                    act("Edit"),
                    sep(),
                    key("s"),
                    act("Save"),
                    sep(),
                    key("t"),
                    act("Test"),
                ],
                _ => vec![
                    key("Tab"),
                    act("Rail"),
                    sep(),
                    key("←"),
                    act("Category"),
                    sep(),
                    key("Enter"),
                    act("Edit"),
                    sep(),
                    key("s"),
                    act("Save"),
                ],
            },
        },
    };

    // Calculate system status parts
    let av_status = app.clamav_status.lock().unwrap().clone();
    let s3_status = if app.settings.bucket.is_empty() {
        "Not Configured"
    } else {
        "Ready"
    };

    let left_content = Line::from(footer_spans);

    let right_content = Line::from(vec![
        Span::styled("ClamAV: ", app.theme.muted_style()),
        Span::styled(
            av_status,
            if app.clamav_status.lock().unwrap().contains("Ready") {
                app.theme.status_style(StatusKind::Success)
            } else {
                app.theme.status_style(StatusKind::Warning)
            },
        ),
        Span::styled("  │  ", app.theme.muted_style()),
        Span::styled("S3: ", app.theme.muted_style()),
        Span::styled(
            s3_status,
            if app.settings.bucket.is_empty() {
                app.theme.status_style(StatusKind::Error)
            } else {
                app.theme.status_style(StatusKind::Success)
            },
        ),
        Span::styled(
            if app.settings.bucket.is_empty() {
                String::new()
            } else {
                format!(" [{}]", &app.settings.bucket)
            },
            app.theme.muted_style(),
        ),
        Span::styled("  │  ", app.theme.muted_style()),
        Span::styled("Jobs: ", app.theme.muted_style()),
        Span::styled(
            format!("{}", app.jobs.len()),
            if app.jobs.is_empty() {
                app.theme.muted_style()
            } else {
                app.theme.highlight_style()
            },
        ),
        Span::styled("  │  ", app.theme.muted_style()),
        Span::styled("Threats: ", app.theme.muted_style()),
        Span::styled(
            format!("{}", app.quarantine.len()),
            if app.quarantine.is_empty() {
                app.theme.muted_style()
            } else {
                app.theme.status_style(StatusKind::Error)
            },
        ),
    ]);

    let footer_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(40), Constraint::Length(120)])
        .split(area);

    f.render_widget(
        Paragraph::new(left_content).alignment(ratatui::layout::Alignment::Left),
        footer_chunks[0],
    );
    f.render_widget(
        Paragraph::new(right_content).alignment(ratatui::layout::Alignment::Right),
        footer_chunks[1],
    );
}

fn draw_layout_adjustment_overlay(f: &mut ratatui::Frame, app: &App, area: Rect) {
    if app.input_mode != InputMode::LayoutAdjust {
        return;
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .title(" Layout Adjustment Mode ")
        .style(app.theme.panel_style())
        .border_style(app.theme.border_active_style());

    let popup_area = centered_fixed_rect(area, 108, 17);
    f.render_widget(ratatui::widgets::Clear, popup_area);
    f.render_widget(block.clone(), popup_area);

    let inner = block.inner(popup_area);

    let cfg = app.config.lock().unwrap();
    let current = app.layout_adjust_target;

    let highlight = app.theme.selection_style();
    let normal = app.theme.text_style();

    let queue_width = 100 - cfg.hopper_width_percent;

    let lines = vec![
        Line::from("Select a panel to adjust:"),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                "1. ",
                if current == Some(LayoutTarget::Hopper) {
                    highlight
                } else {
                    normal
                },
            ),
            Span::styled(format!("Hopper: {}%", cfg.hopper_width_percent), normal),
        ]),
        Line::from(vec![
            Span::styled(
                "2. ",
                if current == Some(LayoutTarget::Queue) {
                    highlight
                } else {
                    normal
                },
            ),
            Span::styled(format!("Queue: {}%", queue_width), normal),
        ]),
        Line::from(vec![
            Span::styled(
                "3. ",
                if current == Some(LayoutTarget::History) {
                    highlight
                } else {
                    normal
                },
            ),
            Span::styled(format!("History: {} chars", cfg.history_width), normal),
        ]),
        Line::from(""),
        Line::from(Span::styled("Controls:", app.theme.highlight_style())),
        Line::from("  +/- : Adjust selected dimension"),
        Line::from("  r   : Reset selected to default"),
        Line::from("  R   : Reset all to defaults"),
        Line::from("  s   : Save layout"),
        Line::from("  q   : Cancel (discard changes)"),
        Line::from(""),
        Line::from(Span::styled(
            &app.layout_adjust_message,
            app.theme.accent_style(),
        )),
    ];

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(paragraph, inner);
}

fn draw_confirmation_modal(f: &mut ratatui::Frame, app: &App, area: Rect) {
    if app.input_mode != InputMode::Confirmation {
        return;
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .title(" Confirmation ")
        .style(app.theme.panel_style())
        .border_style(app.theme.border_active_style());

    let area = centered_fixed_rect(area, 60, 8);
    f.render_widget(ratatui::widgets::Clear, area); // Clear background
    f.render_widget(block.clone(), area);

    // Use inner area with padding for content
    let content_area = block.inner(area);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),    // Message
            Constraint::Length(1), // Spacer
            Constraint::Length(1), // Actions
        ])
        .margin(1)
        .split(content_area);

    let msg = Paragraph::new(app.confirmation_msg.as_str())
        .wrap(Wrap { trim: true })
        .alignment(ratatui::layout::Alignment::Center)
        .style(app.theme.text_style());

    f.render_widget(msg, chunks[0]);

    let actions = Paragraph::new("Enter/y: Confirm  |  Esc/n: Cancel")
        .alignment(ratatui::layout::Alignment::Center)
        .style(app.theme.accent_style().add_modifier(Modifier::BOLD));

    f.render_widget(actions, chunks[2]);
}

fn centered_fixed_rect(r: Rect, width: u16, height: u16) -> Rect {
    let empty_x = r.width.saturating_sub(width) / 2;
    let empty_y = r.height.saturating_sub(height) / 2;

    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(empty_y),
            Constraint::Length(height),
            Constraint::Length(empty_y),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(empty_x),
            Constraint::Length(width),
            Constraint::Length(empty_x),
        ])
        .split(popup_layout[1])[1]
}

fn format_bytes_rate(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn ui(f: &mut ratatui::Frame, app: &App) {
    // If wizard is active, render wizard overlay instead
    if app.show_wizard {
        draw_wizard(f, app);
        return;
    }

    f.render_widget(Block::default().style(app.theme.base_style()), f.size());

    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(1), // Combined Footer + Status
        ])
        .split(f.size());

    let cfg = app.config.lock().unwrap();
    let main_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(14),
            Constraint::Min(0),
            Constraint::Length(cfg.history_width),
        ])
        .split(root[0]);
    drop(cfg);

    draw_rail(f, app, main_layout[0]);

    match app.current_tab {
        AppTab::Transfers => draw_transfers(f, app, main_layout[1]),
        AppTab::Quarantine => draw_quarantine(f, app, main_layout[1]),
        AppTab::Settings => draw_settings(f, app, main_layout[1]),
    }

    // Render History / Details panel (Right side) - With Metrics support
    let right_panel_full = main_layout[2];

    // Check if metrics enabled
    let cfg = app.config.lock().unwrap();
    let metrics_enabled = cfg.host_metrics_enabled;
    drop(cfg);
    let (metrics_area, right_panel) = if metrics_enabled {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(0), Constraint::Length(4)])
            .split(right_panel_full);
        (Some(chunks[1]), chunks[0])
    } else {
        (None, right_panel_full)
    };

    if let Some(area) = metrics_area {
        draw_metrics_panel(f, app, area);
    }

    // Logic:
    // 1. If Queue focused & selected -> Replace generic History list with Job Details (retaining existing behavior for Queue focus).
    // 2. If History focused -> Split panel: Top=List, Bottom=Details.
    // 3. Otherwise -> Show full History list.

    if app.current_tab == AppTab::Quarantine
        && !app.quarantine.is_empty()
        && app.selected_quarantine < app.quarantine.len()
    {
        // Quarantine Tab: Show selected threat details in full right panel
        draw_job_details(
            f,
            app,
            right_panel,
            &app.quarantine[app.selected_quarantine],
        );
    } else if app.focus == AppFocus::Queue
        && !app.visual_jobs.is_empty()
        && app.selected < app.visual_jobs.len()
    {
        // Queue focused: Show selected job details in full right panel
        let visual_item = &app.visual_jobs[app.selected];
        if let Some(idx) = visual_item.index_in_jobs.or(visual_item.first_job_index) {
            draw_job_details(f, app, right_panel, &app.jobs[idx]);
        }
    } else if app.focus == AppFocus::History
        && !app.visual_history.is_empty()
        && app.selected_history < app.visual_history.len()
    {
        // History focused: Split view
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(right_panel);

        draw_history(f, app, chunks[0]);
        let visual_item = &app.visual_history[app.selected_history];
        if let Some(idx) = visual_item.index_in_jobs.or(visual_item.first_job_index) {
            draw_job_details(f, app, chunks[1], &app.history[idx]);
        }
    } else {
        // Default: Full history list
        draw_history(f, app, right_panel);
    }
    draw_footer(f, app, root[1]);

    // Render Modals last (on top)
    draw_layout_adjustment_overlay(f, app, f.size());
    draw_confirmation_modal(f, app, f.size());
}

fn draw_metrics_panel(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let m = &app.last_metrics;
    let read_fmt = format_bytes_rate(m.disk_read_bytes_sec);
    let write_fmt = format_bytes_rate(m.disk_write_bytes_sec);
    let rx_fmt = format_bytes_rate(m.net_rx_bytes_sec);
    let tx_fmt = format_bytes_rate(m.net_tx_bytes_sec);

    let mk_style = app.theme.accent_style().add_modifier(Modifier::BOLD);
    let mk_val = app.theme.text_style();

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .border_style(app.theme.border_style())
        .title(" Metrics ")
        .style(app.theme.panel_style());

    let inner_area = block.inner(area);
    f.render_widget(block, area);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner_area);

    let disk_content = vec![
        Line::from(vec![
            Span::styled("Disk Read:  ", mk_style),
            Span::styled(&read_fmt, mk_val),
        ]),
        Line::from(vec![
            Span::styled("Disk Write: ", mk_style),
            Span::styled(&write_fmt, mk_val),
        ]),
    ];
    f.render_widget(Paragraph::new(disk_content), chunks[0]);

    let net_content = vec![
        Line::from(vec![
            Span::styled("Net Down: ", mk_style),
            Span::styled(&rx_fmt, mk_val),
        ]),
        Line::from(vec![
            Span::styled("Net Up:   ", mk_style),
            Span::styled(&tx_fmt, mk_val),
        ]),
    ];
    f.render_widget(Paragraph::new(net_content), chunks[1]);
}

fn draw_wizard(f: &mut ratatui::Frame, app: &App) {
    let area = f.size();

    // Center the wizard
    let wizard_area = centered_rect(area, 60, 70);

    // Clear background
    f.render_widget(Block::default().style(app.theme.base_style()), area);

    let step_title = match app.wizard.step {
        WizardStep::Paths => "Step 1/4: Directory Paths",
        WizardStep::Scanner => "Step 2/4: ClamAV Scanner",
        WizardStep::S3 => "Step 3/4: S3 Storage",
        WizardStep::Performance => "Step 4/4: Performance",
        WizardStep::Done => "Setup Complete!",
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .title(format!(" {} Setup Wizard ", "🚀"))
        .border_style(app.theme.border_active_style());

    let inner = block.inner(wizard_area);
    f.render_widget(block, wizard_area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Title
            Constraint::Min(0),    // Fields
            Constraint::Length(2), // Navigation
        ])
        .split(inner);

    // Step title
    let title = Paragraph::new(step_title)
        .style(app.theme.highlight_style())
        .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(title, chunks[0]);

    // Fields based on step
    let fields: Vec<(&str, &str)> = match app.wizard.step {
        WizardStep::Paths => vec![
            ("Staging Directory", &app.wizard.staging_dir),
            ("Quarantine Directory", &app.wizard.quarantine_dir),
        ],
        WizardStep::Scanner => vec![
            ("ClamAV Host", &app.wizard.clamd_host),
            ("ClamAV Port", &app.wizard.clamd_port),
        ],
        WizardStep::S3 => vec![
            ("Bucket Name", &app.wizard.bucket),
            ("Region", &app.wizard.region),
            ("Endpoint (optional)", &app.wizard.endpoint),
            ("Access Key", &app.wizard.access_key),
            ("Secret Key", &app.wizard.secret_key),
        ],
        WizardStep::Performance => vec![
            ("Part Size (MB)", &app.wizard.part_size),
            ("Concurrency", &app.wizard.concurrency),
        ],
        WizardStep::Done => vec![],
    };

    if app.wizard.step == WizardStep::Done {
        let done_text = Paragraph::new("Configuration saved! Press Enter to continue.")
            .style(app.theme.status_style(StatusKind::Success))
            .alignment(ratatui::layout::Alignment::Center);
        f.render_widget(done_text, chunks[1]);
    } else {
        let field_items: Vec<Line> = fields
            .iter()
            .enumerate()
            .flat_map(|(i, (label, value))| {
                let is_selected = i == app.wizard.field;
                let is_editing = is_selected && app.wizard.editing;

                let label_style = if is_selected {
                    app.theme.highlight_style()
                } else {
                    app.theme.muted_style()
                };

                let value_display = if is_editing {
                    format!("{}█", value)
                } else {
                    value.to_string()
                };

                let value_style = if is_editing {
                    app.theme.input_style(true)
                } else if is_selected {
                    app.theme.selection_soft_style()
                } else {
                    app.theme.dim_style()
                };

                vec![
                    Line::from(Span::styled(format!("  {}", label), label_style)),
                    Line::from(Span::styled(format!("  > {}", value_display), value_style)),
                    Line::from(""),
                ]
            })
            .collect();

        let fields_widget = Paragraph::new(field_items);
        f.render_widget(fields_widget, chunks[1]);
    }

    // Navigation hints
    let nav_text = if app.wizard.step == WizardStep::Done {
        "Enter: Continue"
    } else if app.wizard.editing {
        "Enter: Confirm | Esc: Cancel"
    } else {
        "↑/↓: Select | Enter: Edit | Tab: Next Step | Shift+Tab: Back | q: Quit"
    };

    let nav = Paragraph::new(nav_text)
        .style(app.theme.dim_style())
        .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(nav, chunks[2]);
}

fn draw_jobs(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::Queue {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let header_cells = ["File", "Status", "Size", "Progress", "Time"]
        .iter()
        .map(|h| Cell::from(*h).style(app.theme.header_style()));
    let header = Row::new(header_cells)
        .style(app.theme.header_style())
        .height(1);

    let total_rows = app.visual_jobs.len();
    let display_height = area.height.saturating_sub(4) as usize; // Border + Header
    let offset = calculate_list_offset(app.selected, total_rows, display_height);

    let rows = app
        .visual_jobs
        .iter()
        .enumerate()
        .skip(offset)
        .take(display_height)
        .map(|(idx, item)| {
            let is_selected = (idx == app.selected) && (app.focus == AppFocus::Queue);
            let row_style = if is_selected {
                app.theme.selection_style()
            } else {
                app.theme.row_style(idx % 2 != 0)
            };

            // Indentation
            let indent = " ".repeat(item.depth * 2);
            let text = if item.text.len() > 35 {
                format!("{}…", &item.text[..34])
            } else {
                item.text.clone()
            };

            let display_name = if item.is_folder {
                format!("{}📁 {}", indent, text)
            } else {
                format!("{}📄 {}", indent, text)
            };

            if let Some(job_idx) = item.index_in_jobs {
                // It's a file with a job
                let job = &app.jobs[job_idx];

                let p_str = {
                    if job.status == "uploading" {
                        let p_map = app.progress.lock().unwrap();
                        let entry = p_map.get(&job.id).cloned();
                        drop(p_map);
                        if let Some(info) = entry {
                            let bar_width = 10;
                            let filled = (info.percent.clamp(0.0, 100.0) / 100.0
                                * (bar_width as f64))
                                as usize;
                            format!(
                                "{}{} {:.0}%",
                                "█".repeat(filled),
                                "░".repeat(bar_width - filled),
                                info.percent
                            )
                        } else {
                            "░░░░░░░░░░ 0%".to_string()
                        }
                    } else if job.status == "complete" {
                        "██████████ 100%".to_string()
                    } else if job.status == "quarantined" || job.status == "quarantined_removed" {
                        "XXXXXXXXXX ERR".to_string()
                    } else if job.status == "error" || job.status == "failed" {
                        "!! ERROR !!".to_string()
                    } else {
                        "----------".to_string()
                    }
                };

                let time_str = format_relative_time(&job.created_at);

                let status_style = if is_selected {
                    app.theme.selection_style()
                } else {
                    app.theme.status_style(status_kind(job.status.as_str()))
                };
                let progress_style = if is_selected {
                    app.theme.selection_style()
                } else {
                    app.theme.progress_style()
                };

                Row::new(vec![
                    Cell::from(display_name),
                    Cell::from(job.status.clone()).style(status_style),
                    Cell::from(format_bytes(job.size_bytes as u64)),
                    Cell::from(p_str).style(progress_style),
                    Cell::from(time_str),
                ])
                .style(row_style)
            } else {
                // Folder Row
                Row::new(vec![
                    Cell::from(display_name),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                ])
                .style(row_style)
            }
        });

    let table = Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(16),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(app.theme.border_type)
            .title(" Queue (Jobs) ")
            .border_style(focus_style)
            .style(app.theme.panel_style()),
    );

    f.render_widget(table, area);
}

fn format_relative_time(timestamp: &str) -> String {
    use chrono::{DateTime, Local};

    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp) {
        let now = Local::now();
        let duration = now.signed_duration_since(dt);

        let secs = duration.num_seconds();
        if secs < 60 {
            format!("{}s ago", secs)
        } else if secs < 3600 {
            format!("{}m ago", secs / 60)
        } else if secs < 86400 {
            format!("{}h ago", secs / 3600)
        } else {
            format!("{}d ago", secs / 86400)
        }
    } else {
        // Try parsing as simple datetime
        timestamp.split('T').next().unwrap_or(timestamp).to_string()
    }
}

fn format_size(size: u64) -> String {
    if size == 0 {
        return "-".to_string();
    }
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut s = size as f64;
    let mut unit = 0;
    while s >= 1024.0 && unit < UNITS.len() - 1 {
        s /= 1024.0;
        unit += 1;
    }
    format!("{:.1} {}", s, UNITS[unit])
}

fn format_modified(time: Option<SystemTime>) -> String {
    match time {
        Some(t) => {
            let datetime: chrono::DateTime<chrono::Local> = t.into();
            datetime.format("%Y-%m-%d %H:%M").to_string()
        }
        None => "-".to_string(),
    }
}

fn fuzzy_match(pattern: &str, text: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    let pattern = pattern.to_lowercase();
    let text = text.to_lowercase();
    let mut pattern_chars = pattern.chars();
    let mut current_char = pattern_chars.next();

    for c in text.chars() {
        if let Some(p) = current_char {
            if c == p {
                current_char = pattern_chars.next();
            }
        } else {
            return true;
        }
    }
    current_char.is_none()
}

fn draw_browser(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let is_focused = app.focus == AppFocus::Browser;
    let focus_style = if is_focused {
        if app.input_mode == InputMode::Browsing || app.input_mode == InputMode::Filter {
            // Activated Hopper: Use Success/Green style to indicate active navigation
            app.theme.status_style(StatusKind::Success)
        } else {
            app.theme.border_active_style()
        }
    } else {
        app.theme.border_style()
    };

    let title = match app.input_mode {
        InputMode::Browsing => " Hopper (Browsing) ",
        InputMode::Filter => " Hopper (Filter) ",
        InputMode::Normal if is_focused => " Hopper (Press 'a' to browse) ",
        InputMode::Normal => " Hopper ",
        InputMode::Confirmation => " Hopper ",
        InputMode::LayoutAdjust => " Hopper ",
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .title(title)
        .border_style(focus_style)
        .style(app.theme.panel_style());

    let inner_area = block.inner(area);
    f.render_widget(block, area);

    if inner_area.height < 3 {
        return;
    }

    // Breadcrumbs
    let path_str = app.picker.cwd.to_string_lossy().to_string();
    let breadcrumbs = Line::from(vec![
        Span::styled(" 📂 ", app.theme.accent_style()),
        Span::styled(
            path_str,
            app.theme.text_style().add_modifier(Modifier::BOLD),
        ),
    ]);

    let breadcrumb_area = Rect::new(inner_area.x, inner_area.y, inner_area.width, 1);
    f.render_widget(Paragraph::new(breadcrumbs), breadcrumb_area);

    // Filter indicator
    if !app.input_buffer.is_empty() || app.input_mode == InputMode::Filter {
        let filter_line = Line::from(vec![
            Span::styled(" 🔍 ", app.theme.muted_style()),
            Span::styled(
                if app.picker.search_recursive {
                    "Recursive Filter: "
                } else {
                    "Filter: "
                },
                app.theme.muted_style(),
            ),
            Span::styled(&app.input_buffer, app.theme.accent_style()),
            Span::styled(
                if app.input_mode == InputMode::Filter {
                    " █"
                } else {
                    ""
                },
                app.theme.accent_style(),
            ),
        ]);
        let filter_area = Rect::new(inner_area.x, inner_area.y + 1, inner_area.width, 1);
        f.render_widget(Paragraph::new(filter_line), filter_area);
    }

    let table_area = Rect::new(
        inner_area.x,
        inner_area.y + if app.input_buffer.is_empty() { 1 } else { 2 },
        inner_area.width,
        inner_area
            .height
            .saturating_sub(if app.input_buffer.is_empty() { 1 } else { 2 }),
    );

    // Live Filtering
    let filter = if app.input_mode == InputMode::Filter {
        app.input_buffer.clone()
    } else {
        app.input_buffer.clone() // Still use it if not empty but in Normal mode
    };

    let filtered_entries: Vec<(usize, &FileEntry)> = app
        .picker
        .entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.is_parent || filter.is_empty() || fuzzy_match(&filter, &e.name))
        .collect();

    let total_rows = filtered_entries.len();
    let display_height = table_area.height.saturating_sub(1) as usize; // -1 for header

    // Find current index in filtered list
    let filtered_selected = filtered_entries
        .iter()
        .position(|(i, _)| *i == app.picker.selected)
        .unwrap_or(0);
    let offset = calculate_list_offset(filtered_selected, total_rows, display_height);

    let header = Row::new(vec![
        Cell::from("Name"),
        Cell::from("Size"),
        Cell::from("Modified"),
    ])
    .style(app.theme.header_style())
    .height(1);

    let rows: Vec<Row> = filtered_entries
        .iter()
        .skip(offset)
        .take(display_height)
        .enumerate()
        .map(|(visible_idx, (orig_idx, entry))| {
            let prefix = if entry.is_dir { "📁 " } else { "📄 " };
            let expand = if entry.is_dir && !entry.is_parent {
                if app.picker.expanded.contains(&entry.path) {
                    "[-] "
                } else {
                    "[+] "
                }
            } else {
                ""
            };

            let name_styled =
                format!("{}{}{}", "  ".repeat(entry.depth), expand, prefix) + &entry.name;
            let size_str = if entry.is_dir {
                "-".to_string()
            } else {
                format_size(entry.size)
            };
            let mod_str = format_modified(entry.modified);

            let is_selected =
                (*orig_idx == app.picker.selected) && (app.focus == AppFocus::Browser);
            let is_staged = app.picker.selected_paths.contains(&entry.path);

            let mut style = if is_selected {
                app.theme.selection_style()
            } else if (visible_idx + offset) % 2 == 0 {
                app.theme.row_style(false)
            } else {
                app.theme.row_style(true)
            };

            if is_staged {
                if is_selected {
                    // Combined: Selected + Staged.
                    // Assuming Selection is Reversed, setting FG to Green makes BG Green.
                    style = style.fg(ratatui::style::Color::Green);
                } else {
                    style = app.theme.status_style(StatusKind::Success);
                }
            }

            Row::new(vec![
                Cell::from(name_styled),
                Cell::from(size_str),
                Cell::from(mod_str),
            ])
            .style(style)
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Fill(1),
            Constraint::Length(10),
            Constraint::Length(16),
        ],
    )
    .header(header)
    .style(app.theme.panel_style());

    f.render_widget(table, table_area);
}

fn draw_settings(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let has_focus =
        app.focus == AppFocus::SettingsCategory || app.focus == AppFocus::SettingsFields;
    let focus_style = if has_focus {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let title = " Configuration ";
    let outer_block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .title(title)
        .border_style(focus_style)
        .style(app.theme.panel_style());

    let inner_area = outer_block.inner(area);
    f.render_widget(outer_block, area);

    let main_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(16), Constraint::Min(0)])
        .split(inner_area);

    // Sidebar Categories
    let categories = vec!["S3 Storage", "Scanner", "Performance", "Theme"];
    let cat_items: Vec<ListItem> = categories
        .iter()
        .enumerate()
        .map(|(id, name)| {
            let is_active = app.settings.active_category as usize == id;
            let style = if is_active {
                if app.focus == AppFocus::SettingsCategory {
                    app.theme.selection_style()
                } else {
                    app.theme.accent_style().add_modifier(Modifier::BOLD)
                }
            } else {
                app.theme.text_style()
            };
            ListItem::new(format!(" {} ", name)).style(style)
        })
        .collect();

    let sidebar_block = Block::default()
        .borders(Borders::RIGHT)
        .border_type(app.theme.border_type)
        .border_style(app.theme.border_style())
        .style(app.theme.panel_style());
    let sidebar = List::new(cat_items).block(sidebar_block);
    f.render_widget(sidebar, main_layout[0]);

    // Fields area
    let fields_area = main_layout[1];
    let display_height = fields_area.height as usize;
    let fields_per_view = display_height / 3;

    let fields = match app.settings.active_category {
        SettingsCategory::S3 => vec![
            ("S3 Endpoint", app.settings.endpoint.as_str()),
            ("S3 Bucket", app.settings.bucket.as_str()),
            ("S3 Region", app.settings.region.as_str()),
            ("S3 Prefix", app.settings.prefix.as_str()),
            ("S3 Access Key", app.settings.access_key.as_str()),
            (
                "S3 Secret Key",
                if app.settings.editing && app.settings.selected_field == 5 {
                    app.settings.secret_key.as_str()
                } else {
                    "*******"
                },
            ),
        ],
        SettingsCategory::Scanner => vec![
            ("ClamAV Host", app.settings.clamd_host.as_str()),
            ("ClamAV Port", app.settings.clamd_port.as_str()),
            (
                "Scan Chunk Size (MB)",
                app.settings.scan_chunk_size.as_str(),
            ),
            (
                "Enable Scanner",
                if app.settings.scanner_enabled {
                    "[X] Enabled"
                } else {
                    "[ ] Disabled"
                },
            ),
        ],
        SettingsCategory::Performance => vec![
            ("Part Size (MB)", app.settings.part_size.as_str()),
            (
                "Global Concurrency",
                app.settings.concurrency_global.as_str(),
            ),
            ("Scan Concurrency", app.settings.scan_concurrency.as_str()),
            (
                "Staging Mode",
                if app.settings.staging_mode_direct {
                    "[X] Direct (no copy)"
                } else {
                    "[ ] Copy"
                },
            ),
            (
                "Delete Source After Upload",
                if app.settings.delete_source_after_upload {
                    "[X] Enabled"
                } else {
                    "[ ] Disabled"
                },
            ),
            (
                "Show Metrics",
                if app.settings.host_metrics_enabled {
                    "[X] Enabled"
                } else {
                    "[ ] Disabled"
                },
            ),
        ],
        SettingsCategory::Theme => vec![("Theme", app.settings.theme.as_str())],
    };

    if fields_per_view == 0 {
        return;
    }

    let field_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![Constraint::Length(3); fields_per_view])
        .split(fields_area);

    let mut offset = 0;
    if app.settings.selected_field >= fields_per_view {
        offset = app
            .settings
            .selected_field
            .saturating_sub(fields_per_view / 2);
    }

    // Safety check for offset
    if fields.len() > fields_per_view && offset + fields_per_view > fields.len() {
        offset = fields.len() - fields_per_view;
    }

    for (i, (title, value)) in fields.iter().enumerate().skip(offset).take(fields_per_view) {
        let chunk_idx = i - offset;
        if chunk_idx >= field_chunks.len() {
            break;
        }

        let is_selected =
            (app.settings.selected_field == i) && (app.focus == AppFocus::SettingsFields);
        let border_style = if is_selected {
            if app.settings.editing {
                app.theme.input_border_style(true)
            } else {
                app.theme.border_active_style()
            }
        } else {
            app.theme.border_style()
        };

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(app.theme.border_type)
            .title(*title)
            .border_style(border_style)
            .style(app.theme.panel_style());

        let value_style = if is_selected
            && app.settings.editing
            && app.settings.active_category != SettingsCategory::Theme
        {
            app.theme.input_style(true)
        } else if is_selected {
            app.theme.text_style().add_modifier(Modifier::BOLD)
        } else {
            app.theme.text_style()
        };

        let p = Paragraph::new(*value).block(block).style(value_style);
        f.render_widget(p, field_chunks[chunk_idx]);

        // Draw cursor if editing non-theme fields
        if is_selected
            && app.settings.editing
            && app.settings.active_category != SettingsCategory::Theme
        {
            f.set_cursor(
                field_chunks[chunk_idx].x + 1 + value.len() as u16,
                field_chunks[chunk_idx].y + 1,
            );
        }
    }

    // Special rendering for Theme Selection List
    if app.settings.editing && app.settings.active_category == SettingsCategory::Theme {
        let max_width = fields_area.width.saturating_sub(2);
        let max_height = fields_area.height.saturating_sub(2);
        if max_width < 22 || max_height < 8 {
            return;
        }

        let width = max_width.min(62);
        let height = max_height.min(14).max(8);
        let theme_area = Rect {
            x: fields_area.x + 1,
            y: fields_area.y + 1,
            width,
            height,
        };

        let theme_area = theme_area.intersection(fields_area);
        f.render_widget(ratatui::widgets::Clear, theme_area);

        let show_preview = theme_area.width >= 54;
        let gap = if show_preview { 1 } else { 0 };
        let list_width = if show_preview { 24 } else { theme_area.width };
        let preview_width = theme_area.width.saturating_sub(list_width + gap);

        let list_area = Rect {
            x: theme_area.x,
            y: theme_area.y,
            width: list_width,
            height: theme_area.height,
        };

        let preview_area = Rect {
            x: theme_area.x + list_width + gap,
            y: theme_area.y,
            width: preview_width,
            height: theme_area.height,
        };

        let current_theme = app.settings.theme.as_str();
        let items: Vec<ListItem> = app
            .theme_names
            .iter()
            .map(|&name| {
                let is_selected = name == current_theme;
                let style = if is_selected {
                    app.theme.selection_style()
                } else {
                    app.theme.text_style()
                };
                ListItem::new(format!(" {} ", name)).style(style)
            })
            .collect();

        let list_block = Block::default()
            .borders(Borders::ALL)
            .border_type(app.theme.border_type)
            .title(" Select Theme ")
            .border_style(app.theme.border_active_style())
            .style(app.theme.panel_style());

        let max_visible = list_area.height.saturating_sub(2) as usize;
        let total = app.theme_names.len();
        let mut list_state = ratatui::widgets::ListState::default();
        let mut visible_items = items;
        if max_visible > 0 && total > max_visible {
            if let Some(idx) = app.theme_names.iter().position(|&n| n == current_theme) {
                let mut start = idx.saturating_sub(max_visible / 2);
                if start + max_visible > total {
                    start = total.saturating_sub(max_visible);
                }
                let end = (start + max_visible).min(total);
                visible_items = app
                    .theme_names
                    .iter()
                    .skip(start)
                    .take(end - start)
                    .map(|&name| {
                        let is_selected = name == current_theme;
                        let style = if is_selected {
                            app.theme.selection_style()
                        } else {
                            app.theme.text_style()
                        };
                        ListItem::new(format!(" {} ", name)).style(style)
                    })
                    .collect();
                list_state.select(Some(idx.saturating_sub(start)));
            }
        } else if let Some(idx) = app.theme_names.iter().position(|&n| n == current_theme) {
            list_state.select(Some(idx));
        }

        let list = List::new(visible_items).block(list_block);
        f.render_stateful_widget(list, list_area, &mut list_state);

        if show_preview && preview_area.width > 0 {
            let preview_theme = Theme::from_name(current_theme);
            render_theme_preview(f, &preview_theme, preview_area);
        }
    }
}

fn render_theme_preview(f: &mut ratatui::Frame, theme: &Theme, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(theme.border_type)
        .title(format!(" Preview: {} ", theme.name))
        .border_style(theme.border_style())
        .style(theme.panel_style());
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.height < 6 || inner.width < 16 {
        return;
    }

    let progress_bar = "██████░░ 60%";
    let lines = vec![
        Line::from(Span::styled(" Header / Title ", theme.header_style())),
        Line::from(""),
        Line::from(Span::styled(" Selected Row ", theme.selection_style())),
        Line::from(vec![
            Span::styled(" Progress ", theme.text_style()),
            Span::styled(progress_bar, theme.progress_style()),
        ]),
        Line::from(vec![
            Span::styled(" OK ", theme.status_badge_style(StatusKind::Success)),
            Span::from(" "),
            Span::styled(" WARN ", theme.status_badge_style(StatusKind::Warning)),
            Span::from(" "),
            Span::styled(" ERR ", theme.status_badge_style(StatusKind::Error)),
        ]),
        Line::from(vec![
            Span::styled(" Input ", theme.input_style(false)),
            Span::from(" "),
            Span::styled("Accent", theme.accent_style().add_modifier(Modifier::BOLD)),
            Span::from(" "),
            Span::styled("Alt", theme.accent_alt_style().add_modifier(Modifier::BOLD)),
        ]),
    ];

    let preview = Paragraph::new(lines)
        .style(theme.text_style())
        .wrap(Wrap { trim: true });
    f.render_widget(preview, inner);
}

fn format_bytes(value: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    const TB: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;
    let v = value as f64;
    if v >= TB {
        format!("{:.2} TB", v / TB)
    } else if v >= GB {
        format!("{:.2} GB", v / GB)
    } else if v >= MB {
        format!("{:.2} MB", v / MB)
    } else if v >= KB {
        format!("{:.2} KB", v / KB)
    } else {
        format!("{} B", value)
    }
}

fn status_kind(status: &str) -> StatusKind {
    match status {
        "complete" => StatusKind::Success,
        "quarantined" | "failed" => StatusKind::Error,
        "quarantined_removed" => StatusKind::Info,
        "uploading" | "scanning" => StatusKind::Info,
        _ => StatusKind::Warning,
    }
}

fn draw_quarantine(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::Quarantine {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    // Header
    let header_cells = ["File", "Threat", "Status", "Time"]
        .iter()
        .map(|h| Cell::from(*h).style(app.theme.header_style()));
    let header = Row::new(header_cells)
        .style(app.theme.header_style())
        .height(1);

    let total_rows = app.quarantine.len();
    let display_height = area.height.saturating_sub(4) as usize; // Border + Header
    let offset = calculate_list_offset(app.selected_quarantine, total_rows, display_height);

    let rows = app
        .quarantine
        .iter()
        .enumerate()
        .skip(offset)
        .take(display_height)
        .map(|(idx, job)| {
            let is_selected =
                (idx == app.selected_quarantine) && (app.focus == AppFocus::Quarantine);
            let row_style = if is_selected {
                app.theme.selection_style()
            } else {
                app.theme.row_style(idx % 2 != 0)
            };

            let filename = std::path::Path::new(&job.source_path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| job.source_path.clone());
            let display_name = if filename.len() > 25 {
                format!("{}…", &filename[..24])
            } else {
                filename
            };

            let threat = extract_threat_name(job.scan_status.as_deref().unwrap_or(""));
            let status = if job.status == "quarantined_removed" {
                "Removed"
            } else {
                "Detected"
            };
            let time_str = format_relative_time(&job.created_at);

            let status_style = if is_selected {
                app.theme.selection_style()
            } else if job.status == "quarantined_removed" {
                app.theme.status_style(StatusKind::Info)
            } else {
                app.theme.status_style(StatusKind::Error)
            };

            Row::new(vec![
                Cell::from(display_name),
                Cell::from(threat).style(if is_selected {
                    Style::default()
                } else {
                    app.theme.status_style(StatusKind::Error)
                }),
                Cell::from(status).style(status_style),
                Cell::from(time_str),
            ])
            .style(row_style)
        });

    let table = Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(25),
            Constraint::Length(12),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(app.theme.border_type)
            .title(" Quarantine Zone ")
            .border_style(focus_style)
            .style(app.theme.panel_style()),
    );

    f.render_widget(table, area);
}

fn extract_threat_name(scan_status: &str) -> String {
    if scan_status.contains("FOUND") {
        // Example: "/path/to/file: Eicar-Test-Signature FOUND" or similar
        // Or if using clamav-client, it might be just "FOUND Eicar-Test-Signature"
        // Let's try to find the word before "FOUND" or the word after "FOUND" if it's "FOUND <Signature>"
        // Actually, typically it is "FOUND <Signature>" or "<Signature> FOUND"
        // Let's assume common format. If we split by space:
        // "FOUND Eicar-Signature" -> parts[1]
        let parts: Vec<&str> = scan_status.split_whitespace().collect();
        if let Some(pos) = parts.iter().position(|&s| s == "FOUND") {
            // Check next
            if pos + 1 < parts.len() {
                return parts[pos + 1].to_string();
            }
            // Check prev
            if pos > 0 {
                let candidate = parts[pos - 1];
                // Simple heuristic: if it doesn't look like a path, use it
                if !candidate.contains('/') && !candidate.contains('\\') {
                    return candidate.to_string();
                }
            }
        }
        // Fallback: just return the status but truncated
        if scan_status.len() > 20 {
            format!("{}...", &scan_status[..20])
        } else {
            scan_status.to_string()
        }
    } else {
        "-".to_string()
    }
}

fn adjust_layout_dimension(config: &mut Arc<Mutex<Config>>, target: LayoutTarget, delta: i16) {
    let mut cfg = config.lock().unwrap();
    match target {
        LayoutTarget::Hopper => {
            cfg.hopper_width_percent =
                (cfg.hopper_width_percent as i16 + delta * 5).clamp(20, 80) as u16;
        }
        LayoutTarget::Queue => {
            // Queue is automatically 100 - hopper, so adjust hopper in reverse
            cfg.hopper_width_percent =
                (cfg.hopper_width_percent as i16 - delta * 5).clamp(20, 80) as u16;
        }
        LayoutTarget::History => {
            cfg.history_width = (cfg.history_width as i16 + delta).clamp(40, 100) as u16;
        }
    }
}

fn reset_layout_dimension(config: &mut Arc<Mutex<Config>>, target: LayoutTarget) {
    let mut cfg = config.lock().unwrap();
    match target {
        LayoutTarget::Hopper | LayoutTarget::Queue => cfg.hopper_width_percent = 50,
        LayoutTarget::History => cfg.history_width = 60,
    }
}

fn reset_all_layout_dimensions(config: &mut Arc<Mutex<Config>>) {
    let mut cfg = config.lock().unwrap();
    cfg.hopper_width_percent = 50;
    cfg.history_width = 60;
}

fn update_layout_message(app: &mut App, target: LayoutTarget) {
    let cfg = app.config.lock().unwrap();
    app.layout_adjust_message = match target {
        LayoutTarget::Hopper => format!("Hopper Width: {}% (20-80)", cfg.hopper_width_percent),
        LayoutTarget::Queue => format!("Queue Width: {}% (20-80)", 100 - cfg.hopper_width_percent),
        LayoutTarget::History => format!("History Width: {} chars (40-100)", cfg.history_width),
    };
}
