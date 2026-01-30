use crate::config::Config;
use crate::db::{list_active_jobs, list_history_jobs, list_quarantined_jobs, JobRow};
use crate::ingest::ingest_path;
use crate::state::ProgressInfo;
use crate::theme::{StatusKind, Theme};
use crate::watch::Watcher;
use anyhow::Result;
use std::sync::mpsc;
use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Wrap};
// #[allow(unused_imports)]
// use ratatui::layout::Alignment;

#[allow(dead_code)]
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
    Quarantine, // Only in Quarantine tab
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
    active_category: SettingsCategory,
    selected_field: usize,
    editing: bool,
    theme: String,
    original_theme: Option<String>,
    scanner_enabled: bool,
    staging_mode_direct: bool,  // true = Direct mode, false = Copy mode
    delete_source_after_upload: bool,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalAction {
    None,
    ClearHistory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Browsing,  // Actively navigating in Hopper
    Filter,
    Confirmation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PickerView {
    List,
    Tree,
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
}

impl App {
    fn new(conn: Arc<Mutex<Connection>>, config: Arc<Mutex<Config>>, progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>) -> Self {
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
                let status = if std::net::TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(1)).is_ok() {
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
        
        // Auto-fix selected if out of bounds
        if self.selected >= self.jobs.len() {
            self.selected = self.jobs.len().saturating_sub(1);
        }
        if self.selected_history >= self.history.len() {
            self.selected_history = self.history.len().saturating_sub(1);
        }
        if self.selected_quarantine >= self.quarantine.len() {
            self.selected_quarantine = self.quarantine.len().saturating_sub(1);
        }
        
        self.last_refresh = Instant::now();
        Ok(())
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

        let filtered_indices: Vec<usize> = self.picker.entries.iter().enumerate()
            .filter(|(_, e)| e.is_parent || e.name.to_lowercase().contains(&filter))
            .map(|(i, _)| i)
            .collect();

        if filtered_indices.is_empty() { return; }

        let current_pos = filtered_indices.iter().position(|i| *i == self.picker.selected).unwrap_or(0);
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

        let filtered_indices: Vec<usize> = self.picker.entries.iter().enumerate()
            .filter(|(_, e)| e.is_parent || e.name.to_lowercase().contains(&filter))
            .map(|(i, _)| i)
            .collect();

        if filtered_indices.is_empty() { return; }

        let current_pos = filtered_indices.iter().position(|i| *i == self.picker.selected).unwrap_or(0);
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
        if filter.is_empty() { return; }

        let filtered_indices: Vec<usize> = self.picker.entries.iter().enumerate()
            .filter(|(_, e)| e.is_parent || e.name.to_lowercase().contains(&filter))
            .map(|(i, _)| i)
            .collect();

        if !filtered_indices.is_empty() && !filtered_indices.contains(&self.picker.selected) {
            self.picker.selected = filtered_indices[0];
        }
    }
}

pub fn run_tui(conn_mutex: Arc<Mutex<Connection>>, cfg: Arc<Mutex<Config>>, progress: Arc<Mutex<HashMap<i64, ProgressInfo>>>, needs_wizard: bool) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut app = App::new(conn_mutex.clone(), cfg.clone(), progress);
    app.show_wizard = needs_wizard;
    
    {
        let conn = conn_mutex.lock().unwrap();
        app.refresh_jobs(&conn)?;
    }

    let tick_rate = Duration::from_millis(200);
    loop {
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
            // ... (keep commented or simplified logic for now)
            app.last_watch_scan = Instant::now();
        }

        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                // Handle Confirmation Mode
                if app.input_mode == InputMode::Confirmation {
                     match key.code {
                        KeyCode::Char('y') | KeyCode::Enter => {
                            // Execute Action
                            if app.pending_action == ModalAction::ClearHistory {
                                let conn = conn_mutex.lock().unwrap();
                                let filter_str = if app.history_filter == HistoryFilter::All { None } else { Some(app.history_filter.as_str()) };
                                if let Err(e) = crate::db::clear_history(&conn, filter_str) {
                                    app.status_message = format!("Error clearing history: {}", e);
                                } else {
                                    app.selected_history = 0;
                                    app.status_message = format!("History cleared ({})", app.history_filter.as_str());
                                    let _ = app.refresh_jobs(&conn);
                                }
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
                                cfg.s3_bucket = if app.wizard.bucket.is_empty() { None } else { Some(app.wizard.bucket.clone()) };
                                cfg.s3_region = if app.wizard.region.is_empty() { None } else { Some(app.wizard.region.clone()) };
                                cfg.s3_endpoint = if app.wizard.endpoint.is_empty() { None } else { Some(app.wizard.endpoint.clone()) };
                                cfg.s3_access_key = if app.wizard.access_key.is_empty() { None } else { Some(app.wizard.access_key.clone()) };
                                cfg.s3_secret_key = if app.wizard.secret_key.is_empty() { None } else { Some(app.wizard.secret_key.clone()) };
                                cfg.part_size_mb = app.wizard.part_size.parse().unwrap_or(64);
                                cfg.concurrency_upload_global = app.wizard.concurrency.parse().unwrap_or(4);
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
                    AppFocus::Browser => app.input_mode == InputMode::Filter || app.input_mode == InputMode::Browsing,
                    _ => false,
                };

                // Meta keys (Quit, Tab)
                if !capturing_input {
                    let old_focus = app.focus;
                    match key.code {
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
                                AppFocus::History | AppFocus::Quarantine | AppFocus::SettingsFields => AppFocus::Rail,
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
                                AppFocus::Browser | AppFocus::Quarantine | AppFocus::SettingsCategory => AppFocus::Rail,
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
                                AppFocus::Browser | AppFocus::Quarantine | AppFocus::SettingsCategory => AppFocus::Rail,
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
                    AppFocus::Rail => {
                         match key.code {
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
                         }
                    }
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
                                    KeyCode::Down | KeyCode::Char('j') => app.browser_move_down(),
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
                                    KeyCode::Down | KeyCode::Char('j') => app.browser_move_down(),
                                    KeyCode::PageUp => app.picker.page_up(),
                                    KeyCode::PageDown => app.picker.page_down(),
                                    KeyCode::Left | KeyCode::Char('h') | KeyCode::Backspace => app.picker.go_parent(),
                                    KeyCode::Right | KeyCode::Char('l') | KeyCode::Enter => {
                                        if let Some(entry) = app.picker.selected_entry() {
                                            if entry.is_dir {
                                                if app.picker.view == PickerView::Tree && key.code == KeyCode::Right {
                                                    app.picker.toggle_expand();
                                                } else {
                                                    app.picker.try_set_cwd(entry.path.clone());
                                                    app.input_buffer.clear();
                                                    app.picker.is_searching = false;
                                                }
                                            } else if key.code == KeyCode::Enter || key.code == KeyCode::Char('l') {
                                                // Only queue files on Enter or 'l', not Right arrow
                                                let conn = conn_mutex.lock().unwrap();
                                                let cfg_guard = cfg.lock().unwrap();
                                                let staging = cfg_guard.staging_dir.clone();
                                                let staging_mode = cfg_guard.staging_mode.clone();
                                                drop(cfg_guard);
                                                let _ = ingest_path(&conn, &staging, &staging_mode, &entry.path.to_string_lossy());
                                                app.status_message = "File added to queue".to_string();
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
                                        app.picker.search_recursive = !app.picker.search_recursive;
                                        app.picker.refresh();
                                        app.status_message = if app.picker.search_recursive { "Recursive search enabled" } else { "Recursive search disabled" }.to_string();
                                    }
                                    KeyCode::Char('c') => app.picker.clear_selected(),
                                    KeyCode::Char('s') => {
                                        let paths: Vec<PathBuf> = app.picker.selected_paths.iter().cloned().collect();
                                        if !paths.is_empty() {
                                            let conn = conn_mutex.lock().unwrap();
                                            let cfg_guard = cfg.lock().unwrap();
                                            let staging = cfg_guard.staging_dir.clone();
                                            let staging_mode = cfg_guard.staging_mode.clone();
                                            drop(cfg_guard);
                                            let mut total = 0;
                                            for path in paths {
                                                if let Ok(count) = ingest_path(&conn, &staging, &staging_mode, &path.to_string_lossy()) {
                                                    total += count;
                                                }
                                            }
                                            app.status_message = format!("Ingested {} items", total);
                                            app.picker.clear_selected();
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            InputMode::Filter => {
                                match key.code {
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
                                }
                            }
                            InputMode::Confirmation => {},
                        }
                    }
                    AppFocus::Queue => {
                         match key.code {
                            KeyCode::Up | KeyCode::Char('k') => if app.selected > 0 { app.selected -= 1; },
                            KeyCode::Down | KeyCode::Char('j') => if app.selected + 1 < app.jobs.len() { app.selected += 1; },
                            KeyCode::Char('R') => {
                                let conn = conn_mutex.lock().unwrap();
                                let _ = app.refresh_jobs(&conn);
                            }
                            KeyCode::Char('r') => {
                                if !app.jobs.is_empty() && app.selected < app.jobs.len() {
                                    let id = app.jobs[app.selected].id;
                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = crate::db::retry_job(&conn, id);
                                    app.status_message = format!("Retried job {}", id);
                                }
                            }
                            KeyCode::Char('d') | KeyCode::Delete => {
                                if !app.jobs.is_empty() && app.selected < app.jobs.len() {
                                    let id = app.jobs[app.selected].id;
                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = crate::db::delete_job(&conn, id);
                                    app.status_message = format!("Deleted job {}", id);
                                    if app.selected >= app.jobs.len() && app.selected > 0 { app.selected -= 1; }
                                }
                            }
                            KeyCode::Char('c') => {
                                // Clear all completed jobs
                                let conn = conn_mutex.lock().unwrap();
                                let ids_to_delete: Vec<i64> = app.jobs.iter()
                                    .filter(|j| j.status == "complete")
                                    .map(|j| j.id)
                                    .collect();
                                let count = ids_to_delete.len();
                                for id in ids_to_delete {
                                    let _ = crate::db::delete_job(&conn, id);
                                }
                                if count > 0 {
                                    app.status_message = format!("Cleared {} completed jobs", count);
                                    let _ = app.refresh_jobs(&conn);
                                }
                            }
                            _ => {}
                         }
                    }
                    AppFocus::History => {
                         match key.code {
                            KeyCode::Up | KeyCode::Char('k') => if app.selected_history > 0 { app.selected_history -= 1; },
                            KeyCode::Down | KeyCode::Char('j') => if app.selected_history + 1 < app.history.len() { app.selected_history += 1; },
                            KeyCode::Char('f') => {
                                app.history_filter = app.history_filter.next();
                                app.selected_history = 0; // Reset selection
                                app.status_message = format!("History Filter: {}", app.history_filter.as_str());
                                let conn = conn_mutex.lock().unwrap();
                                let _ = app.refresh_jobs(&conn);
                            }
                            KeyCode::Char('c') => {
                                // Trigger confirmation logic for History
                                app.input_mode = InputMode::Confirmation;
                                app.pending_action = ModalAction::ClearHistory;
                                app.confirmation_msg = format!("Clear {} history entries? This cannot be undone.", match app.history_filter {
                                    HistoryFilter::All => "ALL",
                                    HistoryFilter::Complete => "completed",
                                    HistoryFilter::Quarantined => "quarantined",
                                });
                            }
                            KeyCode::Char('d') | KeyCode::Delete => {
                                if !app.history.is_empty() && app.selected_history < app.history.len() {
                                    let job = &app.history[app.selected_history];
                                    let id = job.id;
                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = crate::db::delete_job(&conn, id);
                                    let _ = app.refresh_jobs(&conn);
                                    if app.selected_history >= app.history.len() && app.selected_history > 0 {
                                        app.selected_history -= 1;
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
                    AppFocus::Quarantine => {
                         match key.code {
                            KeyCode::Up | KeyCode::Char('k') => if app.selected_quarantine > 0 { app.selected_quarantine -= 1; },
                            KeyCode::Down | KeyCode::Char('j') => if app.selected_quarantine + 1 < app.quarantine.len() { app.selected_quarantine += 1; },
                            KeyCode::Char('d') | KeyCode::Delete => {
                                if !app.quarantine.is_empty() && app.selected_quarantine < app.quarantine.len() {
                                    let job = &app.quarantine[app.selected_quarantine];
                                    let id = job.id;
                                    let q_path = job.staged_path.clone();
                                    
                                    let conn = conn_mutex.lock().unwrap();
                                    let _ = crate::db::update_scan_status(&conn, id, "removed", "quarantined_removed");
                                    
                                    if let Some(p) = q_path {
                                        let _ = std::fs::remove_file(p);
                                    }
                                    
                                    app.status_message = format!("Threat neutralized: File '{}' deleted", job.source_path);
                                    
                                    if app.selected_quarantine >= app.quarantine.len() && app.selected_quarantine > 0 { app.selected_quarantine -= 1; }
                                }
                            }
                            KeyCode::Char('R') => {
                                let conn = conn_mutex.lock().unwrap();
                                let _ = app.refresh_jobs(&conn);
                            }
                            _ => {}
                         }
                    }
                    AppFocus::SettingsCategory => {
                         match key.code {
                            KeyCode::Up | KeyCode::Char('k') => {
                                app.settings.active_category = match app.settings.active_category {
                                    SettingsCategory::S3 => SettingsCategory::Theme,
                                    SettingsCategory::Scanner => SettingsCategory::S3,
                                    SettingsCategory::Performance => SettingsCategory::Scanner,
                                    SettingsCategory::Theme => SettingsCategory::Performance,
                                };
                                app.settings.selected_field = 0;
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                app.settings.active_category = match app.settings.active_category {
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
                            SettingsCategory::Performance => 4,
                            SettingsCategory::Theme => 1,
                        };
                        match key.code {
                            KeyCode::Enter => {
                                // Special handling for Boolean Toggles
                                let is_toggle = 
                                    (app.settings.active_category == SettingsCategory::Scanner && app.settings.selected_field == 3) ||
                                    (app.settings.active_category == SettingsCategory::Performance && app.settings.selected_field >= 2);
                                
                                if is_toggle {
                                    // Handle toggle based on category and field
                                    match (app.settings.active_category, app.settings.selected_field) {
                                        (SettingsCategory::Scanner, 3) => {
                                            app.settings.scanner_enabled = !app.settings.scanner_enabled;
                                            app.status_message = format!("Scanner {}", if app.settings.scanner_enabled { "Enabled" } else { "Disabled" });
                                        }
                                        (SettingsCategory::Performance, 2) => {
                                            app.settings.staging_mode_direct = !app.settings.staging_mode_direct;
                                            app.status_message = format!("Staging Mode: {}", if app.settings.staging_mode_direct { "Direct (no copy)" } else { "Copy (default)" });
                                        }
                                        (SettingsCategory::Performance, 3) => {
                                            app.settings.delete_source_after_upload = !app.settings.delete_source_after_upload;
                                            app.status_message = format!("Delete Source: {}", if app.settings.delete_source_after_upload { "Enabled" } else { "Disabled" });
                                        }
                                        _ => {}
                                    }
                                    
                                    // Trigger Auto-Save logic immediately
                                    let mut cfg = app.config.lock().unwrap();
                                    app.settings.apply_to_config(&mut cfg);
                                    let conn = conn_mutex.lock().unwrap();
                                    if let Err(e) = crate::config::save_config_to_db(&conn, &cfg) {
                                        app.status_message = format!("Save error: {}", e);
                                    }
                                } else {
                                    app.settings.editing = !app.settings.editing;
                                }
                                
                                if app.settings.editing {
                                    // Entering edit mode
                                    if app.settings.active_category == SettingsCategory::Theme {
                                         app.settings.original_theme = Some(app.settings.theme.clone());
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
                                    if let Err(e) = crate::config::save_config_to_db(&conn, &cfg) {
                                        app.status_message = format!("Save error: {}", e);
                                    } else {
                                        let field_name = match app.settings.active_category {
                                            SettingsCategory::S3 => match app.settings.selected_field {
                                                0 => "S3 Endpoint", 1 => "S3 Bucket", 2 => "S3 Region", 3 => "Prefix", 4 => "Access Key", 5 => "Secret Key", _ => "Settings"
                                            },
                                            SettingsCategory::Scanner => match app.settings.selected_field {
                                                0 => "ClamAV Host", 1 => "ClamAV Port", 2 => "Chunk Size", 3 => "Enable Scanner", _ => "Scanner"
                                            },
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
                            KeyCode::Right | KeyCode::Down if app.settings.editing && app.settings.active_category == SettingsCategory::Theme => {
                                let current = app.settings.theme.as_str();
                                if let Some(idx) = app.theme_names.iter().position(|&n| n == current) {
                                    let next_idx = (idx + 1) % app.theme_names.len();
                                    app.settings.theme = app.theme_names[next_idx].to_string();
                                    // Live preview
                                    app.theme = Theme::from_name(&app.settings.theme);
                                }
                            }
                            KeyCode::Left | KeyCode::Up if app.settings.editing && app.settings.active_category == SettingsCategory::Theme => {
                                let current = app.settings.theme.as_str();
                                if let Some(idx) = app.theme_names.iter().position(|&n| n == current) {
                                    let prev_idx = (idx + app.theme_names.len() - 1) % app.theme_names.len();
                                    app.settings.theme = app.theme_names[prev_idx].to_string();
                                    // Live preview
                                    app.theme = Theme::from_name(&app.settings.theme);
                                }
                            }
                            // When editing, handle ALL characters including j/k/s
                            KeyCode::Char(c) if app.settings.editing => {
                                match (app.settings.active_category, app.settings.selected_field) {
                                    (SettingsCategory::Theme, _) => {
                                        // Specific handling for j/k in theme selection
                                        if c == 'j' {
                                            let current = app.settings.theme.as_str();
                                            if let Some(idx) = app.theme_names.iter().position(|&n| n == current) {
                                                let next_idx = (idx + 1) % app.theme_names.len();
                                                app.settings.theme = app.theme_names[next_idx].to_string();
                                                app.theme = Theme::from_name(&app.settings.theme);
                                            }
                                        } else if c == 'k' {
                                            let current = app.settings.theme.as_str();
                                            if let Some(idx) = app.theme_names.iter().position(|&n| n == current) {
                                                let prev_idx = (idx + app.theme_names.len() - 1) % app.theme_names.len();
                                                app.settings.theme = app.theme_names[prev_idx].to_string();
                                                app.theme = Theme::from_name(&app.settings.theme);
                                            }
                                        }
                                    }
                                    (SettingsCategory::S3, 0) => app.settings.endpoint.push(c),
                                    (SettingsCategory::S3, 1) => app.settings.bucket.push(c),
                                    (SettingsCategory::S3, 2) => app.settings.region.push(c),
                                    (SettingsCategory::S3, 3) => app.settings.prefix.push(c),
                                    (SettingsCategory::S3, 4) => app.settings.access_key.push(c),
                                    (SettingsCategory::S3, 5) => app.settings.secret_key.push(c),
                                    (SettingsCategory::Scanner, 0) => app.settings.clamd_host.push(c),
                                    (SettingsCategory::Scanner, 1) => app.settings.clamd_port.push(c),
                                    (SettingsCategory::Scanner, 2) => app.settings.scan_chunk_size.push(c),
                                    (SettingsCategory::Performance, 0) => app.settings.part_size.push(c),
                                    (SettingsCategory::Performance, 1) => app.settings.concurrency_global.push(c),
                                    _ => {}
                                }
                            }
                            KeyCode::Backspace if app.settings.editing => {
                                match (app.settings.active_category, app.settings.selected_field) {
                                    (SettingsCategory::S3, 0) => { app.settings.endpoint.pop(); }
                                    (SettingsCategory::S3, 1) => { app.settings.bucket.pop(); }
                                    (SettingsCategory::S3, 2) => { app.settings.region.pop(); }
                                    (SettingsCategory::S3, 3) => { app.settings.prefix.pop(); }
                                    (SettingsCategory::S3, 4) => { app.settings.access_key.pop(); }
                                    (SettingsCategory::S3, 5) => { app.settings.secret_key.pop(); }
                                    (SettingsCategory::Scanner, 0) => { app.settings.clamd_host.pop(); }
                                    (SettingsCategory::Scanner, 1) => { app.settings.clamd_port.pop(); }
                                    (SettingsCategory::Scanner, 2) => { app.settings.scan_chunk_size.pop(); }
                                    (SettingsCategory::Performance, 0) => { app.settings.part_size.pop(); }
                                    (SettingsCategory::Performance, 1) => { app.settings.concurrency_global.pop(); }
                                    _ => {}
                                }
                            }
                            // Navigation only when NOT editing
                            KeyCode::Up | KeyCode::Char('k') if !app.settings.editing => {
                                if app.settings.selected_field > 0 { app.settings.selected_field -= 1; }
                            }
                            KeyCode::Down | KeyCode::Char('j') if !app.settings.editing => {
                                if app.settings.selected_field + 1 < field_count { app.settings.selected_field += 1; }
                            }
                            KeyCode::Char('s') if !app.settings.editing || (app.settings.editing && app.settings.active_category == SettingsCategory::Theme) => {
                                // If saving while editing theme, exit edit mode and commit
                                if app.settings.editing && app.settings.active_category == SettingsCategory::Theme {
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
                                    app.status_message = format!("Configuration saved. Bucket: {}", cfg.s3_bucket.as_deref().unwrap_or("None"));
                                }
                            }
                            KeyCode::Char('w') if !app.settings.editing => {
                                // Launch setup wizard
                                app.wizard = WizardState::new();
                                app.show_wizard = true;
                            }
                            KeyCode::Char('t') if !app.settings.editing => {
                                let cat = app.settings.active_category;
                                if cat == SettingsCategory::S3 || cat == SettingsCategory::Scanner {
                                    app.status_message = "Testing connection...".to_string();
                                    let tx = app.async_tx.clone();
                                    let config_clone = app.config.lock().unwrap().clone();
                                    
                                    std::thread::spawn(move || {
                                        let rt = tokio::runtime::Runtime::new().unwrap();
                                        let res = rt.block_on(async {
                                            if cat == SettingsCategory::S3 {
                                                 crate::uploader::Uploader::check_connection(&config_clone).await
                                            } else {
                                                 crate::scanner::Scanner::new(&config_clone).check_connection().await
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
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
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
            active_category: SettingsCategory::S3,
            selected_field: 0,
            editing: false,
            theme: cfg.theme.clone(),
            original_theme: None,
            scanner_enabled: cfg.scanner_enabled,
            staging_mode_direct: cfg.staging_mode == StagingMode::Direct,
            delete_source_after_upload: cfg.delete_source_after_upload,
        }
    }

    fn apply_to_config(&self, cfg: &mut Config) {
        use crate::config::StagingMode;
        cfg.s3_endpoint = if self.endpoint.trim().is_empty() { None } else { Some(self.endpoint.trim().to_string()) };
        cfg.s3_bucket = if self.bucket.trim().is_empty() { None } else { Some(self.bucket.trim().to_string()) };
        cfg.s3_region = if self.region.trim().is_empty() { None } else { Some(self.region.trim().to_string()) };
        cfg.s3_prefix = if self.prefix.trim().is_empty() { None } else { Some(self.prefix.trim().to_string()) };
        cfg.s3_access_key = if self.access_key.trim().is_empty() { None } else { Some(self.access_key.trim().to_string()) };
        cfg.s3_secret_key = if self.secret_key.trim().is_empty() { None } else { Some(self.secret_key.trim().to_string()) };
        cfg.clamd_host = self.clamd_host.trim().to_string();
        if let Ok(p) = self.clamd_port.trim().parse() { cfg.clamd_port = p; }
        if let Ok(v) = self.scan_chunk_size.trim().parse() { cfg.scan_chunk_size_mb = v; }
        if let Ok(v) = self.part_size.trim().parse() { cfg.part_size_mb = v; }
        if let Ok(v) = self.concurrency_global.trim().parse() { cfg.concurrency_upload_global = v; }
        cfg.theme = self.theme.clone();
        cfg.scanner_enabled = self.scanner_enabled;
        cfg.staging_mode = if self.staging_mode_direct { StagingMode::Direct } else { StagingMode::Copy };
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
    let rows: Vec<Row> = items.iter().enumerate().map(|(id, name)| {
        let style = if app.current_tab as usize == id {
            app.theme.selection_style()
        } else {
            app.theme.text_style()
        };
        Row::new(vec![Cell::from(name.to_string())]).style(style)
    }).collect();

    let table = Table::new(rows, [Constraint::Length(12)])
        .block(
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
            Span::styled(job.status.clone(), app.theme.status_style(status_kind(job.status.as_str()))),
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
                    app.theme.highlight_style().add_modifier(Modifier::UNDERLINED),
                )));
                
                // Show parts progress bar
                let bar_width = 20;
                let filled = (parts_done * bar_width) / parts_total.max(1);
                let bar = format!("[{}{}] {}/{}",
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
        app.theme.highlight_style().add_modifier(Modifier::UNDERLINED),
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
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(50), // Browser (Hopper)
            Constraint::Percentage(50), // Jobs (Queue)
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

    let total_rows = app.history.len();
    let display_height = area.height.saturating_sub(4) as usize; // Border + Header
    let mut offset = 0;
    
    if total_rows > display_height {
        if app.selected_history >= display_height / 2 {
            offset = (app.selected_history + 1).saturating_sub(display_height / 2);
        }
        if offset + display_height > total_rows {
            offset = total_rows.saturating_sub(display_height);
        }
    }

    let rows = app.history.iter().enumerate()
        .skip(offset)
        .take(display_height)
        .map(|(idx, job)| {
        let is_selected = (idx == app.selected_history) && (app.focus == AppFocus::History);
        let row_style = if is_selected {
            app.theme.selection_style()
        } else {
            app.theme.row_style(idx % 2 != 0)
        };
        
        // Extract filename from source_path
        let filename = std::path::Path::new(&job.source_path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| job.source_path.clone());
        let display_name = if filename.len() > 30 {
            format!("{}…", &filename[..29])
        } else {
            filename
        };

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
                "quarantined" | "quarantined_removed" => app.theme.status_style(StatusKind::Error),
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
    });

    let title = format!(" History [{}] ", app.history_filter.as_str());

    let table = Table::new(rows, [
        Constraint::Min(20),
        Constraint::Length(16),
        Constraint::Length(10),
        Constraint::Length(10),
    ])
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
    let footer_text = match app.input_mode {
        InputMode::Filter => {
            "Filter: Type to search fuzzy | ↑/↓: Select | Enter: Confirm | Esc: Back"
        }
        InputMode::Browsing => {
            "Hopper: ←/→ dirs | ↑/↓ select | /: Filter | t: Tree | space: Select | s: Stage | Esc: Exit"
        }
        InputMode::Confirmation => {
            "Confirmation Required"
        }
        InputMode::Normal => match app.focus {
            AppFocus::Rail => "Tab/Right: Content | ↑/↓: Switch Tab | q: Quit",
            AppFocus::Browser => "↑/↓: Select | a/Enter: Browse | Tab: Next Panel",
            AppFocus::Queue => "↑/↓: Select | Enter: Details | c: Clear Done | r: Retry | d: Delete",
            AppFocus::History => "Tab: Rail | Left: Queue",
            AppFocus::Quarantine => "Tab: Rail | Left: Rail | d: Clear | R: Refresh",
            AppFocus::SettingsCategory => match app.settings.active_category {
                 SettingsCategory::S3 | SettingsCategory::Scanner => "Tab/Right: Fields | Left: Rail | ↑/↓: Category | s: Save | t: Test",
                 _ => "Tab/Right: Fields | Left: Rail | ↑/↓: Category | s: Save",
            },
            AppFocus::SettingsFields => match app.settings.active_category {
                 SettingsCategory::S3 | SettingsCategory::Scanner => "Tab: Rail | Left: Category | Enter: Edit | s: Save | t: Test",
                 _ => "Tab: Rail | Left: Category | Enter: Edit | s: Save",
            },
        },
    };
    let status_line = format!("{} | {}", footer_text, app.status_message);
    let footer = Paragraph::new(status_line).style(app.theme.muted_style());
    f.render_widget(footer, area);
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
            Constraint::Min(1), // Message
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

fn draw_system_status(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let av_status = app.clamav_status.lock().unwrap().clone();
    let s3_status = if app.settings.bucket.is_empty() { "Not Configured" } else { "Ready" };
    
    let content = vec![
        Line::from(vec![
            Span::styled(" CLAMAV ", app.theme.button_style(true)),
            Span::from(" "),
            Span::styled(
                av_status,
                if app.clamav_status.lock().unwrap().contains("Ready") {
                    app.theme.status_style(StatusKind::Success)
                } else {
                    app.theme.status_style(StatusKind::Warning)
                },
            ),
            Span::from("  │  "),
            Span::styled(" S3 STORAGE ", app.theme.button_style(true)),
            Span::from(" "),
            Span::styled(
                s3_status,
                if app.settings.bucket.is_empty() {
                    app.theme.status_style(StatusKind::Error)
                } else {
                    app.theme.status_style(StatusKind::Success)
                },
            ),
            Span::from(format!(" (Bucket: {})", if app.settings.bucket.is_empty() { "-" } else { &app.settings.bucket })),
            Span::from("  │  "),
            Span::styled(" QUEUE ", app.theme.button_style(true)),
            Span::from(format!(" {} Jobs", app.jobs.len())),
            Span::from("  │  "),
            Span::styled(" QUARANTINE ", app.theme.status_badge_style(StatusKind::Error)),
            Span::from(format!(" {} Threats", app.quarantine.len())),
        ]),
    ];

    let p = Paragraph::new(content)
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_type(app.theme.border_type)
                .border_style(app.theme.border_style())
                .style(app.theme.panel_style()),
        );
    f.render_widget(p, area);
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
            Constraint::Length(2), // System Status
            Constraint::Length(1), // Footer
        ])
        .split(f.size());

    let main_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(14), // Rail
            Constraint::Min(0),     // Content Area (Hopper + Queue)
            Constraint::Length(60), // History / Details
        ])
        .split(root[0]);

    draw_rail(f, app, main_layout[0]);

    match app.current_tab {
        AppTab::Transfers => draw_transfers(f, app, main_layout[1]),
        AppTab::Quarantine => draw_quarantine(f, app, main_layout[1]),
        AppTab::Settings => draw_settings(f, app, main_layout[1]),
    }

    // Render History / Details panel (Right side)
    let right_panel = main_layout[2];
    
    // Logic: 
    // 1. If Queue focused & selected -> Replace generic History list with Job Details (retaining existing behavior for Queue focus).
    // 2. If History focused -> Split panel: Top=List, Bottom=Details.
    // 3. Otherwise -> Show full History list.

    if app.current_tab == AppTab::Quarantine && !app.quarantine.is_empty() && app.selected_quarantine < app.quarantine.len() {
        // Quarantine Tab: Show selected threat details in full right panel
        draw_job_details(f, app, right_panel, &app.quarantine[app.selected_quarantine]);
    } else if app.focus == AppFocus::Queue && !app.jobs.is_empty() && app.selected < app.jobs.len() {
        // Queue focused: Show selected job details in full right panel
        draw_job_details(f, app, right_panel, &app.jobs[app.selected]);
    } else if app.focus == AppFocus::History && !app.history.is_empty() && app.selected_history < app.history.len() {
        // History focused: Split view
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(50),
                Constraint::Percentage(50),
            ])
            .split(right_panel);
        
        draw_history(f, app, chunks[0]);
        draw_job_details(f, app, chunks[1], &app.history[app.selected_history]);
    } else {
        // Default: Full history list
        draw_history(f, app, right_panel);
    }
    draw_system_status(f, app, root[1]);
    draw_footer(f, app, root[2]);
    
    // Render Modal last (on top)
    draw_confirmation_modal(f, app, f.size());
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
        let field_items: Vec<Line> = fields.iter().enumerate().flat_map(|(i, (label, value))| {
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
        }).collect();
        
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

    let total_rows = app.jobs.len();
    let display_height = area.height.saturating_sub(4) as usize; // Border + Header
    let mut offset = 0;
    
    if total_rows > display_height {
        if app.selected >= display_height / 2 {
            offset = (app.selected + 1).saturating_sub(display_height / 2);
        }
        if offset + display_height > total_rows {
            offset = total_rows.saturating_sub(display_height);
        }
    }

    let rows = app.jobs.iter().enumerate()
        .skip(offset)
        .take(display_height)
        .map(|(idx, job)| {
        let is_selected = (idx == app.selected) && (app.focus == AppFocus::Queue);
        let row_style = if is_selected {
            app.theme.selection_style()
        } else {
            app.theme.row_style(idx % 2 != 0)
        };
        
        // Extract filename from source_path
        let filename = std::path::Path::new(&job.source_path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| job.source_path.clone());
        let display_name = if filename.len() > 25 {
            format!("{}…", &filename[..24])
        } else {
            filename
        };

        let p_str = {
            if job.status == "uploading" {
                let p_map = app.progress.lock().unwrap();
                let entry = p_map.get(&job.id).cloned();
                drop(p_map);
                if let Some(info) = entry {
                    let bar_width = 10;
                    let filled = (info.percent.clamp(0.0, 100.0) / 100.0 * (bar_width as f64)) as usize;
                    format!("{}{} {:.0}%", "█".repeat(filled), "░".repeat(bar_width - filled), info.percent)
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

        // Relative time from created_at
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
    });

    let table = Table::new(rows, [
        Constraint::Min(20),
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Length(16),
        Constraint::Length(10),
    ])
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
    if size == 0 { return "-".to_string(); }
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
    if pattern.is_empty() { return true; }
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
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let title = match app.input_mode {
        InputMode::Browsing => " Hopper (Browsing) ",
        InputMode::Filter => " Hopper (Filter) ",
        InputMode::Normal if is_focused => " Hopper (Press 'a' to browse) ",
        InputMode::Normal => " Hopper ",
        InputMode::Confirmation => " Hopper ",
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .title(title)
        .border_style(focus_style)
        .style(app.theme.panel_style());
    
    let inner_area = block.inner(area);
    f.render_widget(block, area);

    if inner_area.height < 3 { return; }

    // Breadcrumbs
    let path_str = app.picker.cwd.to_string_lossy().to_string();
    let breadcrumbs = Line::from(vec![
        Span::styled(" 📂 ", app.theme.accent_style()),
        Span::styled(path_str, app.theme.text_style().add_modifier(Modifier::BOLD)),
    ]);
    
    let breadcrumb_area = Rect::new(inner_area.x, inner_area.y, inner_area.width, 1);
    f.render_widget(Paragraph::new(breadcrumbs), breadcrumb_area);

    // Filter indicator
    if !app.input_buffer.is_empty() || app.input_mode == InputMode::Filter {
        let filter_line = Line::from(vec![
            Span::styled(" 🔍 ", app.theme.muted_style()),
            Span::styled(if app.picker.search_recursive { "Recursive Filter: " } else { "Filter: " }, app.theme.muted_style()),
            Span::styled(&app.input_buffer, app.theme.accent_style()),
            Span::styled(if app.input_mode == InputMode::Filter { " █" } else { "" }, app.theme.accent_style()),
        ]);
        let filter_area = Rect::new(inner_area.x, inner_area.y + 1, inner_area.width, 1);
        f.render_widget(Paragraph::new(filter_line), filter_area);
    }

    let table_area = Rect::new(
        inner_area.x, 
        inner_area.y + if app.input_buffer.is_empty() { 1 } else { 2 }, 
        inner_area.width, 
        inner_area.height.saturating_sub(if app.input_buffer.is_empty() { 1 } else { 2 })
    );

    // Live Filtering
    let filter = if app.input_mode == InputMode::Filter { 
        app.input_buffer.clone() 
    } else { 
        app.input_buffer.clone() // Still use it if not empty but in Normal mode
    };

    let filtered_entries: Vec<(usize, &FileEntry)> = app.picker.entries.iter().enumerate()
        .filter(|(_, e)| e.is_parent || filter.is_empty() || fuzzy_match(&filter, &e.name))
        .collect();

    let total_rows = filtered_entries.len();
    let display_height = table_area.height.saturating_sub(1) as usize; // -1 for header
    
    // Find current index in filtered list
    let filtered_selected = filtered_entries.iter().position(|(i, _)| *i == app.picker.selected).unwrap_or(0);

    let mut offset = 0;
    if total_rows > display_height {
        if filtered_selected >= display_height / 2 {
            offset = (filtered_selected + 1).saturating_sub(display_height / 2);
        }
        if offset + display_height > total_rows {
            offset = total_rows.saturating_sub(display_height);
        }
    }

    let header = Row::new(vec![
        Cell::from("Name"),
        Cell::from("Size"),
        Cell::from("Modified"),
    ]).style(app.theme.header_style()).height(1);

    let rows: Vec<Row> = filtered_entries
        .iter()
        .skip(offset)
        .take(display_height)
        .enumerate()
        .map(|(visible_idx, (orig_idx, entry))| {
            let prefix = if entry.is_dir { "📁 " } else { "📄 " };
            let expand = if entry.is_dir && !entry.is_parent {
                 if app.picker.expanded.contains(&entry.path) { "[-] " } else { "[+] " }
            } else { "" };
            
            let name_styled = format!("{}{}{}", "  ".repeat(entry.depth), expand, prefix) + &entry.name;
            let size_str = if entry.is_dir { "-".to_string() } else { format_size(entry.size) };
            let mod_str = format_modified(entry.modified);

            let style = if (*orig_idx == app.picker.selected) && (app.focus == AppFocus::Browser) {
                app.theme.selection_style()
            } else if app.picker.selected_paths.contains(&entry.path) {
                app.theme.status_style(StatusKind::Success)
            } else if (visible_idx + offset) % 2 == 0 {
                app.theme.row_style(false)
            } else {
                 app.theme.row_style(true)
            };

            Row::new(vec![
                Cell::from(name_styled),
                Cell::from(size_str),
                Cell::from(mod_str),
            ]).style(style)
        })
        .collect();

    let table = Table::new(rows, [
        Constraint::Fill(1),
        Constraint::Length(10),
        Constraint::Length(16),
    ])
    .header(header)
    .style(app.theme.panel_style());

    f.render_widget(table, table_area);
}

fn draw_settings(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let has_focus = app.focus == AppFocus::SettingsCategory || app.focus == AppFocus::SettingsFields;
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
        .constraints([
            Constraint::Length(16),
            Constraint::Min(0),
        ])
        .split(inner_area);

    // Sidebar Categories
    let categories = vec!["S3 Storage", "Scanner", "Performance", "Theme"];
    let cat_items: Vec<ListItem> = categories.iter().enumerate().map(|(id, name)| {
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
    }).collect();

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
            ("S3 Secret Key", if app.settings.editing && app.settings.selected_field == 5 { app.settings.secret_key.as_str() } else { "*******" }),
        ],
        SettingsCategory::Scanner => vec![
            ("ClamAV Host", app.settings.clamd_host.as_str()),
            ("ClamAV Port", app.settings.clamd_port.as_str()),
            ("Scan Chunk Size (MB)", app.settings.scan_chunk_size.as_str()),
            ("Enable Scanner", if app.settings.scanner_enabled { "[X] Enabled" } else { "[ ] Disabled" }),
        ],
        SettingsCategory::Performance => vec![
            ("Part Size (MB)", app.settings.part_size.as_str()),
            ("Global Concurrency", app.settings.concurrency_global.as_str()),
            ("Staging Mode", if app.settings.staging_mode_direct { "[X] Direct (no copy)" } else { "[ ] Copy (default)" }),
            ("Delete Source After Upload", if app.settings.delete_source_after_upload { "[X] Enabled" } else { "[ ] Disabled" }),
        ],
        SettingsCategory::Theme => vec![
            ("Theme", app.settings.theme.as_str()),
        ],
    };

    if fields_per_view == 0 { return; }

    let field_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![Constraint::Length(3); fields_per_view])
        .split(fields_area);

    let mut offset = 0;
    if app.settings.selected_field >= fields_per_view {
        offset = app.settings.selected_field.saturating_sub(fields_per_view / 2);
    }
    
    // Safety check for offset
    if fields.len() > fields_per_view && offset + fields_per_view > fields.len() {
        offset = fields.len() - fields_per_view;
    }

    for (i, (title, value)) in fields.iter().enumerate().skip(offset).take(fields_per_view) {
        let chunk_idx = i - offset;
        if chunk_idx >= field_chunks.len() { break; }

        let is_selected = (app.settings.selected_field == i) && (app.focus == AppFocus::SettingsFields);
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

        let value_style = if is_selected && app.settings.editing && app.settings.active_category != SettingsCategory::Theme {
            app.theme.input_style(true)
        } else if is_selected {
            app.theme.text_style().add_modifier(Modifier::BOLD)
        } else {
            app.theme.text_style()
        };

        let p = Paragraph::new(*value).block(block).style(value_style);
        f.render_widget(p, field_chunks[chunk_idx]);

        // Draw cursor if editing non-theme fields
        if is_selected && app.settings.editing && app.settings.active_category != SettingsCategory::Theme {
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
    let mut offset = 0;
    
    if total_rows > display_height {
        if app.selected_quarantine >= display_height / 2 {
            offset = (app.selected_quarantine + 1).saturating_sub(display_height / 2);
        }
        if offset + display_height > total_rows {
            offset = total_rows.saturating_sub(display_height);
        }
    }

    let rows = app.quarantine.iter().enumerate()
        .skip(offset)
        .take(display_height)
        .map(|(idx, job)| {
        let is_selected = (idx == app.selected_quarantine) && (app.focus == AppFocus::Quarantine);
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
        let status = if job.status == "quarantined_removed" { "Removed" } else { "Detected" };
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
            Cell::from(threat).style(if is_selected { Style::default() } else { app.theme.status_style(StatusKind::Error) }),
            Cell::from(status).style(status_style),
            Cell::from(time_str),
        ])
        .style(row_style)
    });

    let table = Table::new(rows, [
        Constraint::Min(20),
        Constraint::Length(25),
        Constraint::Length(12),
        Constraint::Length(10),
    ])
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
