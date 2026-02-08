// UI Snapshot Tests for drifter TUI
//
// These tests render the TUI to an in-memory buffer using Ratatui's TestBackend
// and compare against saved snapshots using insta.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use ratatui::{Terminal, backend::TestBackend};
use rusqlite::Connection;
use tokio::sync::Mutex as AsyncMutex;

use drifter::app::settings::SettingsState;
use drifter::app::state::{
    App, AppFocus, AppTab, HistoryFilter, InputMode, LayoutTarget, ModalAction, RemoteTarget,
    ViewMode, VisualItem,
};
use drifter::components::file_picker::{FileEntry, FilePicker, PickerView};
use drifter::components::wizard::WizardState;
use drifter::coordinator::ProgressInfo;
use drifter::core::config::Config;
use drifter::core::metrics::{HostMetricsSnapshot, MetricsCollector};
use drifter::core::transfer::TransferDirection;
use drifter::db::JobRow;
use drifter::services::uploader::S3Object;
use drifter::services::watch::Watcher;
use drifter::ui::render::ui;
use drifter::ui::theme::Theme;

/// Create an in-memory test database with minimal schema
fn setup_test_db() -> Connection {
    let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
    conn.execute_batch(
        "
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL DEFAULT 'test',
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            source_path TEXT NOT NULL,
            staged_path TEXT,
            size_bytes INTEGER NOT NULL,
            scan_status TEXT,
            upload_status TEXT,
            s3_bucket TEXT,
            s3_key TEXT,
            s3_upload_id TEXT,
            checksum TEXT,
            remote_checksum TEXT,
            error TEXT,
            priority INTEGER DEFAULT 0,
            retry_count INTEGER DEFAULT 0,
            next_retry_at TEXT,
            scan_duration_ms INTEGER,
            upload_duration_ms INTEGER
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE secrets (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        ",
    )
    .expect("Failed to create test tables");
    conn
}

/// Builder for creating App instances in test isolation
struct TestAppBuilder {
    conn: Arc<Mutex<Connection>>,
    config: Config,
    current_tab: AppTab,
    focus: AppFocus,
    input_mode: InputMode,
    jobs: Vec<JobRow>,
    history: Vec<JobRow>,
    quarantine: Vec<JobRow>,
    picker_entries: Vec<FileEntry>,
    staged_paths: HashSet<PathBuf>,
    s3_objects: Vec<S3Object>,
    input_buffer: String,
    view_mode: ViewMode,
    theme_name: String,
    show_wizard: bool,
    pending_action: ModalAction,
    confirmation_msg: String,
    layout_adjust_target: Option<LayoutTarget>,
}

#[allow(dead_code)]
impl TestAppBuilder {
    fn new() -> Self {
        let conn = Arc::new(Mutex::new(setup_test_db()));
        Self {
            conn,
            config: Config::default(),
            current_tab: AppTab::Transfers,
            focus: AppFocus::Browser,
            input_mode: InputMode::Normal,
            jobs: Vec::new(),
            history: Vec::new(),
            quarantine: Vec::new(),
            picker_entries: Vec::new(),
            staged_paths: HashSet::new(),
            s3_objects: Vec::new(),
            input_buffer: String::new(),
            view_mode: ViewMode::Flat,
            theme_name: "dracula".to_string(),
            show_wizard: false,
            pending_action: ModalAction::None,
            confirmation_msg: String::new(),
            layout_adjust_target: None,
        }
    }

    fn with_tab(mut self, tab: AppTab) -> Self {
        self.current_tab = tab;
        self
    }

    fn with_focus(mut self, focus: AppFocus) -> Self {
        self.focus = focus;
        self
    }

    fn with_input_mode(mut self, mode: InputMode) -> Self {
        self.input_mode = mode;
        self
    }

    fn with_jobs(mut self, jobs: Vec<JobRow>) -> Self {
        self.jobs = jobs;
        self
    }

    fn with_history(mut self, history: Vec<JobRow>) -> Self {
        self.history = history;
        self
    }

    fn with_quarantine(mut self, quarantine: Vec<JobRow>) -> Self {
        self.quarantine = quarantine;
        self
    }

    fn with_picker_entries(mut self, entries: Vec<FileEntry>) -> Self {
        self.picker_entries = entries;
        self
    }

    fn with_staged_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.staged_paths = paths.into_iter().collect();
        self
    }

    fn with_s3_objects(mut self, objects: Vec<S3Object>) -> Self {
        self.s3_objects = objects;
        self
    }

    fn with_filter(mut self, filter_text: &str) -> Self {
        self.input_buffer = filter_text.to_string();
        self.input_mode = InputMode::Filter;
        self
    }

    fn with_view_mode(mut self, mode: ViewMode) -> Self {
        self.view_mode = mode;
        self
    }

    fn with_theme(mut self, theme_name: &str) -> Self {
        self.theme_name = theme_name.to_string();
        self
    }

    fn with_wizard(mut self) -> Self {
        self.show_wizard = true;
        self
    }

    fn with_confirmation(mut self, action: ModalAction, msg: &str) -> Self {
        self.pending_action = action;
        self.confirmation_msg = msg.to_string();
        self.input_mode = InputMode::Confirmation;
        self
    }

    fn with_layout_adjust(mut self, target: LayoutTarget) -> Self {
        self.layout_adjust_target = Some(target);
        self.input_mode = InputMode::LayoutAdjust;
        self
    }

    fn build(self) -> App {
        let config = Arc::new(AsyncMutex::new(self.config.clone()));
        let progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>> =
            Arc::new(AsyncMutex::new(HashMap::new()));
        let cancellation_tokens: Arc<AsyncMutex<HashMap<i64, Arc<AtomicBool>>>> =
            Arc::new(AsyncMutex::new(HashMap::new()));

        let (tx, rx) = mpsc::channel();

        // Create a minimal FilePicker without filesystem access
        let mut picker = FilePicker {
            cwd: PathBuf::from("/test/path"),
            entries: self.picker_entries,
            selected: 0,
            view: PickerView::List,
            expanded: HashSet::new(),
            selected_paths: self.staged_paths.clone(),
            last_error: None,
            search_recursive: false,
            is_searching: false,
        };

        // Add default entries if none provided
        if picker.entries.is_empty() {
            picker.entries = vec![
                FileEntry {
                    name: "..".to_string(),
                    path: PathBuf::from("/test"),
                    is_dir: true,
                    is_parent: true,
                    depth: 0,
                    size: 0,
                    modified: None,
                },
                FileEntry {
                    name: "documents".to_string(),
                    path: PathBuf::from("/test/path/documents"),
                    is_dir: true,
                    is_parent: false,
                    depth: 0,
                    size: 0,
                    modified: None,
                },
                FileEntry {
                    name: "file1.txt".to_string(),
                    path: PathBuf::from("/test/path/file1.txt"),
                    is_dir: false,
                    is_parent: false,
                    depth: 0,
                    size: 1024,
                    modified: None,
                },
                FileEntry {
                    name: "file2.pdf".to_string(),
                    path: PathBuf::from("/test/path/file2.pdf"),
                    is_dir: false,
                    is_parent: false,
                    depth: 0,
                    size: 2048000,
                    modified: None,
                },
            ];
        }

        let theme = Theme::from_name(&self.theme_name);
        let settings = SettingsState::from_config(&self.config);

        // Build visual lists from jobs
        let visual_jobs: Vec<VisualItem> = self
            .jobs
            .iter()
            .enumerate()
            .map(|(i, job)| {
                let name = std::path::Path::new(&job.source_path)
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
            .collect();

        let visual_history: Vec<VisualItem> = self
            .history
            .iter()
            .enumerate()
            .map(|(i, job)| {
                let name = std::path::Path::new(&job.source_path)
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
            .collect();

        App {
            jobs: self.jobs,
            history: self.history,
            job_transfer_metadata: HashMap::new(),
            quarantine: self.quarantine,
            selected: 0,
            selected_history: 0,
            selected_quarantine: 0,

            s3_objects: self.s3_objects,
            selected_remote: 0,
            remote_current_path: "uploads/".to_string(),
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

            last_refresh: Instant::now(),
            status_message: "Ready".to_string(),
            status_message_at: None,
            input_mode: self.input_mode,
            input_buffer: self.input_buffer,
            history_filter: HistoryFilter::All,
            picker,
            watch_enabled: false,
            _watch_path: None,
            _watch_seen: HashSet::new(),
            last_watch_scan: Instant::now(),
            _watcher: Watcher::new(self.conn.clone()),
            current_tab: self.current_tab,
            focus: self.focus,
            settings,
            config,
            cached_config: self.config.clone(),
            clamav_status: Arc::new(Mutex::new("Connected".to_string())),
            progress,
            cached_progress: HashMap::new(),
            cancellation_tokens,
            conn: self.conn,

            logs: VecDeque::new(),
            logs_scroll: 0,
            logs_scroll_x: 0,
            logs_stick_to_bottom: true,
            log_search_active: false,
            log_search_query: String::new(),
            log_search_results: Vec::new(),
            log_search_current: 0,
            log_handle: None,
            current_log_level: "info".to_string(),

            queue_search_query: String::new(),
            history_search_query: String::new(),

            view_mode: self.view_mode,
            visual_jobs,
            visual_history,

            async_rx: rx,
            async_tx: tx,
            show_wizard: self.show_wizard,
            wizard: WizardState::new(),
            wizard_from_settings: false,
            theme,
            theme_names: Theme::list_names(),
            pending_action: self.pending_action,
            confirmation_msg: self.confirmation_msg,
            confirmation_return_mode: None,
            creating_folder_name: String::new(),

            metrics: MetricsCollector::new(),
            last_metrics: HostMetricsSnapshot::default(),
            last_metrics_refresh: Instant::now(),

            last_click_time: None,
            last_click_pos: None,
            hover_pos: None,

            layout_adjust_target: self.layout_adjust_target,
            layout_adjust_message: String::new(),
        }
    }
}

/// Helper to create sample job rows for testing.
/// Uses a fixed timestamp so snapshots remain stable across runs.
fn create_sample_job(id: i64, name: &str, status: &str, size: i64) -> JobRow {
    const FIXED_CREATED_AT: &str = "2026-01-15T10:30:00+00:00";
    JobRow {
        id,
        session_id: "test-session".to_string(),
        created_at: FIXED_CREATED_AT.to_string(),
        status: status.to_string(),
        source_path: format!("/uploads/{}", name),
        size_bytes: size,
        staged_path: Some(format!("/uploads/{}", name)),
        error: None,
        scan_status: Some("clean".to_string()),
        upload_status: None,
        s3_upload_id: None,
        s3_key: Some(format!("uploads/{}", name)),
        priority: 0,
        checksum: None,
        remote_checksum: None,
        retry_count: 0,
        next_retry_at: None,
        scan_duration_ms: Some(500),
        upload_duration_ms: None,
    }
}

/// Render app to buffer and return as string for snapshot comparison
fn render_to_string(app: &App, width: u16, height: u16) -> String {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).expect("Failed to create terminal");

    terminal
        .draw(|f| ui(f, app))
        .expect("Failed to draw to terminal");

    // Convert buffer to string representation
    let buffer = terminal.backend().buffer();
    let mut output = String::new();

    for y in 0..buffer.area.height {
        for x in 0..buffer.area.width {
            let cell = buffer.get(x, y);
            output.push_str(cell.symbol());
        }
        output.push('\n');
    }

    output
}

// =============================================================================
// SNAPSHOT TESTS
// =============================================================================

#[test]
fn test_transfers_view_empty() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Browser)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_transfers_view_with_jobs() {
    let jobs = vec![
        create_sample_job(1, "document.pdf", "uploading", 5_000_000),
        create_sample_job(2, "image.png", "scanning", 2_500_000),
        create_sample_job(3, "data.csv", "queued", 150_000),
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Queue)
        .with_jobs(jobs)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_browser_focused() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Browser)
        .with_input_mode(InputMode::Browsing)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_settings_view() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Settings)
        .with_focus(AppFocus::SettingsCategory)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_quarantine_view_empty() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Quarantine)
        .with_focus(AppFocus::Quarantine)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_quarantine_view_with_threats() {
    let mut threat = create_sample_job(1, "malware.exe", "quarantined", 500_000);
    threat.error = Some("Infected: Win.Trojan.Agent-123456".to_string());
    threat.scan_status = Some("infected".to_string());

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Quarantine)
        .with_focus(AppFocus::Quarantine)
        .with_quarantine(vec![threat])
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_history_with_items() {
    let history = vec![
        create_sample_job(1, "completed_file.zip", "complete", 10_000_000),
        create_sample_job(2, "another_upload.tar.gz", "complete", 25_000_000),
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::History)
        .with_history(history)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_confirmation_modal() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_confirmation(ModalAction::ClearHistory, "Clear all job history?")
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_quit_confirmation_modal() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_confirmation(ModalAction::QuitApp, "Quit Drifter?")
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_layout_adjustment_overlay() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_layout_adjust(LayoutTarget::Local)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_wizard_view() {
    let app = TestAppBuilder::new().with_wizard().build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_logs_view() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Logs)
        .with_focus(AppFocus::Logs)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

// =============================================================================
// ADDITIONAL TESTS - Filter, Remote, Staged, Errors, Tree View, Themes
// =============================================================================

#[test]
fn test_filter_mode_active() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Browser)
        .with_filter("doc")
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_remote_panel_with_objects() {
    let s3_objects = vec![
        S3Object {
            key: "uploads/reports/".to_string(),
            name: "reports/".to_string(),
            size: 0,
            last_modified: String::new(),
            is_dir: true,
            is_parent: false,
        },
        S3Object {
            key: "uploads/backup.zip".to_string(),
            name: "backup.zip".to_string(),
            size: 52_428_800,
            last_modified: "2025-01-20T14:30:00Z".to_string(),
            is_dir: false,
            is_parent: false,
        },
        S3Object {
            key: "uploads/data.csv".to_string(),
            name: "data.csv".to_string(),
            size: 1_048_576,
            last_modified: "2025-01-19T09:15:00Z".to_string(),
            is_dir: false,
            is_parent: false,
        },
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Remote)
        .with_s3_objects(s3_objects)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_remote_browsing_hints() {
    let s3_objects = vec![
        S3Object {
            key: "uploads/reports/".to_string(),
            name: "reports/".to_string(),
            size: 0,
            last_modified: String::new(),
            is_dir: true,
            is_parent: false,
        },
        S3Object {
            key: "uploads/data.csv".to_string(),
            name: "data.csv".to_string(),
            size: 1_048_576,
            last_modified: "2025-01-19T09:15:00Z".to_string(),
            is_dir: false,
            is_parent: false,
        },
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Remote)
        .with_input_mode(InputMode::RemoteBrowsing)
        .with_s3_objects(s3_objects)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_remote_folder_create_modal() {
    let mut app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Remote)
        .with_input_mode(InputMode::RemoteFolderCreate)
        .build();
    app.creating_folder_name = "reports_2025".to_string();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_remote_delete_folder_confirmation() {
    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_confirmation(
            ModalAction::DeleteRemoteObject(
                "uploads/archive/".to_string(),
                "uploads/".to_string(),
                true,
                RemoteTarget::Primary,
            ),
            "Delete folder 'archive' and all contents?",
        )
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_staged_files_selected() {
    let staged = vec![
        PathBuf::from("/test/path/file1.txt"),
        PathBuf::from("/test/path/file2.pdf"),
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Browser)
        .with_input_mode(InputMode::Browsing)
        .with_staged_paths(staged)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_job_with_error() {
    let mut failed_job = create_sample_job(1, "broken_upload.zip", "failed", 15_000_000);
    failed_job.error = Some("Network timeout after 3 retries".to_string());
    failed_job.retry_count = 3;

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Queue)
        .with_jobs(vec![failed_job])
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_tree_view_mode() {
    let jobs = vec![
        create_sample_job(1, "reports/q1/sales.csv", "uploading", 500_000),
        create_sample_job(2, "reports/q1/expenses.csv", "queued", 300_000),
        create_sample_job(3, "reports/q2/summary.pdf", "scanning", 1_200_000),
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Queue)
        .with_jobs(jobs)
        .with_view_mode(ViewMode::Tree)
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_nord_theme() {
    let jobs = vec![
        create_sample_job(1, "document.pdf", "uploading", 5_000_000),
        create_sample_job(2, "image.png", "complete", 2_500_000),
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Queue)
        .with_jobs(jobs)
        .with_theme("nord")
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_rose_pine_moon_theme() {
    let jobs = vec![
        create_sample_job(1, "summary.pdf", "uploading", 5_000_000),
        create_sample_job(2, "notes.txt", "complete", 12_000),
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Queue)
        .with_jobs(jobs)
        .with_theme("rose pine moon")
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_catppuccin_latte_theme() {
    let jobs = vec![
        create_sample_job(1, "report.csv", "uploading", 1_200_000),
        create_sample_job(2, "photo.jpg", "complete", 4_800_000),
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Queue)
        .with_jobs(jobs)
        .with_theme("catppuccin latte")
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_catppuccin_frappe_theme() {
    let jobs = vec![
        create_sample_job(1, "archive.tar.gz", "uploading", 9_800_000),
        create_sample_job(2, "readme.md", "complete", 24_000),
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Queue)
        .with_jobs(jobs)
        .with_theme("catppuccin frappe")
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}

#[test]
fn test_catppuccin_macchiato_theme() {
    let jobs = vec![
        create_sample_job(1, "design.sketch", "uploading", 2_400_000),
        create_sample_job(2, "spec.docx", "complete", 600_000),
    ];

    let app = TestAppBuilder::new()
        .with_tab(AppTab::Transfers)
        .with_focus(AppFocus::Queue)
        .with_jobs(jobs)
        .with_theme("catppuccin macchiato")
        .build();

    let output = render_to_string(&app, 120, 40);
    insta::assert_snapshot!(output);
}
