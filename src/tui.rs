use crate::core::config::Config;
mod helpers;

use crate::services::ingest::ingest_path;
use crate::services::uploader::S3Object;

use crate::coordinator::ProgressInfo;
use crate::ui::theme::Theme;
use uuid::Uuid;

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
use ratatui::widgets::{Block, Borders};

use ratatui::Terminal;
use rusqlite::Connection;
use std::collections::HashMap;
use std::io::{self};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::app::settings::{SettingsCategory, SettingsState};
use crate::app::state::{
    App, AppEvent, AppFocus, AppTab, HistoryFilter, InputMode, LayoutTarget, ModalAction, ViewMode,
};
use crate::components::file_picker::PickerView;
use crate::components::wizard::{WizardState, WizardStep};
use crate::ui::util::{calculate_list_offset, fuzzy_match};
use crate::utils::lock_mutex;
use self::helpers::{
    adjust_layout_dimension, prepare_remote_delete, request_remote_list,
    reset_all_layout_dimensions, reset_layout_dimension, s3_ready, selected_remote_object,
    start_remote_download, update_layout_message,
};

use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;

pub struct TuiArgs {
    pub conn_mutex: Arc<Mutex<Connection>>,
    pub cfg: Arc<AsyncMutex<Config>>,
    pub progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>>,
    pub cancellation_tokens: Arc<AsyncMutex<HashMap<i64, Arc<AtomicBool>>>>,
    pub needs_wizard: bool,
    pub log_handle: crate::logging::LogHandle,
    pub app_tx: std::sync::mpsc::Sender<AppEvent>,
    pub app_rx: std::sync::mpsc::Receiver<AppEvent>,
}

pub async fn run_tui(args: TuiArgs) -> Result<()> {
    let TuiArgs {
        conn_mutex,
        cfg,
        progress,
        cancellation_tokens,
        needs_wizard,
        log_handle,
        app_tx,
        app_rx,
    } = args;

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut app = App::new(
        conn_mutex.clone(),
        cfg.clone(),
        progress.clone(),
        cancellation_tokens.clone(),
        log_handle.clone(),
        app_tx,
        app_rx,
    )
    .await?;
    app.show_wizard = needs_wizard;

    {
        let conn = lock_mutex(&conn_mutex)?;
        app.refresh_jobs(&conn)?;
    }

    let should_prefetch_remote = {
        let cfg = app.config.lock().await;
        s3_ready(&cfg)
    };
    if should_prefetch_remote {
        request_remote_list(&mut app, false).await;
    }

    // Start Log Watcher
    let log_tx = app.async_tx.clone();
    let log_path = PathBuf::from("debug.log");
    crate::services::log_watcher::LogWatcher::new(log_path, log_tx).start();

    let tick_rate = Duration::from_millis(50);
    loop {
        // Update valid cached state for rendering (Pre-render sync)
        {
            let cfg = app.config.lock().await;
            app.cached_config = cfg.clone();
        }
        {
            let prog = app.progress.lock().await;
            app.cached_progress = prog.clone();
        }

        // Refresh metrics periodically (e.g. every 1s) if enabled
        if app.cached_config.host_metrics_enabled
            && app.last_metrics_refresh.elapsed() >= Duration::from_secs(1)
        {
            app.last_metrics = app.metrics.refresh();
            app.last_metrics_refresh = Instant::now();
        }

        // Status Message Timeout (10s)
        if let Some(at) = app.status_message_at
            && at.elapsed() >= Duration::from_secs(10)
        {
            app.status_message = "Ready".to_string();
            app.status_message_at = None;
        }

        terminal.draw(|f| crate::ui::ui(f, &app))?;

        if app.last_refresh.elapsed() > Duration::from_secs(1) {
            let conn = lock_mutex(&conn_mutex)?;
            app.refresh_jobs(&conn)?;
        }

        // Check for async messages (e.g. connection tests, S3 lists)
        while let Ok(event) = app.async_rx.try_recv() {
            match event {
                AppEvent::Notification(msg) => {
                    app.set_status(msg);
                }
                AppEvent::RemoteFileList(path, files) => {
                    // Clear loading state if this was the pending request
                    if app.remote_request_pending.as_ref() == Some(&path) {
                        app.remote_loading = false;
                        app.remote_request_pending = None;
                    }

                    // Store in cache (without parent entry)
                    app.remote_cache
                        .insert(path.clone(), (files.clone(), Instant::now()));

                    // Only update display if still viewing this path
                    if app.remote_current_path == path {
                        let mut display_files = files;
                        // Prepend ".." entry if not at root
                        if !path.is_empty() {
                            display_files.insert(
                                0,
                                S3Object {
                                    key: "..".to_string(),
                                    name: "..".to_string(),
                                    size: 0,
                                    last_modified: String::new(),
                                    is_dir: true,
                                    is_parent: true,
                                },
                            );
                        }
                        app.s3_objects = display_files;
                        app.selected_remote = 0;
                        let msg = format!("Loaded {} items from S3", app.s3_objects.len());
                        app.set_status(msg);
                    }
                }
                AppEvent::LogLine(line) => {
                    app.logs.push_back(line);
                    if app.logs.len() > 1000 {
                        app.logs.pop_front();
                    }
                }
                AppEvent::RefreshRemote => {
                    // Refresh remote panel after upload completion (force refresh to show new files)
                    let should_refresh = {
                        let cfg = app.config.lock().await;
                        s3_ready(&cfg)
                    };
                    if should_refresh {
                        request_remote_list(&mut app, true).await;
                    }
                }
            }
        }

        if app.watch_enabled && app.last_watch_scan.elapsed() > Duration::from_secs(2) {
            let _conn = lock_mutex(&conn_mutex)?;
            app.last_watch_scan = Instant::now();
        }

        if event::poll(tick_rate)? {
            let mut events = vec![event::read()?];
            while event::poll(Duration::from_secs(0))? {
                events.push(event::read()?);
            }
            let mut should_quit = false;
            for event in events {
                match event {
                    Event::Key(key) => {
                        // Handle Layout Adjustment Mode
                        if app.input_mode == InputMode::LayoutAdjust {
                            match key.code {
                                KeyCode::Char('1') => {
                                    app.layout_adjust_target = Some(LayoutTarget::Local);
                                    update_layout_message(&mut app, LayoutTarget::Local).await;
                                }
                                KeyCode::Char('2') => {
                                    app.layout_adjust_target = Some(LayoutTarget::Queue);
                                    update_layout_message(&mut app, LayoutTarget::Queue).await;
                                }
                                KeyCode::Char('3') => {
                                    app.layout_adjust_target = Some(LayoutTarget::History);
                                    update_layout_message(&mut app, LayoutTarget::History).await;
                                }
                                KeyCode::Char('+') | KeyCode::Char('=') => {
                                    if let Some(target) = app.layout_adjust_target {
                                        adjust_layout_dimension(&app.config, target, 1).await;
                                        update_layout_message(&mut app, target).await;
                                    }
                                }
                                KeyCode::Char('-') | KeyCode::Char('_') => {
                                    if let Some(target) = app.layout_adjust_target {
                                        adjust_layout_dimension(&app.config, target, -1).await;
                                        update_layout_message(&mut app, target).await;
                                    }
                                }
                                KeyCode::Char('r') => {
                                    if let Some(target) = app.layout_adjust_target {
                                        reset_layout_dimension(&app.config, target).await;
                                        update_layout_message(&mut app, target).await;
                                    }
                                }
                                KeyCode::Char('R') => {
                                    reset_all_layout_dimensions(&app.config).await;
                                    app.layout_adjust_message =
                                        "All dimensions reset to defaults".to_string();
                                }
                                KeyCode::Char('s') => {
                                    let cfg_guard = app.config.lock().await;
                                    let conn = lock_mutex(&conn_mutex)?;
                                    if let Err(e) =
                                        crate::core::config::save_config_to_db(&conn, &cfg_guard)
                                    {
                                        app.status_message = format!("Save error: {}", e);
                                    } else {
                                        app.status_message = "Layout saved".to_string();
                                    }
                                    app.input_mode = InputMode::Normal;
                                    app.layout_adjust_target = None;
                                }
                                KeyCode::Esc | KeyCode::Char('q') => {
                                    let loaded_cfg = {
                                        let conn = lock_mutex(&conn_mutex)?;
                                        crate::core::config::load_config_from_db(&conn)
                                    };
                                    if let Ok(loaded_cfg) = loaded_cfg {
                                        *app.config.lock().await = loaded_cfg;
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
                                        ModalAction::None => {}
                                        ModalAction::ClearHistory => {
                                            let conn = lock_mutex(&conn_mutex)?;
                                            let filter_str =
                                                if app.history_filter == HistoryFilter::All {
                                                    None
                                                } else {
                                                    Some(app.history_filter.as_str())
                                                };
                                            if let Err(e) =
                                                crate::db::clear_history(&conn, filter_str)
                                            {
                                                app.status_message =
                                                    format!("Error clearing history: {}", e);
                                            } else {
                                                app.selected_history = 0;
                                                app.status_message = format!(
                                                    "History cleared ({})",
                                                    app.history_filter.as_str()
                                                );
                                                if let Err(e) = app.refresh_jobs(&conn) {
                                                    app.status_message =
                                                        format!("Refresh failed: {}", e);
                                                }
                                            }
                                        }
                                        ModalAction::CancelJob(id) => {
                                            // Trigger cancellation token
                                            if let Some(token) =
                                                app.cancellation_tokens.lock().await.get(&id)
                                            {
                                                token.store(true, Ordering::Relaxed);
                                            }

                                            // Update DB status (Soft Delete / Cancel)
                                            let conn = lock_mutex(&conn_mutex)?;
                                            if let Err(e) = crate::db::cancel_job(&conn, id) {
                                                app.status_message =
                                                    format!("Failed to cancel job {}: {}", id, e);
                                            } else {
                                                app.status_message = format!("Cancelled job {}", id);
                                                // Refresh to update list
                                                if let Err(e) = app.refresh_jobs(&conn) {
                                                    app.status_message =
                                                        format!("Refresh failed: {}", e);
                                                }
                                            }
                                        }
                                        ModalAction::DeleteRemoteObject(key, path_context) => {
                                            app.status_message = format!("Deleting {}...", key);
                                            let tx = app.async_tx.clone();
                                            let config_clone = app.config.lock().await.clone();

                                            tokio::spawn(async move {
                                                let is_dir = key.ends_with('/');

                                                let res = if is_dir {
                                                    crate::services::uploader::Uploader::delete_folder_recursive(
                                                        &config_clone,
                                                        &key,
                                                    )
                                                    .await
                                                    .map(|count| {
                                                        if count == 1 {
                                                            format!("Deleted folder {} (1 object)", key)
                                                        } else {
                                                            format!(
                                                                "Deleted folder {} ({} objects)",
                                                                key, count
                                                            )
                                                        }
                                                    })
                                                } else {
                                                    crate::services::uploader::Uploader::delete_file(
                                                        &config_clone,
                                                        &key,
                                                    )
                                                    .await
                                                    .map(|_| format!("Deleted {}", key))
                                                };

                                                match res {
                                                    Ok(msg) => {
                                                        if let Err(send_err) =
                                                            tx.send(AppEvent::Notification(msg))
                                                        {
                                                            warn!(
                                                                "Failed to send delete notification: {}",
                                                                send_err
                                                            );
                                                        }

                                                        // 2. Refresh the list using the SAME runtime
                                                        let path_arg = if path_context.is_empty() {
                                                            None
                                                        } else {
                                                            Some(path_context.as_str())
                                                        };
                                                        let res_list = crate::services::uploader::Uploader::list_bucket_contents(&config_clone, path_arg).await;

                                                        match res_list {
                                                            Ok(files) => {
                                                                if let Err(send_err) = tx.send(
                                                                    AppEvent::RemoteFileList(
                                                                        path_context.clone(),
                                                                        files,
                                                                    ),
                                                                ) {
                                                                    warn!(
                                                                        "Failed to send remote refresh after delete: {}",
                                                                        send_err
                                                                    );
                                                                }
                                                            }
                                                            Err(e) => {
                                                                if let Err(send_err) = tx.send(
                                                                    AppEvent::Notification(format!(
                                                                        "Refresh Failed: {}",
                                                                        e
                                                                    )),
                                                                ) {
                                                                    warn!(
                                                                        "Failed to send remote refresh error notification: {}",
                                                                        send_err
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        if let Err(send_err) = tx.send(
                                                            AppEvent::Notification(format!(
                                                                "Delete Failed: {}",
                                                                e
                                                            )),
                                                        ) {
                                                            warn!(
                                                                "Failed to send delete failure notification: {}",
                                                                send_err
                                                            );
                                                        }
                                                    }
                                                }
                                            });
                                        }
                                        ModalAction::QuitApp => {
                                            should_quit = true;
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
                                KeyCode::Char('q') if !app.wizard.editing => {
                                    app.input_mode = InputMode::Confirmation;
                                    app.pending_action = ModalAction::QuitApp;
                                    app.confirmation_msg = "Quit Drifter?".to_string();
                                }
                                KeyCode::Esc if app.wizard.editing => {
                                    app.wizard.editing = false;
                                }
                                KeyCode::Enter => {
                                    if app.wizard.step == WizardStep::Done {
                                        // Save config and close wizard
                                        let cfg = Config {
                                            quarantine_dir: app.wizard.quarantine_dir.clone(),
                                            clamd_host: app.wizard.clamd_host.clone(),
                                            clamd_port: app
                                                .wizard
                                                .clamd_port
                                                .parse()
                                                .unwrap_or(3310),
                                            s3_bucket: if app.wizard.bucket.is_empty() {
                                                None
                                            } else {
                                                Some(app.wizard.bucket.clone())
                                            },
                                            s3_region: if app.wizard.region.is_empty() {
                                                None
                                            } else {
                                                Some(app.wizard.region.clone())
                                            },
                                            s3_endpoint: if app.wizard.endpoint.is_empty() {
                                                None
                                            } else {
                                                Some(app.wizard.endpoint.clone())
                                            },
                                            s3_access_key: if app.wizard.access_key.is_empty() {
                                                None
                                            } else {
                                                Some(app.wizard.access_key.clone())
                                            },
                                            s3_secret_key: if app.wizard.secret_key.is_empty() {
                                                None
                                            } else {
                                                Some(app.wizard.secret_key.clone())
                                            },
                                            part_size_mb: app
                                                .wizard
                                                .part_size
                                                .parse()
                                                .unwrap_or(64),
                                            concurrency_upload_global: app
                                                .wizard
                                                .concurrency
                                                .parse()
                                                .unwrap_or(1),
                                            concurrency_upload_parts: 4,
                                            concurrency_scan_parts: 4,
                                            ..Config::default()
                                        };

                                        {
                                            let conn = lock_mutex(&conn_mutex)?;
                                            if let Err(e) =
                                                crate::core::config::save_config_to_db(&conn, &cfg)
                                            {
                                                app.status_message =
                                                    format!("Failed to save setup config: {}", e);
                                            }
                                        }

                                        // Create directories if they don't exist
                                        if let Err(e) = std::fs::create_dir_all(&cfg.quarantine_dir)
                                        {
                                            app.status_message = format!(
                                                "Failed to create quarantine directory: {}",
                                                e
                                            );
                                        }
                                        if let Err(e) = std::fs::create_dir_all(&cfg.state_dir) {
                                            app.status_message =
                                                format!("Failed to create state directory: {}", e);
                                        }

                                        // Update shared config
                                        let mut shared_cfg = app.config.lock().await;
                                        *shared_cfg = cfg;
                                        drop(shared_cfg);

                                        // Reload settings state
                                        let cfg_guard = app.config.lock().await;
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
                        let capturing_input = app.input_mode == InputMode::Confirmation
                            || app.input_mode == InputMode::RemoteFolderCreate
                            || match app.focus {
                                AppFocus::SettingsFields => app.settings.editing,
                                AppFocus::Browser => {
                                    app.input_mode == InputMode::Filter
                                        || app.input_mode == InputMode::Browsing
                                }
                                AppFocus::Logs => app.input_mode == InputMode::LogSearch,
                                AppFocus::Remote => app.input_mode == InputMode::RemoteBrowsing,
                                _ => false,
                            };

                        // Meta keys (Quit, Tab)
                        if !capturing_input {
                            let old_focus = app.focus;
                            match key.code {
                                KeyCode::Char('l')
                                    if key.modifiers.contains(KeyModifiers::CONTROL) =>
                                {
                                    app.input_mode = InputMode::LayoutAdjust;
                                    app.layout_adjust_target = None;
                                    app.layout_adjust_message = "Layout Adjustment: 1=Local 2=Queue 3=History | +/- adjust | r reset | R reset-all | s save | q cancel".to_string();
                                    continue;
                                }
                                KeyCode::Char('q') if app.focus != AppFocus::Logs => {
                                    app.input_mode = InputMode::Confirmation;
                                    app.pending_action = ModalAction::QuitApp;
                                    app.confirmation_msg = "Quit Drifter?".to_string();
                                }
                                KeyCode::Tab => {
                                    app.focus = match app.focus {
                                        AppFocus::Rail => match app.current_tab {
                                            AppTab::Transfers => AppFocus::Browser,
                                            AppTab::Quarantine => AppFocus::Quarantine,
                                            AppTab::Logs => AppFocus::Logs,
                                            AppTab::Settings => AppFocus::SettingsCategory,
                                        },
                                        AppFocus::Browser => {
                                            // In Transfers tab, cycle to Remote; otherwise to Queue
                                            if app.current_tab == AppTab::Transfers {
                                                AppFocus::Remote
                                            } else {
                                                AppFocus::Queue
                                            }
                                        }
                                        AppFocus::Remote => AppFocus::Queue,
                                        AppFocus::Queue => AppFocus::History,
                                        AppFocus::SettingsCategory => AppFocus::SettingsFields,
                                        AppFocus::History
                                        | AppFocus::Quarantine
                                        | AppFocus::Logs
                                        | AppFocus::SettingsFields => AppFocus::Rail,
                                    };
                                }
                                KeyCode::BackTab => {
                                    app.focus = match app.focus {
                                        AppFocus::Rail => match app.current_tab {
                                            AppTab::Transfers => AppFocus::History,
                                            AppTab::Quarantine => AppFocus::Quarantine,
                                            AppTab::Logs => AppFocus::Logs,
                                            AppTab::Settings => AppFocus::SettingsFields,
                                        },
                                        AppFocus::History => AppFocus::Queue,
                                        AppFocus::Queue => {
                                            // In Transfers tab, cycle to Remote; otherwise to Browser
                                            if app.current_tab == AppTab::Transfers {
                                                AppFocus::Remote
                                            } else {
                                                AppFocus::Browser
                                            }
                                        }
                                        AppFocus::Remote => AppFocus::Browser,
                                        AppFocus::SettingsFields => AppFocus::SettingsCategory,
                                        AppFocus::Browser
                                        | AppFocus::Quarantine
                                        | AppFocus::SettingsCategory
                                        | AppFocus::Logs => AppFocus::Rail,
                                    };
                                }
                                KeyCode::Right => {
                                    app.focus = match app.focus {
                                        AppFocus::Rail => match app.current_tab {
                                            AppTab::Transfers => AppFocus::Browser,
                                            AppTab::Quarantine => AppFocus::Quarantine,
                                            AppTab::Logs => AppFocus::Logs,
                                            AppTab::Settings => AppFocus::SettingsCategory,
                                        },
                                        AppFocus::Browser => {
                                            if app.current_tab == AppTab::Transfers {
                                                AppFocus::Remote
                                            } else {
                                                AppFocus::Queue
                                            }
                                        }
                                        AppFocus::Remote => AppFocus::Queue,
                                        AppFocus::Queue => AppFocus::History,
                                        AppFocus::SettingsCategory => AppFocus::SettingsFields,
                                        _ => app.focus,
                                    };
                                }
                                KeyCode::Left => {
                                    app.focus = match app.focus {
                                        AppFocus::History => AppFocus::Queue,
                                        AppFocus::Queue => {
                                            if app.current_tab == AppTab::Transfers {
                                                AppFocus::Remote
                                            } else {
                                                AppFocus::Browser
                                            }
                                        }
                                        AppFocus::Remote => AppFocus::Browser,
                                        AppFocus::Browser
                                        | AppFocus::Quarantine
                                        | AppFocus::SettingsCategory => AppFocus::Rail,
                                        AppFocus::SettingsFields => AppFocus::SettingsCategory,
                                        _ => app.focus,
                                    };
                                }
                                KeyCode::Enter => {
                                    if app.focus == AppFocus::Rail {
                                        app.focus = match app.current_tab {
                                            AppTab::Transfers => AppFocus::Browser,
                                            AppTab::Quarantine => AppFocus::Quarantine,
                                            AppTab::Logs => AppFocus::Logs,
                                            AppTab::Settings => AppFocus::SettingsCategory,
                                        };
                                    }
                                }
                                _ => {}
                            }
                            if app.focus != old_focus
                                && app.current_tab == AppTab::Transfers
                                && app.focus == AppFocus::Remote
                            {
                                request_remote_list(&mut app, false).await;
                            }

                            // If focus changed, don't also process this key in focus-specific handling
                            if app.focus != old_focus {
                                continue;
                            }
                        }

                        // Remote Folder Creation Modal
                        if app.input_mode == InputMode::RemoteFolderCreate {
                            match key.code {
                                KeyCode::Esc => {
                                    app.input_mode = InputMode::RemoteBrowsing;
                                    app.creating_folder_name.clear();
                                }
                                KeyCode::Enter => {
                                    if !app.creating_folder_name.is_empty() {
                                        let folder_name = app.creating_folder_name.clone();
                                        // Construct full key
                                        let folder_key = if app.remote_current_path.is_empty() {
                                            format!("{}/", folder_name)
                                        } else {
                                            format!(
                                                "{}/{}/",
                                                app.remote_current_path.trim_end_matches('/'),
                                                folder_name
                                            )
                                        };

                                        let tx = app.async_tx.clone();
                                        let config = app.config.lock().await.clone();
                                        let folder_name_clone = folder_name.clone();

                                        tokio::spawn(async move {
                                            match crate::services::uploader::Uploader::create_folder(&config, &folder_key).await {
                                                Ok(_) => {
                                                    // Refresh list
                                                    if let Err(send_err) = tx.send(AppEvent::RefreshRemote) {
                                                        warn!("Failed to send RefreshRemote after folder create: {}", send_err);
                                                    }
                                                    if let Err(send_err) = tx.send(AppEvent::Notification(format!("Created folder '{}'", folder_name_clone))) {
                                                        warn!("Failed to send folder created notification: {}", send_err);
                                                    }
                                                }
                                                Err(e) => {
                                                    if let Err(send_err) = tx.send(AppEvent::Notification(format!("Failed to create folder: {}", e))) {
                                                        warn!("Failed to send folder creation failure notification: {}", send_err);
                                                    }
                                                }
                                            }
                                        });

                                        app.input_mode = InputMode::RemoteBrowsing;
                                        app.creating_folder_name.clear();
                                        app.status_message =
                                            format!("Creating folder '{}'...", folder_name);
                                    }
                                }
                                KeyCode::Backspace => {
                                    app.creating_folder_name.pop();
                                }
                                KeyCode::Char(c) => {
                                    if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                                        app.creating_folder_name.push(c);
                                    }
                                }
                                _ => {}
                            }
                            continue;
                        }

                        // Focus-specific handling
                        match app.focus {
                            AppFocus::Rail => match key.code {
                                KeyCode::Up | KeyCode::Char('k') => {
                                    app.current_tab = match app.current_tab {
                                        AppTab::Transfers => AppTab::Settings,
                                        AppTab::Quarantine => AppTab::Transfers,
                                        AppTab::Logs => AppTab::Quarantine,
                                        AppTab::Settings => AppTab::Logs,
                                    };
                                }
                                KeyCode::Down | KeyCode::Char('j') => {
                                    app.current_tab = match app.current_tab {
                                        AppTab::Transfers => AppTab::Quarantine,
                                        AppTab::Quarantine => AppTab::Logs,
                                        AppTab::Logs => AppTab::Settings,
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
                                                app.status_message =
                                                    "Browsing files...".to_string();
                                            }
                                            KeyCode::Up | KeyCode::Char('k') => {
                                                app.browser_move_up()
                                            }
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
                                            KeyCode::Up | KeyCode::Char('k') => {
                                                app.browser_move_up()
                                            }
                                            KeyCode::Down | KeyCode::Char('j') => {
                                                app.browser_move_down()
                                            }
                                            KeyCode::PageUp => app.picker.page_up(),
                                            KeyCode::PageDown => app.picker.page_down(),
                                            KeyCode::Left
                                            | KeyCode::Char('h')
                                            | KeyCode::Backspace => app.picker.go_parent(),
                                            KeyCode::Right
                                            | KeyCode::Char('l')
                                            | KeyCode::Enter => {
                                                if let Some(entry) =
                                                    app.picker.selected_entry().cloned()
                                                {
                                                    if app.picker.is_searching
                                                        && app.picker.search_recursive
                                                    {
                                                        // Jump to parent directory
                                                        if let Some(parent) = entry.path.parent() {
                                                            app.picker
                                                                .try_set_cwd(parent.to_path_buf());
                                                            app.input_buffer.clear();
                                                            app.picker.is_searching = false;
                                                            app.picker.refresh();

                                                            // Find the index of the file we were just looking at
                                                            if let Some(idx) =
                                                                app.picker.entries.iter().position(
                                                                    |e| e.path == entry.path,
                                                                )
                                                            {
                                                                app.picker.selected = idx;
                                                            }
                                                            app.status_message = format!(
                                                                "Jumped to {}",
                                                                parent.display()
                                                            );
                                                        }
                                                    } else if entry.is_dir {
                                                        if app.picker.view == PickerView::Tree
                                                            && key.code == KeyCode::Right
                                                        {
                                                            app.picker.toggle_expand();
                                                        } else {
                                                            app.picker
                                                                .try_set_cwd(entry.path.clone());
                                                            app.input_buffer.clear();
                                                            app.picker.is_searching = false;
                                                        }
                                                    } else if key.code == KeyCode::Enter
                                                        || key.code == KeyCode::Char('l')
                                                    {
                                                        // Only queue files on Enter or 'l', not Right arrow
                                                        let conn_clone = conn_mutex.clone();
                                                        let tx = app.async_tx.clone();
                                                        let path = entry
                                                            .path
                                                            .to_string_lossy()
                                                            .to_string();

                                                        tokio::spawn(async move {
                                                            let session_id =
                                                                Uuid::new_v4().to_string();
                                                            if let Err(e) = ingest_path(
                                                                conn_clone,
                                                                &path,
                                                                &session_id,
                                                                None,
                                                            )
                                                            .await
                                                                && let Err(send_err) = tx.send(
                                                                    AppEvent::Notification(format!(
                                                                        "Failed to queue file: {}",
                                                                        e
                                                                    )),
                                                                )
                                                            {
                                                                warn!(
                                                                    "Failed to send queue failure notification: {}",
                                                                    send_err
                                                                );
                                                            }
                                                        });

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
                                                app.status_message =
                                                    "Search (recursive)".to_string();
                                            }
                                            KeyCode::Char(' ') => app.picker.toggle_select(),
                                            KeyCode::Char('t') => app.picker.toggle_view(),
                                            KeyCode::Char('f') => {
                                                app.picker.search_recursive =
                                                    !app.picker.search_recursive;
                                                app.picker.refresh();
                                                app.status_message = if app.picker.search_recursive
                                                {
                                                    "Recursive search enabled"
                                                } else {
                                                    "Recursive search disabled"
                                                }
                                                .to_string();
                                            }
                                            KeyCode::Char('c') => app.picker.clear_selected(),
                                            KeyCode::Char('s') => {
                                                let paths: Vec<PathBuf> = app
                                                    .picker
                                                    .selected_paths
                                                    .iter()
                                                    .cloned()
                                                    .collect();
                                                if !paths.is_empty() {
                                                    // Check if S3 is configured
                                                    let s3_configured = {
                                                        let cfg = app.config.lock().await;
                                                        s3_ready(&cfg)
                                                    };

                                                    if s3_configured {
                                                        // S3 configured, ingest with current remote path
                                                        let destination = if app
                                                            .remote_current_path
                                                            .is_empty()
                                                        {
                                                            None
                                                        } else {
                                                            Some(app.remote_current_path.clone())
                                                        };

                                                        let paths_count = paths.len();
                                                        let conn_clone = conn_mutex.clone();
                                                        let dest_clone = destination.clone();

                                                        let pending_paths: Vec<String> = paths
                                                            .iter()
                                                            .map(|p| {
                                                                p.to_string_lossy().to_string()
                                                            })
                                                            .collect();

                                                        tokio::spawn(async move {
                                                            let session_id =
                                                                Uuid::new_v4().to_string();
                                                            let mut total = 0;
                                                            for path in pending_paths {
                                                                if let Ok(count) = ingest_path(
                                                                    conn_clone.clone(),
                                                                    &path,
                                                                    &session_id,
                                                                    dest_clone.as_deref(),
                                                                )
                                                                .await
                                                                {
                                                                    total += count;
                                                                }
                                                            }
                                                            tracing::info!(
                                                                "Ingest complete: {} files to destination {:?}",
                                                                total,
                                                                dest_clone
                                                            );
                                                        });

                                                        app.status_message = format!(
                                                            "Ingesting {} items to '{}'",
                                                            paths_count,
                                                            if let Some(d) = &destination {
                                                                d
                                                            } else {
                                                                "(bucket root)"
                                                            }
                                                        );
                                                        app.picker.clear_selected();
                                                    } else {
                                                        // S3 not configured, ingest with default path
                                                        let conn_clone = conn_mutex.clone();
                                                        let paths_count = paths.len();
                                                        tokio::spawn(async move {
                                                            let session_id =
                                                                Uuid::new_v4().to_string();
                                                            let mut total = 0;
                                                            for path in paths {
                                                                if let Ok(count) = ingest_path(
                                                                    conn_clone.clone(),
                                                                    &path.to_string_lossy(),
                                                                    &session_id,
                                                                    None,
                                                                )
                                                                .await
                                                                {
                                                                    total += count;
                                                                }
                                                            }
                                                            tracing::info!(
                                                                "Bulk ingest complete: {} files",
                                                                total
                                                            );
                                                        });

                                                        app.status_message = format!(
                                                            "Ingesting {} items in background",
                                                            paths_count
                                                        );
                                                        app.picker.clear_selected();
                                                    }
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
                                        KeyCode::Enter | KeyCode::Right => {
                                            if app.picker.search_recursive
                                                && let Some(entry) =
                                                    app.picker.selected_entry().cloned()
                                                && let Some(parent) = entry.path.parent()
                                            {
                                                app.picker.try_set_cwd(parent.to_path_buf());
                                                app.input_buffer.clear();
                                                app.picker.is_searching = false;
                                                app.picker.refresh();

                                                if let Some(idx) = app
                                                    .picker
                                                    .entries
                                                    .iter()
                                                    .position(|e| e.path == entry.path)
                                                {
                                                    app.picker.selected = idx;
                                                }
                                                app.status_message =
                                                    format!("Jumped to {}", parent.display());
                                            }
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
                                    InputMode::LayoutAdjust => {}
                                    InputMode::LogSearch
                                    | InputMode::Confirmation
                                    | InputMode::QueueSearch
                                    | InputMode::HistorySearch
                                    | InputMode::RemoteBrowsing
                                    | InputMode::RemoteFolderCreate => {}
                                }
                            }
                            AppFocus::Queue => {
                                match app.input_mode {
                                    InputMode::Normal => {
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
                                                let conn = lock_mutex(&conn_mutex)?;
                                                if let Err(e) = app.refresh_jobs(&conn) {
                                                    app.status_message =
                                                        format!("Refresh failed: {}", e);
                                                }
                                            }
                                            KeyCode::Char('p') => {
                                                app.toggle_pause_selected_job().await;
                                            }
                                            KeyCode::Char('+') | KeyCode::Char('=') => {
                                                app.change_selected_job_priority(1);
                                            }
                                            KeyCode::Char('-') => {
                                                app.change_selected_job_priority(-1);
                                            }
                                            KeyCode::Char('/') => {
                                                app.input_mode = InputMode::QueueSearch;
                                                app.queue_search_query.clear();
                                                app.status_message = "Search queue...".to_string();
                                            }
                                            KeyCode::Char('R') => {
                                                let conn = lock_mutex(&conn_mutex)?;
                                                if let Err(e) = app.refresh_jobs(&conn) {
                                                    app.status_message =
                                                        format!("Refresh failed: {}", e);
                                                }
                                            }
                                            KeyCode::Char('r') => {
                                                if !app.visual_jobs.is_empty()
                                                    && app.selected < app.visual_jobs.len()
                                                    && let Some(idx) =
                                                        app.visual_jobs[app.selected].index_in_jobs
                                                {
                                                    let id = app.jobs[idx].id;
                                                    let conn = lock_mutex(&conn_mutex)?;
                                                    if let Err(e) = crate::db::retry_job(&conn, id)
                                                    {
                                                        app.status_message =
                                                            format!("Retry failed: {}", e);
                                                    } else {
                                                        app.status_message =
                                                            format!("Retried job {}", id);
                                                    }
                                                }
                                            }
                                            KeyCode::Char('d') | KeyCode::Delete => {
                                                if !app.visual_jobs.is_empty()
                                                    && app.selected < app.visual_jobs.len()
                                                    && let Some(idx) =
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
                                                        app.pending_action =
                                                            ModalAction::CancelJob(id);
                                                        app.confirmation_msg = format!(
                                                            "Cancel active job #{}? (y/n)",
                                                            id
                                                        );
                                                        app.input_mode = InputMode::Confirmation;
                                                    } else {
                                                        // Just do it (Soft delete/Cancel)
                                                        let conn = lock_mutex(&conn_mutex)?;
                                                        if let Err(e) =
                                                            crate::db::cancel_job(&conn, id)
                                                        {
                                                            app.status_message = format!(
                                                                "Failed to remove job {}: {}",
                                                                id, e
                                                            );
                                                        } else {
                                                            app.status_message =
                                                                format!("Removed job {}", id);
                                                            if let Err(e) = app.refresh_jobs(&conn)
                                                            {
                                                                app.status_message = format!(
                                                                    "Refresh failed: {}",
                                                                    e
                                                                );
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            KeyCode::Char('c') => {
                                                // Clear all completed jobs
                                                let conn = lock_mutex(&conn_mutex)?;
                                                let ids_to_delete: Vec<i64> = app
                                                    .jobs
                                                    .iter()
                                                    .filter(|j| j.status == "complete")
                                                    .map(|j| j.id)
                                                    .collect();
                                                let count = ids_to_delete.len();
                                                let mut failed = 0usize;
                                                for id in ids_to_delete {
                                                    if let Err(e) = crate::db::delete_job(&conn, id)
                                                    {
                                                        failed += 1;
                                                        tracing::error!(
                                                            "Failed deleting completed job {}: {}",
                                                            id,
                                                            e
                                                        );
                                                    }
                                                }
                                                if count > 0 && failed == 0 {
                                                    app.status_message =
                                                        format!("Cleared {} completed jobs", count);
                                                    if let Err(e) = app.refresh_jobs(&conn) {
                                                        app.status_message =
                                                            format!("Refresh failed: {}", e);
                                                    }
                                                } else if failed > 0 {
                                                    app.status_message = format!(
                                                        "Cleared {} jobs ({} failed)",
                                                        count.saturating_sub(failed),
                                                        failed
                                                    );
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    InputMode::QueueSearch => match key.code {
                                        KeyCode::Esc | KeyCode::Enter => {
                                            app.input_mode = InputMode::Normal;
                                        }
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
                                        KeyCode::Backspace => {
                                            app.queue_search_query.pop();
                                            app.rebuild_visual_lists();
                                        }
                                        KeyCode::Char(c) => {
                                            app.queue_search_query.push(c);
                                            app.rebuild_visual_lists();
                                        }
                                        _ => {}
                                    },
                                    _ => {}
                                }
                            }
                            AppFocus::History => {
                                match app.input_mode {
                                    InputMode::Normal => {
                                        match key.code {
                                            KeyCode::Up | KeyCode::Char('k') => {
                                                if app.selected_history > 0 {
                                                    app.selected_history -= 1;
                                                }
                                            }
                                            KeyCode::Down | KeyCode::Char('j') => {
                                                if app.selected_history + 1
                                                    < app.visual_history.len()
                                                {
                                                    app.selected_history += 1;
                                                }
                                            }
                                            KeyCode::Char('t') => {
                                                app.view_mode = match app.view_mode {
                                                    ViewMode::Flat => ViewMode::Tree,
                                                    ViewMode::Tree => ViewMode::Flat,
                                                };
                                                let conn = lock_mutex(&conn_mutex)?;
                                                if let Err(e) = app.refresh_jobs(&conn) {
                                                    app.status_message =
                                                        format!("Refresh failed: {}", e);
                                                }
                                            }
                                            KeyCode::Char('/') => {
                                                app.input_mode = InputMode::HistorySearch;
                                                app.history_search_query.clear();
                                                app.status_message =
                                                    "Search history...".to_string();
                                            }
                                            KeyCode::Char('f') => {
                                                app.history_filter = app.history_filter.next();
                                                app.selected_history = 0; // Reset selection
                                                app.status_message = format!(
                                                    "History Filter: {}",
                                                    app.history_filter.as_str()
                                                );
                                                let conn = lock_mutex(&conn_mutex)?;
                                                if let Err(e) = app.refresh_jobs(&conn) {
                                                    app.status_message =
                                                        format!("Refresh failed: {}", e);
                                                }
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
                                                    && app.selected_history
                                                        < app.visual_history.len()
                                                    && let Some(idx) = app.visual_history
                                                        [app.selected_history]
                                                        .index_in_jobs
                                                {
                                                    let job = &app.history[idx];
                                                    let id = job.id;
                                                    let conn = lock_mutex(&conn_mutex)?;
                                                    if let Err(e) = crate::db::delete_job(&conn, id)
                                                    {
                                                        app.status_message =
                                                            format!("Delete failed: {}", e);
                                                    } else if let Err(e) =
                                                        app.refresh_jobs(&conn)
                                                    {
                                                        app.status_message =
                                                            format!("Refresh failed: {}", e);
                                                    }
                                                }
                                            }
                                            KeyCode::Char('R') => {
                                                let conn = lock_mutex(&conn_mutex)?;
                                                if let Err(e) = app.refresh_jobs(&conn) {
                                                    app.status_message =
                                                        format!("Refresh failed: {}", e);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    InputMode::HistorySearch => match key.code {
                                        KeyCode::Esc | KeyCode::Enter => {
                                            app.input_mode = InputMode::Normal;
                                        }
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
                                        KeyCode::Backspace => {
                                            app.history_search_query.pop();
                                            app.rebuild_visual_lists();
                                        }
                                        KeyCode::Char(c) => {
                                            app.history_search_query.push(c);
                                            app.rebuild_visual_lists();
                                        }
                                        _ => {}
                                    },
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

                                        let conn = lock_mutex(&conn_mutex)?;
                                        let mut quarantine_removed = true;
                                        if let Err(e) = crate::db::update_scan_status(
                                            &conn,
                                            id,
                                            "removed",
                                            "quarantined_removed",
                                        ) {
                                            quarantine_removed = false;
                                            app.status_message =
                                                format!("Failed to update quarantine status: {}", e);
                                        }

                                        if let Some(p) = q_path
                                            && let Err(e) = std::fs::remove_file(&p)
                                        {
                                            quarantine_removed = false;
                                            app.status_message = format!(
                                                "Failed to remove quarantined file {}: {}",
                                                p, e
                                            );
                                        }

                                        if quarantine_removed {
                                            app.status_message = format!(
                                                "Threat neutralized: File '{}' deleted",
                                                job.source_path
                                            );
                                        }

                                        if app.selected_quarantine >= app.quarantine.len()
                                            && app.selected_quarantine > 0
                                        {
                                            app.selected_quarantine -= 1;
                                        }
                                    }
                                }
                                KeyCode::Char('R') => {
                                    let conn = lock_mutex(&conn_mutex)?;
                                    if let Err(e) = app.refresh_jobs(&conn) {
                                        app.status_message = format!("Refresh failed: {}", e);
                                    }
                                }
                                _ => {}
                            },
                            AppFocus::Remote => {
                                match app.input_mode {
                                    InputMode::Normal => match key.code {
                                        KeyCode::Up | KeyCode::Char('k') => {
                                            if app.selected_remote > 0 {
                                                app.selected_remote -= 1;
                                            }
                                        }
                                        KeyCode::Down | KeyCode::Char('j') => {
                                            if app.selected_remote + 1 < app.s3_objects.len() {
                                                app.selected_remote += 1;
                                            }
                                        }
                                        KeyCode::Char('a') => {
                                            app.input_mode = InputMode::RemoteBrowsing;
                                            app.status_message = "Browsing Remote...".to_string();
                                        }
                                        KeyCode::Char('d') => {
                                            if let Some(obj) = selected_remote_object(&app) {
                                                if obj.is_dir {
                                                    app.set_status(
                                                        "Cannot download directories yet."
                                                            .to_string(),
                                                    );
                                                } else {
                                                    start_remote_download(&mut app, obj.key).await;
                                                }
                                            }
                                        }
                                        KeyCode::Char('x') => {
                                            if let Some(obj) = selected_remote_object(&app) {
                                                prepare_remote_delete(
                                                    &mut app, obj.key, obj.name, obj.is_dir,
                                                )
                                                .await;
                                            }
                                        }
                                        KeyCode::Char('r') => {
                                            // Force refresh (bypass cache)
                                            request_remote_list(&mut app, true).await;
                                        }
                                        _ => {}
                                    },
                                    InputMode::RemoteBrowsing => {
                                        match key.code {
                                            KeyCode::Esc | KeyCode::Char('q') => {
                                                app.input_mode = InputMode::Normal;
                                                app.status_message = "Ready".to_string();
                                            }
                                            KeyCode::Char('n') => {
                                                app.input_mode = InputMode::RemoteFolderCreate;
                                                app.creating_folder_name.clear();
                                            }
                                            KeyCode::Up | KeyCode::Char('k') => {
                                                if app.selected_remote > 0 {
                                                    app.selected_remote -= 1;
                                                }
                                            }
                                            KeyCode::Down | KeyCode::Char('j') => {
                                                if app.selected_remote + 1 < app.s3_objects.len() {
                                                    app.selected_remote += 1;
                                                }
                                            }
                                            KeyCode::Right | KeyCode::Enter => {
                                                if !app.s3_objects.is_empty()
                                                    && app.selected_remote < app.s3_objects.len()
                                                {
                                                    let obj = &app.s3_objects[app.selected_remote];
                                                    if obj.is_dir {
                                                        // Handle parent directory navigation
                                                        if obj.is_parent {
                                                            let current = app
                                                                .remote_current_path
                                                                .trim_end_matches('/');
                                                            if let Some(idx) = current.rfind('/') {
                                                                app.remote_current_path
                                                                    .truncate(idx + 1);
                                                            } else {
                                                                app.remote_current_path.clear();
                                                            }
                                                        } else {
                                                            app.remote_current_path
                                                                .push_str(&obj.name);
                                                        }
                                                        // Use cached request
                                                        request_remote_list(&mut app, false).await;
                                                    }
                                                }
                                            }
                                            KeyCode::Left | KeyCode::Backspace => {
                                                if !app.remote_current_path.is_empty() {
                                                    let current = app
                                                        .remote_current_path
                                                        .trim_end_matches('/');
                                                    if let Some(idx) = current.rfind('/') {
                                                        app.remote_current_path.truncate(idx + 1);
                                                    } else {
                                                        app.remote_current_path.clear();
                                                    }
                                                    // Use cached request
                                                    request_remote_list(&mut app, false).await;
                                                }
                                            }
                                            // Allow downloading/deleting in browsing mode too
                                            KeyCode::Char('d') => {
                                                if let Some(obj) = selected_remote_object(&app) {
                                                    if obj.is_dir {
                                                        app.set_status(
                                                            "Cannot download directories yet."
                                                                .to_string(),
                                                        );
                                                    } else {
                                                        start_remote_download(&mut app, obj.key)
                                                            .await;
                                                    }
                                                }
                                            }
                                            KeyCode::Char('x') => {
                                                if let Some(obj) = selected_remote_object(&app) {
                                                    prepare_remote_delete(
                                                        &mut app, obj.key, obj.name, obj.is_dir,
                                                    )
                                                    .await;
                                                }
                                            }
                                            KeyCode::Char('r') => {
                                                // Force refresh (bypass cache)
                                                request_remote_list(&mut app, true).await;
                                            }
                                            _ => {}
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            AppFocus::SettingsCategory => {
                                match key.code {
                                    KeyCode::Up | KeyCode::Char('k') => {
                                        app.settings.active_category =
                                            match app.settings.active_category {
                                                SettingsCategory::S3 => SettingsCategory::Theme,
                                                SettingsCategory::Scanner => SettingsCategory::S3,
                                                SettingsCategory::Performance => {
                                                    SettingsCategory::Scanner
                                                }
                                                SettingsCategory::Theme => {
                                                    SettingsCategory::Performance
                                                }
                                            };
                                        app.settings.selected_field = 0;
                                    }
                                    KeyCode::Down | KeyCode::Char('j') => {
                                        app.settings.active_category =
                                            match app.settings.active_category {
                                                SettingsCategory::S3 => SettingsCategory::Scanner,
                                                SettingsCategory::Scanner => {
                                                    SettingsCategory::Performance
                                                }
                                                SettingsCategory::Performance => {
                                                    SettingsCategory::Theme
                                                }
                                                SettingsCategory::Theme => SettingsCategory::S3,
                                            };
                                        app.settings.selected_field = 0;
                                    }
                                    KeyCode::Right | KeyCode::Enter => {
                                        app.focus = AppFocus::SettingsFields;
                                    }
                                    KeyCode::Char('s') => {
                                        let res = {
                                            let mut cfg = app.config.lock().await;
                                            app.settings.apply_to_config(&mut cfg);

                                            // Apply theme immediately
                                            app.theme = Theme::from_name(&cfg.theme);

                                            // Save entire config to database
                                            let conn = lock_mutex(&conn_mutex)?;
                                            crate::core::config::save_config_to_db(&conn, &cfg)
                                        };

                                        if let Err(e) = res {
                                            app.set_status(format!("Save error: {}", e));
                                        } else {
                                            app.set_status("Configuration saved");
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            AppFocus::SettingsFields => {
                                let field_count = app.settings.active_category.field_count();
                                // Focus-specific rendering logic for fields is handled in render.rs,
                                // input handling is primarily navigation here unless specific overrides needed.

                                match key.code {
                                    KeyCode::Enter => {
                                        // Special handling for Boolean Toggles
                                        let is_toggle = (app.settings.active_category
                                            == SettingsCategory::Scanner
                                            && app.settings.selected_field == 3)
                                            || (app.settings.active_category
                                                == SettingsCategory::Performance
                                                && app.settings.selected_field >= 4);

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
                                            let res = {
                                                let mut cfg = app.config.lock().await;
                                                app.settings.apply_to_config(&mut cfg);
                                                let conn = lock_mutex(&conn_mutex)?;
                                                crate::core::config::save_config_to_db(&conn, &cfg)
                                            };

                                            if let Err(e) = res {
                                                app.set_status(format!("Save error: {}", e));
                                            }
                                        } else {
                                            app.settings.editing = !app.settings.editing;
                                        }

                                        if app.settings.editing {
                                            // Entering edit mode
                                            if app.settings.active_category
                                                == SettingsCategory::Theme
                                            {
                                                app.settings.original_theme =
                                                    Some(app.settings.theme.clone());
                                            }
                                        } else {
                                            // Exiting edit mode (Confirming) - Trigger Auto-Save
                                            if app.settings.active_category
                                                == SettingsCategory::Theme
                                            {
                                                app.settings.original_theme = None;
                                            }

                                            let (res, field_name) = {
                                                let mut cfg = app.config.lock().await;
                                                app.settings.apply_to_config(&mut cfg);

                                                // Apply theme immediately
                                                app.theme = Theme::from_name(&cfg.theme);

                                                let field_name = match app.settings.active_category
                                                {
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

                                                // Save entire config to database
                                                let conn = lock_mutex(&conn_mutex)?;
                                                (
                                                    crate::core::config::save_config_to_db(
                                                        &conn, &cfg,
                                                    ),
                                                    field_name,
                                                )
                                            };

                                            if let Err(e) = res {
                                                app.set_status(format!("Save error: {}", e));
                                            } else {
                                                app.set_status(format!("Saved: {}", field_name));
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
                                            app.settings.theme =
                                                app.theme_names[next_idx].to_string();
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
                                            app.settings.theme =
                                                app.theme_names[prev_idx].to_string();
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
                                                        let prev_idx =
                                                            (idx + app.theme_names.len() - 1)
                                                                % app.theme_names.len();
                                                        app.settings.theme =
                                                            app.theme_names[prev_idx].to_string();
                                                        app.theme =
                                                            Theme::from_name(&app.settings.theme);
                                                    }
                                                }
                                            }
                                            (SettingsCategory::S3, 0) => {
                                                app.settings.endpoint.push(c)
                                            }
                                            (SettingsCategory::S3, 1) => {
                                                app.settings.bucket.push(c)
                                            }
                                            (SettingsCategory::S3, 2) => {
                                                app.settings.region.push(c)
                                            }
                                            (SettingsCategory::S3, 3) => {
                                                app.settings.prefix.push(c)
                                            }
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
                                                app.settings.concurrency_upload_parts.push(c)
                                            }
                                            (SettingsCategory::Performance, 3) => {
                                                app.settings.concurrency_scan_parts.push(c)
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
                                                app.settings.concurrency_upload_parts.pop();
                                            }
                                            (SettingsCategory::Performance, 3) => {
                                                app.settings.concurrency_scan_parts.pop();
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
                                            || app.settings.active_category
                                                == SettingsCategory::Theme =>
                                    {
                                        // If saving while editing theme, exit edit mode and commit
                                        if app.settings.editing
                                            && app.settings.active_category
                                                == SettingsCategory::Theme
                                        {
                                            app.settings.editing = false;
                                            app.settings.original_theme = None;
                                        }

                                        let (res, bucket_name) = {
                                            let mut cfg = app.config.lock().await;
                                            app.settings.apply_to_config(&mut cfg);

                                            // Apply theme immediately
                                            app.theme = Theme::from_name(&cfg.theme);

                                            let bucket_name = cfg.s3_bucket.clone();

                                            // Save entire config to database
                                            let conn = lock_mutex(&conn_mutex)?;
                                            (
                                                crate::core::config::save_config_to_db(&conn, &cfg),
                                                bucket_name,
                                            )
                                        };

                                        if let Err(e) = res {
                                            app.set_status(format!("Save error: {}", e));
                                        } else {
                                            app.set_status(format!(
                                                "Configuration saved. Bucket: {}",
                                                bucket_name.as_deref().unwrap_or("None")
                                            ));
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
                                            app.set_status("Testing connection...");
                                            let tx = app.async_tx.clone();
                                            let config_clone = app.config.lock().await.clone();

                                            tokio::spawn(async move {
                                                let res = if cat == SettingsCategory::S3 {
                                                    crate::services::uploader::Uploader::check_connection(
                                                    &config_clone,
                                                )
                                                .await
                                                } else {
                                                    crate::services::scanner::Scanner::new(
                                                        &config_clone,
                                                    )
                                                    .check_connection()
                                                    .await
                                                };

                                                let msg = match res {
                                                    Ok(s) => s,
                                                    Err(e) => format!("Connection Failed: {}", e),
                                                };
                                                if let Err(send_err) =
                                                    tx.send(AppEvent::Notification(msg))
                                                {
                                                    warn!(
                                                        "Failed to send scanner connection notification: {}",
                                                        send_err
                                                    );
                                                }
                                            });
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            AppFocus::Logs => {
                                match app.input_mode {
                                    InputMode::LogSearch => {
                                        match key.code {
                                            KeyCode::Enter => {
                                                // Execute search
                                                app.input_mode = InputMode::Normal;
                                                app.log_search_results.clear();
                                                if !app.log_search_query.is_empty() {
                                                    let query = app.log_search_query.to_lowercase();
                                                    for (i, line) in app.logs.iter().enumerate() {
                                                        if line.to_lowercase().contains(&query) {
                                                            app.log_search_results.push(i);
                                                        }
                                                    }
                                                }
                                                if !app.log_search_results.is_empty() {
                                                    app.log_search_current = 0;
                                                    app.logs_scroll = app.log_search_results[0];
                                                    app.logs_stick_to_bottom = false;
                                                    app.status_message = format!(
                                                        "Found {} matches",
                                                        app.log_search_results.len()
                                                    );
                                                } else {
                                                    app.status_message =
                                                        "No matches found".to_string();
                                                }
                                            }

                                            KeyCode::Esc => {
                                                app.input_mode = InputMode::Normal;
                                                app.log_search_active = false;
                                                app.status_message = "Search cancelled".to_string();
                                            }
                                            KeyCode::Char(c) => {
                                                app.log_search_query.push(c);
                                            }
                                            KeyCode::Backspace => {
                                                app.log_search_query.pop();
                                            }
                                            _ => {}
                                        }
                                    }
                                    _ => {
                                        // Normal Navigation
                                        match key.code {
                                            KeyCode::Char('/') => {
                                                app.input_mode = InputMode::LogSearch;
                                                app.log_search_query.clear();
                                                app.log_search_active = true;
                                                app.log_search_results.clear();
                                                app.status_message = "Search logs...".to_string();
                                            }
                                            KeyCode::Char('n') => {
                                                if !app.log_search_results.is_empty() {
                                                    app.log_search_current =
                                                        (app.log_search_current + 1)
                                                            % app.log_search_results.len();
                                                    app.logs_scroll = app.log_search_results
                                                        [app.log_search_current];
                                                    app.logs_stick_to_bottom = false;
                                                    app.status_message = format!(
                                                        "Match {}/{}",
                                                        app.log_search_current + 1,
                                                        app.log_search_results.len()
                                                    );
                                                }
                                            }
                                            KeyCode::Char('N') => {
                                                if !app.log_search_results.is_empty() {
                                                    if app.log_search_current == 0 {
                                                        app.log_search_current =
                                                            app.log_search_results.len() - 1;
                                                    } else {
                                                        app.log_search_current -= 1;
                                                    }
                                                    app.logs_scroll = app.log_search_results
                                                        [app.log_search_current];
                                                    app.logs_stick_to_bottom = false;
                                                    app.status_message = format!(
                                                        "Match {}/{}",
                                                        app.log_search_current + 1,
                                                        app.log_search_results.len()
                                                    );
                                                }
                                            }
                                            KeyCode::Char('g') | KeyCode::Home => {
                                                app.logs_scroll = 0;
                                                app.logs_stick_to_bottom = false;
                                            }
                                            KeyCode::Char('G') | KeyCode::End => {
                                                app.logs_stick_to_bottom = true;
                                                if !app.logs.is_empty() {
                                                    app.logs_scroll =
                                                        app.logs.len().saturating_sub(1);
                                                }
                                            }
                                            KeyCode::PageUp => {
                                                app.logs_stick_to_bottom = false;
                                                app.logs_scroll =
                                                    app.logs_scroll.saturating_sub(15);
                                            }
                                            KeyCode::PageDown => {
                                                if app.logs_scroll + 15 < app.logs.len() {
                                                    app.logs_scroll += 15;
                                                } else {
                                                    app.logs_stick_to_bottom = true;
                                                    if !app.logs.is_empty() {
                                                        app.logs_scroll =
                                                            app.logs.len().saturating_sub(1);
                                                    }
                                                }
                                            }
                                            KeyCode::Up | KeyCode::Char('k') => {
                                                app.logs_stick_to_bottom = false;
                                                if app.logs_scroll > 0 {
                                                    app.logs_scroll -= 1;
                                                }
                                            }
                                            KeyCode::Down | KeyCode::Char('j') => {
                                                if app.logs_scroll + 1 < app.logs.len() {
                                                    app.logs_scroll += 1;
                                                }
                                                if app.logs_scroll + 1 == app.logs.len() {
                                                    app.logs_stick_to_bottom = true;
                                                }
                                            }
                                            KeyCode::Esc | KeyCode::Char('q') => {
                                                if app.log_search_active {
                                                    app.log_search_active = false;
                                                    app.log_search_results.clear();
                                                    app.status_message =
                                                        "Search cleared".to_string();
                                                } else {
                                                    app.focus = AppFocus::Rail;
                                                }
                                            }

                                            // Horizontal Scroll
                                            KeyCode::Left | KeyCode::Char('h') => {
                                                if app.logs_scroll_x > 0 {
                                                    app.logs_scroll_x =
                                                        app.logs_scroll_x.saturating_sub(5);
                                                }
                                            }
                                            KeyCode::Right | KeyCode::Char('l') => {
                                                app.logs_scroll_x += 5;
                                            }

                                            // Log Levels
                                            KeyCode::Char('1') => app.set_log_level("error").await,
                                            KeyCode::Char('2') => app.set_log_level("warn").await,
                                            KeyCode::Char('3') => app.set_log_level("info").await,
                                            KeyCode::Char('4') => app.set_log_level("debug").await,
                                            KeyCode::Char('5') => app.set_log_level("trace").await,

                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Event::Mouse(mouse) => {
                        // Track hover position for row highlighting
                        if mouse.kind == MouseEventKind::Moved {
                            app.hover_pos = Some((mouse.column, mouse.row));
                        }

                        let history_width = app.cached_config.history_width;

                        if let Ok(size) = terminal.size() {
                            let main_layout = Layout::default()
                                .direction(Direction::Horizontal)
                                .constraints([
                                    Constraint::Length(16),
                                    Constraint::Min(0),
                                    Constraint::Length(history_width),
                                ])
                                .split(Rect::new(0, 0, size.width, size.height.saturating_sub(1)));

                            let rail_layout = main_layout[0];
                            let center_layout = main_layout[1];
                            let right_layout = main_layout[2];
                            let x = mouse.column;
                            let y = mouse.row;

                            if mouse.kind == MouseEventKind::Down(MouseButton::Left) {
                                if x >= rail_layout.x && x < rail_layout.x + rail_layout.width {
                                    // Rail Click
                                    app.focus = AppFocus::Rail;
                                    let rel_ry = y.saturating_sub(rail_layout.y);
                                    match rel_ry {
                                        1 => {
                                            app.current_tab = AppTab::Transfers;
                                        }
                                        2 => {
                                            app.current_tab = AppTab::Quarantine;
                                        }
                                        3 => {
                                            app.current_tab = AppTab::Logs;
                                        }
                                        4 => {
                                            app.current_tab = AppTab::Settings;
                                        }
                                        _ => {}
                                    }
                                } else if x >= center_layout.x
                                    && x < center_layout.x + center_layout.width
                                {
                                    // Center Click
                                    match app.current_tab {
                                        AppTab::Logs => {
                                            app.focus = AppFocus::Logs;
                                        }
                                        AppTab::Transfers => {
                                            let local_percent =
                                                app.cached_config.local_width_percent;
                                            let vertical_chunks = Layout::default()
                                                .direction(Direction::Vertical)
                                                .constraints([
                                                    Constraint::Min(0),
                                                    Constraint::Length(20),
                                                ])
                                                .split(center_layout);

                                            let top_area = vertical_chunks[0];
                                            let queue_area = vertical_chunks[1];

                                            if y < top_area.y + top_area.height {
                                                let chunks = Layout::default()
                                                    .direction(Direction::Horizontal)
                                                    .constraints([
                                                        Constraint::Percentage(local_percent),
                                                        Constraint::Percentage(100 - local_percent),
                                                    ])
                                                    .split(top_area);

                                                if x < chunks[0].x + chunks[0].width {
                                                    app.focus = AppFocus::Browser;
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

                                                    if is_double_click {
                                                        app.last_click_time = None;
                                                        if app.input_mode == InputMode::Normal {
                                                            app.input_mode = InputMode::Browsing;
                                                        }
                                                    } else {
                                                        app.last_click_time = Some(now);
                                                        app.last_click_pos = Some((x, y));
                                                    }

                                                    let inner_y = chunks[0].y + 1;
                                                    let has_filter = !app.input_buffer.is_empty()
                                                        || app.input_mode == InputMode::Filter;
                                                    let table_y =
                                                        inner_y + if has_filter { 1 } else { 0 };
                                                    let content_y = table_y + 1; // +1 for header

                                                    if y >= content_y {
                                                        let relative_row = (y - content_y) as usize;
                                                        let display_height =
                                                            chunks[0].height.saturating_sub(
                                                                if has_filter { 4 } else { 3 },
                                                            )
                                                                as usize; // Border + table_y + header
                                                        let filter =
                                                            app.input_buffer.trim().to_lowercase();
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

                                                        if relative_row < display_height {
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
                                                            if offset + relative_row < total_rows {
                                                                let target_real_idx =
                                                                    filtered_entries
                                                                        [offset + relative_row];
                                                                app.picker.selected =
                                                                    target_real_idx;
                                                                if is_double_click {
                                                                    let entry = app.picker.entries
                                                                        [target_real_idx]
                                                                        .clone();
                                                                    if entry.is_dir {
                                                                        if entry.is_parent {
                                                                            app.picker.go_parent();
                                                                        } else {
                                                                            app.picker.try_set_cwd(
                                                                                entry.path,
                                                                            );
                                                                        }
                                                                    } else {
                                                                        let conn_clone =
                                                                            conn_mutex.clone();
                                                                        let tx =
                                                                            app.async_tx.clone();
                                                                        let path = entry
                                                                            .path
                                                                            .to_string_lossy()
                                                                            .to_string();
                                                                        tokio::spawn(async move {
                                                                            let session_id =
                                                                                Uuid::new_v4()
                                                                                    .to_string();
                                                                            if let Err(e) = ingest_path(
                                                                                conn_clone,
                                                                                &path,
                                                                                &session_id,
                                                                                None,
                                                                            )
                                                                            .await
                                                                                && let Err(send_err) = tx.send(
                                                                                    AppEvent::Notification(format!(
                                                                                        "Failed to queue file: {}",
                                                                                        e
                                                                                    )),
                                                                                )
                                                                            {
                                                                                warn!(
                                                                                    "Failed to send queue failure notification: {}",
                                                                                    send_err
                                                                                );
                                                                            }
                                                                        });
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    app.focus = AppFocus::Remote;
                                                    if app.s3_objects.is_empty() {
                                                        request_remote_list(&mut app, false).await;
                                                    }

                                                    let inner_y = chunks[1].y + 2;
                                                    if y >= inner_y {
                                                        let relative_row = (y - inner_y) as usize;
                                                        let display_height =
                                                            chunks[1].height.saturating_sub(3)
                                                                as usize;
                                                        let total_rows = app.s3_objects.len();
                                                        let offset = calculate_list_offset(
                                                            app.selected_remote,
                                                            total_rows,
                                                            display_height,
                                                        );
                                                        if relative_row < display_height
                                                            && offset + relative_row < total_rows
                                                        {
                                                            let new_idx = offset + relative_row;
                                                            app.selected_remote = new_idx;

                                                            // Double click to navigate
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

                                                            if is_double_click {
                                                                app.last_click_time = None;
                                                                let obj = &app.s3_objects[new_idx];
                                                                if obj.is_dir {
                                                                    // Handle parent directory navigation
                                                                    if obj.is_parent {
                                                                        let current = app
                                                                            .remote_current_path
                                                                            .trim_end_matches('/');
                                                                        if let Some(idx) =
                                                                            current.rfind('/')
                                                                        {
                                                                            app.remote_current_path
                                                                                .truncate(idx + 1);
                                                                        } else {
                                                                            app.remote_current_path
                                                                                .clear();
                                                                        }
                                                                    } else {
                                                                        app.remote_current_path
                                                                            .push_str(&obj.name);
                                                                    }
                                                                    app.input_mode =
                                                                        InputMode::RemoteBrowsing;
                                                                    // Use cached request
                                                                    request_remote_list(
                                                                        &mut app, false,
                                                                    )
                                                                    .await;
                                                                }
                                                            } else {
                                                                app.last_click_time = Some(now);
                                                                app.last_click_pos = Some((x, y));
                                                            }
                                                        }
                                                    }
                                                }
                                            } else {
                                                app.focus = AppFocus::Queue;
                                                let inner_area = Block::default()
                                                    .borders(
                                                        Borders::LEFT
                                                            | Borders::RIGHT
                                                            | Borders::BOTTOM,
                                                    )
                                                    .inner(queue_area);
                                                let has_filter = !app.queue_search_query.is_empty()
                                                    || app.input_mode == InputMode::QueueSearch;
                                                let table_y =
                                                    inner_area.y + if has_filter { 1 } else { 0 };
                                                let content_y = table_y + 1; // +1 for header

                                                if y >= content_y {
                                                    let relative_row = (y - content_y) as usize;
                                                    let display_height = inner_area
                                                        .height
                                                        .saturating_sub(if has_filter {
                                                            1
                                                        } else {
                                                            0
                                                        })
                                                        .saturating_sub(2)
                                                        as usize;
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
                                            let inner_y = center_layout.y + 1;
                                            if y > inner_y {
                                                let relative_row = (y - inner_y - 1) as usize;
                                                let display_height =
                                                    center_layout.height.saturating_sub(4) as usize;
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
                                            let sidebar_width = 16;
                                            if x < center_layout.x + sidebar_width {
                                                app.focus = AppFocus::SettingsCategory;
                                                let rel_y = y.saturating_sub(center_layout.y + 1);
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
                                                        app.settings.selected_field = 0;
                                                        app.settings.editing = false;
                                                    }
                                                }
                                            } else {
                                                app.focus = AppFocus::SettingsFields;
                                                let fields_area_x = center_layout.x + sidebar_width;

                                                // Handle Theme List Click during editing
                                                if app.settings.editing
                                                    && app.settings.active_category
                                                        == SettingsCategory::Theme
                                                {
                                                    let rel_x = x.saturating_sub(fields_area_x + 1);
                                                    let rel_y =
                                                        y.saturating_sub(center_layout.y + 2);

                                                    let max_width = center_layout
                                                        .width
                                                        .saturating_sub(sidebar_width + 2);
                                                    let max_height =
                                                        center_layout.height.saturating_sub(2);
                                                    let theme_list_width = max_width.min(62);
                                                    let theme_list_height = max_height.clamp(8, 14);

                                                    if rel_x < theme_list_width
                                                        && rel_y < theme_list_height
                                                    {
                                                        // This is a click inside the theme list
                                                        let display_height = theme_list_height
                                                            .saturating_sub(2)
                                                            as usize;
                                                        if rel_y >= 1
                                                            && (rel_y as usize) < display_height + 1
                                                        {
                                                            let inner_rel_y = (rel_y - 1) as usize;
                                                            let total = app.theme_names.len();
                                                            let current_theme =
                                                                app.settings.theme.as_str();

                                                            let mut offset = 0;
                                                            if let Some(idx) = app
                                                                .theme_names
                                                                .iter()
                                                                .position(|&n| n == current_theme)
                                                                && display_height > 0
                                                                && total > display_height
                                                            {
                                                                offset = idx.saturating_sub(
                                                                    display_height / 2,
                                                                );
                                                                if offset + display_height > total {
                                                                    offset = total.saturating_sub(
                                                                        display_height,
                                                                    );
                                                                }
                                                            }

                                                            let target_idx = offset + inner_rel_y;
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
                                                                app.theme = Theme::from_name(
                                                                    &app.settings.theme,
                                                                );

                                                                if is_double_click {
                                                                    app.settings.editing = false;
                                                                    let mut cfg =
                                                                        app.config.lock().await;
                                                                    app.settings
                                                                        .apply_to_config(&mut cfg);
                                                                    let conn =
                                                                        lock_mutex(&conn_mutex)?;
                                                                    if let Err(e) = crate::core::config::save_config_to_db(&conn, &cfg) {
                                                                        app.status_message =
                                                                            format!("Failed to save theme: {}", e);
                                                                    } else {
                                                                        app.status_message =
                                                                            "Theme saved".to_string();
                                                                    }
                                                                    app.last_click_time = None;
                                                                } else {
                                                                    app.last_click_time = Some(now);
                                                                    app.last_click_pos =
                                                                        Some((x, y));
                                                                }
                                                            }
                                                        }
                                                        continue;
                                                    } else {
                                                        // Clicked outside theme list while editing: cancel
                                                        app.settings.editing = false;
                                                        if let Some(orig) =
                                                            app.settings.original_theme.take()
                                                        {
                                                            app.settings.theme = orig;
                                                            app.theme = Theme::from_name(
                                                                &app.settings.theme,
                                                            );
                                                        }
                                                        continue;
                                                    }
                                                }

                                                // Normal Fields selection
                                                let rel_y = y.saturating_sub(center_layout.y + 1);
                                                let clicked_idx = rel_y / 3;
                                                let count =
                                                    app.settings.active_category.field_count();

                                                // Offset calculation for field clicks
                                                let display_height =
                                                    center_layout.height.saturating_sub(2) as usize;
                                                let fields_per_view = display_height / 3;
                                                let mut offset = 0;
                                                if fields_per_view > 0 {
                                                    if app.settings.selected_field
                                                        >= fields_per_view
                                                    {
                                                        offset = app
                                                            .settings
                                                            .selected_field
                                                            .saturating_sub(fields_per_view / 2);
                                                    }
                                                    if count > fields_per_view
                                                        && offset + fields_per_view > count
                                                    {
                                                        offset = count - fields_per_view;
                                                    }
                                                }

                                                let target_idx = offset + clicked_idx as usize;

                                                if target_idx < count {
                                                    app.settings.selected_field = target_idx;
                                                    // Handle boolean toggles on click
                                                    let is_toggle = (app.settings.active_category
                                                        == SettingsCategory::Scanner
                                                        && target_idx == 3)
                                                        || (app.settings.active_category
                                                            == SettingsCategory::Performance
                                                            && target_idx >= 4);

                                                    if is_toggle {
                                                        match (
                                                            app.settings.active_category,
                                                            target_idx,
                                                        ) {
                                                            (SettingsCategory::Scanner, 3) => {
                                                                app.settings.scanner_enabled =
                                                                    !app.settings.scanner_enabled;
                                                            }
                                                            (SettingsCategory::Performance, 4) => {
                                                                app.settings
                                                                    .delete_source_after_upload =
                                                                    !app.settings
                                                                        .delete_source_after_upload;
                                                            }
                                                            (SettingsCategory::Performance, 5) => {
                                                                app.settings.host_metrics_enabled =
                                                                    !app.settings
                                                                        .host_metrics_enabled;
                                                            }
                                                            _ => {}
                                                        }
                                                        let mut cfg = app.config.lock().await;
                                                        app.settings.apply_to_config(&mut cfg);
                                                        let conn = lock_mutex(&conn_mutex)?;
                                                        if let Err(e) =
                                                            crate::core::config::save_config_to_db(
                                                                &conn, &cfg,
                                                            )
                                                        {
                                                            app.status_message = format!(
                                                                "Failed to save settings: {}",
                                                                e
                                                            );
                                                        }
                                                    } else {
                                                        // Double click for other fields
                                                        let now = Instant::now();
                                                        if let (Some(last_time), Some(last_pos)) = (
                                                            app.last_click_time,
                                                            app.last_click_pos,
                                                        ) {
                                                            if now.duration_since(last_time)
                                                                < Duration::from_millis(500)
                                                                && last_pos == (x, y)
                                                            {
                                                                app.settings.editing = true;
                                                                app.last_click_time = None;
                                                            } else {
                                                                app.last_click_time = Some(now);
                                                                app.last_click_pos = Some((x, y));
                                                            }
                                                        } else {
                                                            app.last_click_time = Some(now);
                                                            app.last_click_pos = Some((x, y));
                                                        }

                                                        // Auto-expand theme
                                                        if app.settings.active_category
                                                            == SettingsCategory::Theme
                                                        {
                                                            app.settings.editing = true;
                                                            app.settings.original_theme =
                                                                Some(app.settings.theme.clone());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else if x >= right_layout.x
                                    && x < right_layout.x + right_layout.width
                                {
                                    // Right Panel Click (History)
                                    app.focus = AppFocus::History;
                                    let inner_y = right_layout.y + 1;
                                    let has_filter = !app.history_search_query.is_empty()
                                        || app.input_mode == InputMode::HistorySearch;
                                    let table_y = inner_y + if has_filter { 1 } else { 0 };
                                    let content_y = table_y + 1; // +1 for header

                                    if y >= content_y {
                                        let relative_row = (y - content_y) as usize;
                                        let display_height = right_layout
                                            .height
                                            .saturating_sub(if has_filter { 5 } else { 4 })
                                            as usize;
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
                            } else if mouse.kind == MouseEventKind::ScrollDown
                                || mouse.kind == MouseEventKind::ScrollUp
                            {
                                let is_down = mouse.kind == MouseEventKind::ScrollDown;
                                if x >= rail_layout.x && x < rail_layout.x + rail_layout.width {
                                    if is_down {
                                        app.current_tab = match app.current_tab {
                                            AppTab::Transfers => AppTab::Quarantine,
                                            AppTab::Quarantine => AppTab::Logs,
                                            AppTab::Logs => AppTab::Settings,
                                            AppTab::Settings => AppTab::Transfers,
                                        };
                                    } else {
                                        app.current_tab = match app.current_tab {
                                            AppTab::Transfers => AppTab::Settings,
                                            AppTab::Quarantine => AppTab::Transfers,
                                            AppTab::Logs => AppTab::Quarantine,
                                            AppTab::Settings => AppTab::Logs,
                                        };
                                    }
                                    app.settings.selected_field = 0;
                                } else if x >= center_layout.x
                                    && x < center_layout.x + center_layout.width
                                {
                                    match app.current_tab {
                                        AppTab::Logs => {
                                            if is_down {
                                                if app.logs_scroll + 1 < app.logs.len() {
                                                    app.logs_scroll += 1;
                                                }
                                                if app.logs_scroll + 1 == app.logs.len() {
                                                    app.logs_stick_to_bottom = true;
                                                }
                                            } else {
                                                app.logs_stick_to_bottom = false;
                                                if app.logs_scroll > 0 {
                                                    app.logs_scroll -= 1;
                                                }
                                            }
                                        }
                                        AppTab::Transfers => {
                                            let local_percent =
                                                app.cached_config.local_width_percent;
                                            let vertical_chunks = Layout::default()
                                                .direction(Direction::Vertical)
                                                .constraints([
                                                    Constraint::Min(0),
                                                    Constraint::Length(20),
                                                ])
                                                .split(center_layout);

                                            let top_area = vertical_chunks[0];

                                            if y < top_area.y + top_area.height {
                                                let chunks = Layout::default()
                                                    .direction(Direction::Horizontal)
                                                    .constraints([
                                                        Constraint::Percentage(local_percent),
                                                        Constraint::Percentage(100 - local_percent),
                                                    ])
                                                    .split(top_area);

                                                if x < chunks[0].x + chunks[0].width {
                                                    if is_down {
                                                        app.picker.move_down();
                                                    } else {
                                                        app.picker.move_up();
                                                    }
                                                } else if is_down {
                                                    if app.selected_remote + 1
                                                        < app.s3_objects.len()
                                                    {
                                                        app.selected_remote += 1;
                                                    }
                                                } else if app.selected_remote > 0 {
                                                    app.selected_remote -= 1;
                                                }
                                            } else if is_down {
                                                if app.selected + 1 < app.visual_jobs.len() {
                                                    app.selected += 1;
                                                }
                                            } else if app.selected > 0 {
                                                app.selected -= 1;
                                            }
                                        }
                                        AppTab::Quarantine => {
                                            if is_down {
                                                if app.selected_quarantine + 1
                                                    < app.quarantine.len()
                                                {
                                                    app.selected_quarantine += 1;
                                                }
                                            } else if app.selected_quarantine > 0 {
                                                app.selected_quarantine -= 1;
                                            }
                                        }
                                        AppTab::Settings => {
                                            let sidebar_width = 16;
                                            if x < center_layout.x + sidebar_width {
                                                // Scroll Categories
                                                if is_down {
                                                    app.settings.active_category =
                                                        match app.settings.active_category {
                                                            SettingsCategory::S3 => {
                                                                SettingsCategory::Scanner
                                                            }
                                                            SettingsCategory::Scanner => {
                                                                SettingsCategory::Performance
                                                            }
                                                            SettingsCategory::Performance => {
                                                                SettingsCategory::Theme
                                                            }
                                                            SettingsCategory::Theme => {
                                                                SettingsCategory::S3
                                                            }
                                                        };
                                                } else {
                                                    app.settings.active_category =
                                                        match app.settings.active_category {
                                                            SettingsCategory::S3 => {
                                                                SettingsCategory::Theme
                                                            }
                                                            SettingsCategory::Scanner => {
                                                                SettingsCategory::S3
                                                            }
                                                            SettingsCategory::Performance => {
                                                                SettingsCategory::Scanner
                                                            }
                                                            SettingsCategory::Theme => {
                                                                SettingsCategory::Performance
                                                            }
                                                        };
                                                }
                                                app.settings.selected_field = 0;
                                                app.settings.editing = false;
                                            } else {
                                                // Scroll Fields or Theme Selector
                                                if app.settings.editing
                                                    && app.settings.active_category
                                                        == SettingsCategory::Theme
                                                {
                                                    // Theme Selector Scroll
                                                    let fields_area_x =
                                                        center_layout.x + sidebar_width;
                                                    let rel_x = x.saturating_sub(fields_area_x + 1);
                                                    let rel_y =
                                                        y.saturating_sub(center_layout.y + 2);

                                                    let max_width = center_layout
                                                        .width
                                                        .saturating_sub(sidebar_width + 2);
                                                    let max_height =
                                                        center_layout.height.saturating_sub(2);
                                                    let theme_list_width = max_width.min(62);
                                                    let theme_list_height = max_height.clamp(8, 14);

                                                    if rel_x < theme_list_width
                                                        && rel_y < theme_list_height
                                                    {
                                                        let current_theme =
                                                            app.settings.theme.as_str();
                                                        if let Some(pos) = app
                                                            .theme_names
                                                            .iter()
                                                            .position(|&n| n == current_theme)
                                                        {
                                                            if is_down {
                                                                if pos + 1 < app.theme_names.len() {
                                                                    app.settings.theme = app
                                                                        .theme_names[pos + 1]
                                                                        .to_string();
                                                                    app.theme = Theme::from_name(
                                                                        &app.settings.theme,
                                                                    );
                                                                }
                                                            } else if pos > 0 {
                                                                app.settings.theme = app
                                                                    .theme_names[pos - 1]
                                                                    .to_string();
                                                                app.theme = Theme::from_name(
                                                                    &app.settings.theme,
                                                                );
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    // Normal Fields Scroll
                                                    let count =
                                                        app.settings.active_category.field_count();
                                                    if is_down {
                                                        if app.settings.selected_field + 1 < count {
                                                            app.settings.selected_field += 1;
                                                        }
                                                    } else if app.settings.selected_field > 0 {
                                                        app.settings.selected_field -= 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else if x >= right_layout.x
                                    && x < right_layout.x + right_layout.width
                                {
                                    if is_down {
                                        if app.selected_history + 1 < app.visual_history.len() {
                                            app.selected_history += 1;
                                        }
                                    } else if app.selected_history > 0 {
                                        app.selected_history -= 1;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            if should_quit {
                break;
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
