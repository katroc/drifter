use crate::core::config::Config;

use crate::services::ingest::ingest_path;

use crate::coordinator::ProgressInfo;
use crate::ui::theme::Theme;

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



use ratatui::Terminal;
use rusqlite::Connection;
use std::collections::{HashMap};
use std::io::{self};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::app::settings::{SettingsState, SettingsCategory};
use crate::app::state::{
    App, ModalAction, ViewMode, InputMode, LayoutTarget, HistoryFilter,
    AppTab, AppFocus, AppEvent,
};
use crate::components::file_picker::PickerView;
use crate::components::wizard::{WizardState, WizardStep};
use crate::ui::util::{calculate_list_offset, fuzzy_match};
use crate::utils::lock_mutex;



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
    )?;
    app.show_wizard = needs_wizard;

    {
        let conn = lock_mutex(&conn_mutex)?;
        app.refresh_jobs(&conn)?;
    }

    // Start Log Watcher
    let log_tx = app.async_tx.clone();
    let log_path = PathBuf::from("debug.log");
    crate::services::log_watcher::LogWatcher::new(log_path, log_tx).start();


    let tick_rate = Duration::from_millis(200);
    loop {
        // Refresh metrics periodically (e.g. every 1s) if enabled
        if lock_mutex(&app.config)?.host_metrics_enabled
            && app.last_metrics_refresh.elapsed() >= Duration::from_secs(1)
        {
                app.last_metrics = app.metrics.refresh();
                app.last_metrics_refresh = Instant::now();
        }

        terminal.draw(|f| crate::ui::ui(f, &app))?;

        if app.last_refresh.elapsed() > Duration::from_secs(1) {
            let conn = lock_mutex(&conn_mutex)?;
            app.refresh_jobs(&conn)?;
        }

        // Check for async messages (e.g. connection tests, S3 lists)
        while let Ok(event) = app.async_rx.try_recv() {
            match event {
                AppEvent::Notification(msg) => app.status_message = msg,
                AppEvent::RemoteFileList(files) => {
                    app.s3_objects = files;
                    app.status_message = format!("Loaded {} items from S3", app.s3_objects.len());
                }
                AppEvent::LogLine(line) => {
                    app.logs.push_back(line);
                    if app.logs.len() > 1000 {
                         app.logs.pop_front();
                    }
                }
            }
        }

        if app.watch_enabled && app.last_watch_scan.elapsed() > Duration::from_secs(2) {
            let _conn = lock_mutex(&conn_mutex)?;
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
                                let conn = lock_mutex(&conn_mutex)?;
                                let cfg_guard = lock_mutex(&app.config)?;
                                if let Err(e) = crate::config::save_config_to_db(
                                    &conn,
                                    &cfg_guard,
                                ) {
                                    app.status_message = format!("Save error: {}", e);
                                } else {
                                    app.status_message = "Layout saved".to_string();
                                }
                                app.input_mode = InputMode::Normal;
                                app.layout_adjust_target = None;
                            }
                            KeyCode::Esc | KeyCode::Char('q') => {
                                let conn = lock_mutex(&conn_mutex)?;
                                if let Ok(loaded_cfg) = crate::config::load_config_from_db(&conn) {
                                    *lock_mutex(&app.config)? = loaded_cfg;
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
                                        let conn = lock_mutex(&conn_mutex)?;
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
                                            lock_mutex(&app.cancellation_tokens)?.get(&id)
                                        {
                                            token.store(true, Ordering::Relaxed);
                                        }

                                        // Update DB status (Soft Delete / Cancel)
                                        let conn = lock_mutex(&conn_mutex)?;
                                        let _ = crate::db::cancel_job(&conn, id);
                                        app.status_message = format!("Cancelled job {}", id);

                                        // Refresh to update list
                                        let _ = app.refresh_jobs(&conn);
                                    }
                                    ModalAction::DeleteRemoteObject(key) => {
                                        app.status_message = format!("Deleting {}...", key);
                                        let tx = app.async_tx.clone();
                                        let config_clone = lock_mutex(&app.config)?.clone();
                                        
                                        std::thread::spawn(move || {
                                            let rt = match tokio::runtime::Runtime::new() {
                                                Ok(rt) => rt,
                                                Err(e) => { let _ = tx.send(AppEvent::Notification(format!("(Runtime) {}", e))); return; }
                                            };
                                            let res = rt.block_on(async {
                                                crate::services::uploader::Uploader::delete_file(&config_clone, &key).await
                                            });
                                            match res {
                                                Ok(_) => {
                                                    let _ = tx.send(AppEvent::Notification(format!("Deleted {}", key)));
                                                    // Trigger Refresh
                                                    let rt = tokio::runtime::Runtime::new().unwrap();
                                                    let res = rt.block_on(async {
                                                        crate::services::uploader::Uploader::list_bucket_contents(&config_clone, None).await
                                                    });
                                                    if let Ok(files) = res {
                                                         let _ = tx.send(AppEvent::RemoteFileList(files));
                                                    }
                                                }
                                                Err(e) => {
                                                    let _ = tx.send(AppEvent::Notification(format!("Delete Failed: {}", e)));
                                                }
                                            }
                                        });
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
                                    let conn = lock_mutex(&conn_mutex)?;
                                    let cfg = Config {
                                        staging_dir: app.wizard.staging_dir.clone(),
                                        quarantine_dir: app.wizard.quarantine_dir.clone(),
                                        clamd_host: app.wizard.clamd_host.clone(),
                                        clamd_port: app.wizard.clamd_port.parse().unwrap_or(3310),
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
                                        part_size_mb: app.wizard.part_size.parse().unwrap_or(64),
                                        concurrency_upload_global: app.wizard.concurrency.parse().unwrap_or(4),
                                        concurrency_parts_per_file: 4,
                                        ..Config::default()
                                    };

                                    let _ = crate::config::save_config_to_db(&conn, &cfg);
                                    drop(conn);

                                    // Create directories if they don't exist
                                    let _ = std::fs::create_dir_all(&cfg.staging_dir);
                                    let _ = std::fs::create_dir_all(&cfg.quarantine_dir);
                                    let _ = std::fs::create_dir_all(&cfg.state_dir);

                                    // Update shared config
                                    let mut shared_cfg = lock_mutex(&app.config)?;
                                    *shared_cfg = cfg;
                                    drop(shared_cfg);

                                    // Reload settings state
                                    let cfg_guard = lock_mutex(&app.config)?;
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
                        AppFocus::Logs => app.input_mode == InputMode::LogSearch,
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
                            KeyCode::Char('q') if app.focus != AppFocus::Logs => break,
                            KeyCode::Tab => {
                                app.focus = match app.focus {
                                    AppFocus::Rail => match app.current_tab {
                                        AppTab::Transfers => AppFocus::Browser,
                                        AppTab::Quarantine => AppFocus::Quarantine,
                                        AppTab::Remote => AppFocus::Remote,
                                        AppTab::Logs => AppFocus::Logs,
                                        AppTab::Settings => AppFocus::SettingsCategory,
                                    },
                                    AppFocus::Browser => AppFocus::Queue,
                                    AppFocus::Queue => AppFocus::History,
                                    AppFocus::SettingsCategory => AppFocus::SettingsFields,
                                    AppFocus::History
                                    | AppFocus::Quarantine
                                    | AppFocus::Remote
                                    | AppFocus::Logs
                                    | AppFocus::SettingsFields => AppFocus::Rail,
                                };
                            }
                            KeyCode::BackTab => {
                                app.focus = match app.focus {
                                    AppFocus::Rail => match app.current_tab {
                                        AppTab::Transfers => AppFocus::History,
                                        AppTab::Quarantine => AppFocus::Quarantine,
                                        AppTab::Remote => AppFocus::Remote,
                                        AppTab::Logs => AppFocus::Logs,
                                        AppTab::Settings => AppFocus::SettingsFields,
                                    },
                                    AppFocus::History => AppFocus::Queue,
                                    AppFocus::Queue => AppFocus::Browser,
                                    AppFocus::SettingsFields => AppFocus::SettingsCategory,
                                    AppFocus::Browser
                                    | AppFocus::Quarantine
                                    | AppFocus::Remote
                                    | AppFocus::SettingsCategory | AppFocus::Logs => AppFocus::Rail,
                                };
                            }
                            KeyCode::Right => {
                                app.focus = match app.focus {
                                    AppFocus::Rail => match app.current_tab {
                                        AppTab::Transfers => AppFocus::Browser,
                                        AppTab::Quarantine => AppFocus::Quarantine,
                                        AppTab::Logs => AppFocus::Logs,
                                        AppTab::Remote => AppFocus::Remote,
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
                                    | AppFocus::Remote
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
                                        AppTab::Remote => AppFocus::Remote,
                                        AppTab::Logs => AppFocus::Logs,
                                        AppTab::Settings => AppFocus::SettingsCategory,
                                    };
                                }
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
                                    AppTab::Remote => AppTab::Quarantine,
                                    AppTab::Logs => AppTab::Remote,
                                    AppTab::Settings => AppTab::Logs,

                                };
                                if app.current_tab == AppTab::Remote {
                                    // Auto-refresh
                                    let tx = app.async_tx.clone();
                                    let config_clone = lock_mutex(&app.config)?.clone();
                                    std::thread::spawn(move || {
                                        let rt = match tokio::runtime::Runtime::new() {
                                            Ok(rt) => rt,
                                            Err(e) => { let _ = tx.send(AppEvent::Notification(format!("(Runtime) {}", e))); return; }
                                        };
                                        let res = rt.block_on(async {
                                            crate::services::uploader::Uploader::list_bucket_contents(&config_clone, None).await
                                        });
                                        match res {
                                            Ok(files) => { let _ = tx.send(AppEvent::RemoteFileList(files)); }
                                            Err(e) => { let _ = tx.send(AppEvent::Notification(format!("List Failed: {}", e))); }
                                        }
                                    });
                                }
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                app.current_tab = match app.current_tab {
                                    AppTab::Transfers => AppTab::Quarantine,
                                    AppTab::Quarantine => AppTab::Remote,
                                    AppTab::Remote => AppTab::Logs,
                                    AppTab::Logs => AppTab::Settings,
                                    AppTab::Settings => AppTab::Transfers,
                                };
                                if app.current_tab == AppTab::Remote {
                                    // Auto-refresh
                                    let tx = app.async_tx.clone();
                                    let config_clone = lock_mutex(&app.config)?.clone();
                                    std::thread::spawn(move || {
                                        let rt = match tokio::runtime::Runtime::new() {
                                            Ok(rt) => rt,
                                            Err(e) => { let _ = tx.send(AppEvent::Notification(format!("(Runtime) {}", e))); return; }
                                        };
                                        let res = rt.block_on(async {
                                            crate::services::uploader::Uploader::list_bucket_contents(&config_clone, None).await
                                        });
                                        match res {
                                            Ok(files) => { let _ = tx.send(AppEvent::RemoteFileList(files)); }
                                            Err(e) => { let _ = tx.send(AppEvent::Notification(format!("List Failed: {}", e))); }
                                        }
                                    });
                                }
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
                                                    let conn = lock_mutex(&conn_mutex)?;
                                                    let cfg_guard = lock_mutex(&cfg)?;
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
                                                let conn = lock_mutex(&conn_mutex)?;
                                                let cfg_guard = lock_mutex(&cfg)?;
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
                                InputMode::LogSearch => {}
                                InputMode::Confirmation => {}
                                InputMode::LayoutAdjust => {},

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
                                    let conn = lock_mutex(&conn_mutex)?;
                                    let _ = app.refresh_jobs(&conn);
                                }
                                KeyCode::Char('R') => {
                                    let conn = lock_mutex(&conn_mutex)?;
                                    let _ = app.refresh_jobs(&conn);
                                }
                                KeyCode::Char('r') => {
                                    if !app.visual_jobs.is_empty()
                                        && app.selected < app.visual_jobs.len()
                                        && let Some(idx) = app.visual_jobs[app.selected].index_in_jobs
                                    {
                                            let id = app.jobs[idx].id;
                                            let conn = lock_mutex(&conn_mutex)?;
                                            let _ = crate::db::retry_job(&conn, id);
                                            app.status_message = format!("Retried job {}", id);
                                        }

                                }
                                KeyCode::Char('d') | KeyCode::Delete => {
                                    if !app.visual_jobs.is_empty()
                                        && app.selected < app.visual_jobs.len()
                                        && let Some(idx) = app.visual_jobs[app.selected].index_in_jobs
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
                                                let conn = lock_mutex(&conn_mutex)?;
                                                let _ = crate::db::cancel_job(&conn, id);
                                                app.status_message = format!("Removed job {}", id);
                                                let _ = app.refresh_jobs(&conn);
                                                // Selection adjustment happens in refresh_jobs (auto-fix) or we can decrement if at end
                                                // But standard behavior is fine.
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
                                    let conn = lock_mutex(&conn_mutex)?;
                                    let _ = app.refresh_jobs(&conn);
                                }
                                KeyCode::Char('f') => {
                                    app.history_filter = app.history_filter.next();
                                    app.selected_history = 0; // Reset selection
                                    app.status_message =
                                        format!("History Filter: {}", app.history_filter.as_str());
                                    let conn = lock_mutex(&conn_mutex)?;
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
                                        && let Some(idx) = app.visual_history[app.selected_history].index_in_jobs
                                    {
                                            let job = &app.history[idx];
                                            let id = job.id;
                                            let conn = lock_mutex(&conn_mutex)?;
                                            let _ = crate::db::delete_job(&conn, id);
                                            let _ = app.refresh_jobs(&conn);
                                            // Auto-fix happens in refresh_jobs

                                    }
                                }
                                KeyCode::Char('R') => {
                                    let conn = lock_mutex(&conn_mutex)?;
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

                                    let conn = lock_mutex(&conn_mutex)?;
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
                                let conn = lock_mutex(&conn_mutex)?;
                                let _ = app.refresh_jobs(&conn);
                            }
                            _ => {}
                        },
                        AppFocus::Remote => match key.code {
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
                            KeyCode::Char('r') => {
                                app.status_message = "Refreshing S3 list...".to_string();
                                let tx = app.async_tx.clone();
                                let config_clone = lock_mutex(&app.config)?.clone();
                                std::thread::spawn(move || {
                                    let rt = match tokio::runtime::Runtime::new() {
                                        Ok(rt) => rt,
                                        Err(e) => {
                                            let _ = tx.send(AppEvent::Notification(format!("(Runtime) {}", e)));
                                            return;
                                        }
                                    };
                                    let res = rt.block_on(async {
                                        crate::services::uploader::Uploader::list_bucket_contents(&config_clone, None).await
                                    });
                                    match res {
                                        Ok(files) => {
                                            let _ = tx.send(AppEvent::RemoteFileList(files));
                                        }
                                        Err(e) => {
                                            let _ = tx.send(AppEvent::Notification(format!("List Failed: {}", e)));
                                        }
                                    }
                                });
                            }
                            KeyCode::Char('d') => {
                                if !app.s3_objects.is_empty() && app.selected_remote < app.s3_objects.len() {
                                    let key = app.s3_objects[app.selected_remote].key.clone();
                                    app.status_message = format!("Downloading {}...", key);
                                    let tx = app.async_tx.clone();
                                    let config_clone = lock_mutex(&app.config)?.clone();
                                    
                                    // Default download location: ~/Downloads/Drifter or similar? 
                                    // For now, let's just download to current working dir or ~/Downloads
                                    // Let's assume ~/Downloads.
                                    let download_dir = dirs::download_dir().unwrap_or(PathBuf::from("."));
                                    let dest = download_dir.join(std::path::Path::new(&key).file_name().unwrap_or(std::ffi::OsStr::new("downloaded_file")));
                                    let dest_clone = dest.clone();

                                    std::thread::spawn(move || {
                                        let rt = match tokio::runtime::Runtime::new() {
                                            Ok(rt) => rt,
                                            Err(e) => { let _ = tx.send(AppEvent::Notification(format!("(Runtime) {}", e))); return; }
                                        };
                                        let res = rt.block_on(async {
                                            crate::services::uploader::Uploader::download_file(&config_clone, &key, &dest_clone).await
                                        });
                                        match res {
                                            Ok(_) => {
                                                let _ = tx.send(AppEvent::Notification(format!("Downloaded to {:?}", dest_clone)));
                                            }
                                            Err(e) => {
                                                let _ = tx.send(AppEvent::Notification(format!("Download Failed: {}", e)));
                                            }
                                        }
                                    });
                                }
                            }
                             KeyCode::Char('x') => {
                                // Delete
                                if !app.s3_objects.is_empty() && app.selected_remote < app.s3_objects.len() {
                                    let key = app.s3_objects[app.selected_remote].key.clone();
                                    app.input_mode = InputMode::Confirmation;
                                    app.pending_action = ModalAction::DeleteRemoteObject(key.clone());
                                    app.confirmation_msg = format!("Delete '{}'?", key);
                                }
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
                                    let mut cfg = lock_mutex(&app.config)?;
                                    app.settings.apply_to_config(&mut cfg);

                                    // Apply theme immediately
                                    app.theme = Theme::from_name(&cfg.theme);

                                    // Save entire config to database
                                    let conn = lock_mutex(&conn_mutex)?;
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
                                        let mut cfg = lock_mutex(&app.config)?;
                                        app.settings.apply_to_config(&mut cfg);
                                        let conn = lock_mutex(&conn_mutex)?;
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

                                        let mut cfg = lock_mutex(&app.config)?;
                                        app.settings.apply_to_config(&mut cfg);

                                        // Apply theme immediately
                                        app.theme = Theme::from_name(&cfg.theme);

                                        // Save entire config to database
                                        let conn = lock_mutex(&conn_mutex)?;
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
                                        || app.settings.active_category == SettingsCategory::Theme =>
                                {
                                    // If saving while editing theme, exit edit mode and commit
                                    if app.settings.editing
                                        && app.settings.active_category == SettingsCategory::Theme
                                    {
                                        app.settings.editing = false;
                                        app.settings.original_theme = None;
                                    }

                                    let mut cfg = lock_mutex(&app.config)?;
                                    app.settings.apply_to_config(&mut cfg);

                                    // Apply theme immediately
                                    app.theme = Theme::from_name(&cfg.theme);

                                    // Save entire config to database
                                    let conn = lock_mutex(&conn_mutex)?;
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
                                        let config_clone = lock_mutex(&app.config)?.clone();

                                        std::thread::spawn(move || {
                                            let rt = match tokio::runtime::Runtime::new() {
                                                Ok(rt) => rt,
                                                Err(e) => {
                                                    let _ = tx.send(AppEvent::Notification(format!("Runtime error: {}", e)));
                                                    return;
                                                }
                                            };
                                            let res = rt.block_on(async {
                                                if cat == SettingsCategory::S3 {
                                                    crate::services::uploader::Uploader::check_connection(
                                                        &config_clone,
                                                    )
                                                    .await
                                                } else {
                                                    crate::services::scanner::Scanner::new(&config_clone)
                                                        .check_connection()
                                                        .await
                                                }
                                            });

                                            let msg = match res {
                                                Ok(s) => s,
                                                Err(e) => format!("Connection Failed: {}", e),
                                            };
                                            let _ = tx.send(AppEvent::Notification(msg));
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
                                                app.status_message = format!("Found {} matches", app.log_search_results.len());
                                            } else {
                                                app.status_message = "No matches found".to_string();
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
                                                app.log_search_current = (app.log_search_current + 1) % app.log_search_results.len();
                                                app.logs_scroll = app.log_search_results[app.log_search_current];
                                                app.logs_stick_to_bottom = false;
                                                app.status_message = format!("Match {}/{}", app.log_search_current + 1, app.log_search_results.len());
                                            }
                                        }
                                        KeyCode::Char('N') => {
                                            if !app.log_search_results.is_empty() {
                                                if app.log_search_current == 0 {
                                                    app.log_search_current = app.log_search_results.len() - 1;
                                                } else {
                                                    app.log_search_current -= 1;
                                                }
                                                app.logs_scroll = app.log_search_results[app.log_search_current];
                                                app.logs_stick_to_bottom = false;
                                                app.status_message = format!("Match {}/{}", app.log_search_current + 1, app.log_search_results.len());
                                            }
                                        }
                                        KeyCode::Char('g') | KeyCode::Home => {
                                            app.logs_scroll = 0;
                                            app.logs_stick_to_bottom = false;
                                        }
                                        KeyCode::Char('G') | KeyCode::End => {
                                            app.logs_stick_to_bottom = true;
                                            if !app.logs.is_empty() {
                                                app.logs_scroll = app.logs.len().saturating_sub(1);
                                            }
                                        }
                                        KeyCode::PageUp => {
                                            app.logs_stick_to_bottom = false;
                                            app.logs_scroll = app.logs_scroll.saturating_sub(15);
                                        }
                                        KeyCode::PageDown => {
                                            if app.logs_scroll + 15 < app.logs.len() {
                                                app.logs_scroll += 15;
                                            } else {
                                                app.logs_stick_to_bottom = true;
                                                if !app.logs.is_empty() {
                                                    app.logs_scroll = app.logs.len().saturating_sub(1);
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
                                                app.status_message = "Search cleared".to_string();
                                            } else {
                                                app.focus = AppFocus::Rail;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
                Event::Mouse(mouse) => {
                    let cfg_guard = if let Ok(c) = lock_mutex(&app.config) { c } else { continue };
                    let history_width = cfg_guard.history_width;
                    drop(cfg_guard);

                    if let Ok(size) = terminal.size() {
                        let main_layout = Layout::default()
                            .direction(Direction::Horizontal)
                            .constraints([
                                Constraint::Length(14),
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
                                let rel_ry = y.saturating_sub(rail_layout.y);
                                match rel_ry {
                                    1 => { app.current_tab = AppTab::Transfers; app.focus = AppFocus::Rail; }
                                    2 => { app.current_tab = AppTab::Quarantine; app.focus = AppFocus::Rail; }
                                    3 => {
                                        app.current_tab = AppTab::Remote;
                                        app.focus = AppFocus::Rail;
                                        let tx = app.async_tx.clone();
                                        let config_clone = lock_mutex(&app.config)?.clone();
                                        std::thread::spawn(move || {
                                            let rt = match tokio::runtime::Runtime::new() {
                                                Ok(rt) => rt,
                                                Err(e) => { let _ = tx.send(AppEvent::Notification(format!("(Runtime) {}", e))); return; }
                                            };
                                            let res = rt.block_on(async {
                                                crate::services::uploader::Uploader::list_bucket_contents(&config_clone, None).await
                                            });
                                            match res {
                                                Ok(files) => { let _ = tx.send(AppEvent::RemoteFileList(files)); }
                                                Err(e) => { let _ = tx.send(AppEvent::Notification(format!("List Failed: {}", e))); }
                                            }
                                        });
                                    }
                                    4 => { app.current_tab = AppTab::Logs; app.focus = AppFocus::Rail; }
                                    5 => { app.current_tab = AppTab::Settings; app.focus = AppFocus::Rail; }
                                    _ => {}
                                }
                            } else if x >= center_layout.x && x < center_layout.x + center_layout.width {
                                // Center Click
                                match app.current_tab {
                                    AppTab::Logs => { app.focus = AppFocus::Logs; }
                                    AppTab::Remote => {
                                        app.focus = AppFocus::Remote;
                                        let inner_y = center_layout.y + 1;
                                        if y > inner_y {
                                            let relative_row = (y - inner_y - 1) as usize;
                                            let display_height = center_layout.height.saturating_sub(2) as usize;
                                            let total_rows = app.s3_objects.len();
                                            let offset = calculate_list_offset(app.selected_remote, total_rows, display_height);
                                            if relative_row < display_height && offset + relative_row < total_rows {
                                                app.selected_remote = offset + relative_row;
                                            }
                                        }
                                    }
                                    AppTab::Transfers => {
                                        let hopper_percent = lock_mutex(&app.config).map(|c| c.hopper_width_percent).unwrap_or(50);
                                        let chunks = Layout::default()
                                            .direction(Direction::Horizontal)
                                            .constraints([
                                                Constraint::Percentage(hopper_percent),
                                                Constraint::Percentage(100 - hopper_percent),
                                            ])
                                            .split(center_layout);

                                        if x < chunks[0].x + chunks[0].width {
                                            app.focus = AppFocus::Browser;
                                            let now = Instant::now();
                                            let is_double_click = if let (Some(last_time), Some(last_pos)) = (app.last_click_time, app.last_click_pos) {
                                                now.duration_since(last_time) < Duration::from_millis(500) && last_pos == (x, y)
                                            } else { false };

                                            if is_double_click {
                                                app.last_click_time = None;
                                                if app.input_mode == InputMode::Normal { app.input_mode = InputMode::Browsing; }
                                            } else {
                                                app.last_click_time = Some(now);
                                                app.last_click_pos = Some((x, y));
                                            }

                                            let inner_y = chunks[0].y + 1;
                                            let has_filter = !app.input_buffer.is_empty() || app.input_mode == InputMode::Filter;
                                            let table_y = inner_y + if has_filter { 2 } else { 1 };
                                            let content_y = table_y + 1; // +1 for header

                                            if y >= content_y {
                                                let relative_row = (y - content_y) as usize;
                                                let display_height = chunks[0].height.saturating_sub(if has_filter { 4 } else { 3 }) as usize; // Border + table_y + header
                                                let filter = app.input_buffer.trim().to_lowercase();
                                                let filtered_entries: Vec<usize> = app.picker.entries.iter().enumerate()
                                                    .filter(|(_, e)| e.is_parent || filter.is_empty() || fuzzy_match(&filter, &e.name))
                                                    .map(|(i, _)| i).collect();

                                                if relative_row < display_height {
                                                    let total_rows = filtered_entries.len();
                                                    let current_filtered_selected = filtered_entries.iter().position(|&i| i == app.picker.selected).unwrap_or(0);
                                                    let offset = calculate_list_offset(current_filtered_selected, total_rows, display_height);
                                                    if offset + relative_row < total_rows {
                                                        let target_real_idx = filtered_entries[offset + relative_row];
                                                        app.picker.selected = target_real_idx;
                                                        if is_double_click {
                                                            let entry = app.picker.entries[target_real_idx].clone();
                                                            if entry.is_dir {
                                                                if entry.is_parent { app.picker.go_parent(); }
                                                                else { app.picker.try_set_cwd(entry.path); }
                                                            } else {
                                                                let conn = lock_mutex(&conn_mutex)?;
                                                                let cfg = lock_mutex(&app.config)?;
                                                                let _ = ingest_path(&conn, &cfg.staging_dir, &cfg.staging_mode, &entry.path.to_string_lossy());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        } else {
                                            app.focus = AppFocus::Queue;
                                            let inner_y = chunks[1].y + 1;
                                            let content_y = inner_y + 1; // +1 for header
                                            if y >= content_y {
                                                let relative_row = (y - content_y) as usize;
                                                let display_height = chunks[1].height.saturating_sub(4) as usize; // Match render.rs saturating_sub(4)
                                                let total_rows = app.visual_jobs.len();
                                                let offset = calculate_list_offset(app.selected, total_rows, display_height);
                                                if relative_row < display_height && offset + relative_row < total_rows {
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
                                            let display_height = center_layout.height.saturating_sub(4) as usize;
                                            let total_rows = app.quarantine.len();
                                            let offset = calculate_list_offset(app.selected_quarantine, total_rows, display_height);
                                            if relative_row < display_height && offset + relative_row < total_rows {
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
                                            if app.settings.editing && app.settings.active_category == SettingsCategory::Theme {
                                                let rel_x = x.saturating_sub(fields_area_x + 1);
                                                let rel_y = y.saturating_sub(center_layout.y + 2);
                                                
                                                let max_width = center_layout.width.saturating_sub(sidebar_width + 2);
                                                let max_height = center_layout.height.saturating_sub(2);
                                                let theme_list_width = max_width.min(62);
                                                let theme_list_height = max_height.clamp(8, 14);
                                                
                                                if rel_x < theme_list_width && rel_y < theme_list_height {
                                                    // This is a click inside the theme list
                                                    let display_height = theme_list_height.saturating_sub(2) as usize;
                                                    if rel_y >= 1 && (rel_y as usize) < display_height + 1 {
                                                        let inner_rel_y = (rel_y - 1) as usize;
                                                        let total = app.theme_names.len();
                                                        let current_theme = app.settings.theme.as_str();
                                                        
                                                        let mut offset = 0;
                                                        if let Some(idx) = app.theme_names.iter().position(|&n| n == current_theme) {
                                                            if display_height > 0 && total > display_height {
                                                                offset = idx.saturating_sub(display_height / 2);
                                                                if offset + display_height > total {
                                                                    offset = total.saturating_sub(display_height);
                                                                }
                                                            }
                                                        }
                                                        
                                                        let target_idx = offset + inner_rel_y;
                                                        if target_idx < total {
                                                            let now = Instant::now();
                                                            let is_double_click = if let (Some(last_time), Some(last_pos)) = (app.last_click_time, app.last_click_pos) {
                                                                now.duration_since(last_time) < Duration::from_millis(500) && last_pos == (x, y)
                                                            } else { false };
                                                            
                                                            app.settings.theme = app.theme_names[target_idx].to_string();
                                                            app.theme = Theme::from_name(&app.settings.theme);
                                                            
                                                            if is_double_click {
                                                                app.settings.editing = false;
                                                                let mut cfg = lock_mutex(&app.config)?;
                                                                app.settings.apply_to_config(&mut cfg);
                                                                let conn = lock_mutex(&conn_mutex)?;
                                                                let _ = crate::config::save_config_to_db(&conn, &cfg);
                                                                app.status_message = "Theme saved".to_string();
                                                                app.last_click_time = None;
                                                            } else {
                                                                app.last_click_time = Some(now);
                                                                app.last_click_pos = Some((x, y));
                                                            }
                                                        }
                                                    }
                                                    continue;
                                                } else {
                                                    // Clicked outside theme list while editing: cancel
                                                    app.settings.editing = false;
                                                    if let Some(orig) = app.settings.original_theme.take() {
                                                        app.settings.theme = orig;
                                                        app.theme = Theme::from_name(&app.settings.theme);
                                                    }
                                                    continue;
                                                }
                                            }

                                            // Normal Fields selection
                                            let rel_y = y.saturating_sub(center_layout.y + 1);
                                            let clicked_idx = rel_y / 3;
                                            let count = match app.settings.active_category {
                                                SettingsCategory::S3 => 6,
                                                SettingsCategory::Scanner => 4,
                                                SettingsCategory::Performance => 6,
                                                SettingsCategory::Theme => 1,
                                            };
                                            
                                            // Offset calculation for field clicks
                                            let display_height = center_layout.height.saturating_sub(2) as usize;
                                            let fields_per_view = display_height / 3;
                                            let mut offset = 0;
                                            if fields_per_view > 0 {
                                                if app.settings.selected_field >= fields_per_view {
                                                    offset = app.settings.selected_field.saturating_sub(fields_per_view / 2);
                                                }
                                                if count > fields_per_view && offset + fields_per_view > count {
                                                    offset = count - fields_per_view;
                                                }
                                            }
                                            
                                            let target_idx = offset + clicked_idx as usize;

                                            if target_idx < count {
                                                app.settings.selected_field = target_idx;
                                                // Handle boolean toggles on click
                                                let is_toggle = (app.settings.active_category == SettingsCategory::Scanner && target_idx == 3)
                                                    || (app.settings.active_category == SettingsCategory::Performance && target_idx >= 3);
                                                
                                                if is_toggle {
                                                     match (app.settings.active_category, target_idx) {
                                                        (SettingsCategory::Scanner, 3) => { app.settings.scanner_enabled = !app.settings.scanner_enabled; }
                                                        (SettingsCategory::Performance, 3) => { app.settings.staging_mode_direct = !app.settings.staging_mode_direct; }
                                                        (SettingsCategory::Performance, 4) => { app.settings.delete_source_after_upload = !app.settings.delete_source_after_upload; }
                                                        (SettingsCategory::Performance, 5) => { app.settings.host_metrics_enabled = !app.settings.host_metrics_enabled; }
                                                        _ => {}
                                                     }
                                                     let mut cfg = lock_mutex(&app.config)?;
                                                     app.settings.apply_to_config(&mut cfg);
                                                     let conn = lock_mutex(&conn_mutex)?;
                                                     let _ = crate::config::save_config_to_db(&conn, &cfg);
                                                } else {
                                                    // Double click for other fields
                                                    let now = Instant::now();
                                                    if let (Some(last_time), Some(last_pos)) = (app.last_click_time, app.last_click_pos) {
                                                        if now.duration_since(last_time) < Duration::from_millis(500) && last_pos == (x, y) {
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
                                                    if app.settings.active_category == SettingsCategory::Theme {
                                                        app.settings.editing = true;
                                                        app.settings.original_theme = Some(app.settings.theme.clone());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            } else if x >= right_layout.x && x < right_layout.x + right_layout.width {
                                // Right Panel Click (History)
                                app.focus = AppFocus::History;
                                let inner_y = right_layout.y + 1;
                                if y > inner_y {
                                    let relative_row = (y - inner_y - 1) as usize;
                                    let display_height = right_layout.height.saturating_sub(4) as usize;
                                    let total_rows = app.visual_history.len();
                                    let offset = calculate_list_offset(app.selected_history, total_rows, display_height);
                                    if relative_row < display_height && offset + relative_row < total_rows {
                                        app.selected_history = offset + relative_row;
                                    }
                                }
                            }
                        } else if mouse.kind == MouseEventKind::ScrollDown || mouse.kind == MouseEventKind::ScrollUp {
                            let is_down = mouse.kind == MouseEventKind::ScrollDown;
                            if x >= rail_layout.x && x < rail_layout.x + rail_layout.width {
                                if is_down {
                                    app.current_tab = match app.current_tab {
                                        AppTab::Transfers => AppTab::Quarantine,
                                        AppTab::Quarantine => AppTab::Remote,
                                        AppTab::Remote => AppTab::Logs,
                                        AppTab::Logs => AppTab::Settings,
                                        AppTab::Settings => AppTab::Transfers,
                                    };
                                } else {
                                    app.current_tab = match app.current_tab {
                                        AppTab::Transfers => AppTab::Settings,
                                        AppTab::Quarantine => AppTab::Transfers,
                                        AppTab::Remote => AppTab::Quarantine,
                                        AppTab::Logs => AppTab::Remote,
                                        AppTab::Settings => AppTab::Logs,
                                    };
                                }
                                app.settings.selected_field = 0;
                            } else if x >= center_layout.x && x < center_layout.x + center_layout.width {
                                match app.current_tab {
                                    AppTab::Logs => {
                                        if is_down {
                                            if app.logs_scroll + 1 < app.logs.len() { app.logs_scroll += 1; }
                                            if app.logs_scroll + 1 == app.logs.len() { app.logs_stick_to_bottom = true; }
                                        } else {
                                            app.logs_stick_to_bottom = false;
                                            if app.logs_scroll > 0 { app.logs_scroll -= 1; }
                                        }
                                    }
                                    AppTab::Transfers => {
                                        let hopper_percent = lock_mutex(&app.config).map(|c| c.hopper_width_percent).unwrap_or(50);
                                        let chunks = Layout::default()
                                            .direction(Direction::Horizontal)
                                            .constraints([
                                                Constraint::Percentage(hopper_percent),
                                                Constraint::Percentage(100 - hopper_percent),
                                            ])
                                            .split(center_layout);
                                        if x < chunks[0].x + chunks[0].width {
                                            if is_down { app.picker.move_down(); } else { app.picker.move_up(); }
                                        } else {
                                            if is_down { if app.selected + 1 < app.visual_jobs.len() { app.selected += 1; } }
                                            else { if app.selected > 0 { app.selected -= 1; } }
                                        }
                                    }
                                    AppTab::Quarantine => {
                                        if is_down { if app.selected_quarantine + 1 < app.quarantine.len() { app.selected_quarantine += 1; } }
                                        else { if app.selected_quarantine > 0 { app.selected_quarantine -= 1; } }
                                    }
                                    AppTab::Settings => {
                                        let sidebar_width = 16;
                                        if x < center_layout.x + sidebar_width {
                                            // Scroll Categories
                                            if is_down {
                                                app.settings.active_category = match app.settings.active_category {
                                                    SettingsCategory::S3 => SettingsCategory::Scanner,
                                                    SettingsCategory::Scanner => SettingsCategory::Performance,
                                                    SettingsCategory::Performance => SettingsCategory::Theme,
                                                    SettingsCategory::Theme => SettingsCategory::S3,
                                                };
                                            } else {
                                                app.settings.active_category = match app.settings.active_category {
                                                    SettingsCategory::S3 => SettingsCategory::Theme,
                                                    SettingsCategory::Scanner => SettingsCategory::S3,
                                                    SettingsCategory::Performance => SettingsCategory::Scanner,
                                                    SettingsCategory::Theme => SettingsCategory::Performance,
                                                };
                                            }
                                            app.settings.selected_field = 0;
                                            app.settings.editing = false;
                                        } else {
                                            // Scroll Fields or Theme Selector
                                            if app.settings.editing && app.settings.active_category == SettingsCategory::Theme {
                                                // Theme Selector Scroll
                                                let fields_area_x = center_layout.x + sidebar_width;
                                                let rel_x = x.saturating_sub(fields_area_x + 1);
                                                let rel_y = y.saturating_sub(center_layout.y + 2);
                                                
                                                let max_width = center_layout.width.saturating_sub(sidebar_width + 2);
                                                let max_height = center_layout.height.saturating_sub(2);
                                                let theme_list_width = max_width.min(62);
                                                let theme_list_height = max_height.clamp(8, 14);
                                                
                                                if rel_x < theme_list_width && rel_y < theme_list_height {
                                                    let current_theme = app.settings.theme.as_str();
                                                    if let Some(pos) = app.theme_names.iter().position(|&n| n == current_theme) {
                                                        if is_down {
                                                            if pos + 1 < app.theme_names.len() {
                                                                app.settings.theme = app.theme_names[pos + 1].to_string();
                                                                app.theme = Theme::from_name(&app.settings.theme);
                                                            }
                                                        } else {
                                                            if pos > 0 {
                                                                app.settings.theme = app.theme_names[pos - 1].to_string();
                                                                app.theme = Theme::from_name(&app.settings.theme);
                                                            }
                                                        }
                                                    }
                                                }
                                            } else {
                                                // Normal Fields Scroll
                                                let count = match app.settings.active_category {
                                                    SettingsCategory::S3 => 6,
                                                    SettingsCategory::Scanner => 4,
                                                    SettingsCategory::Performance => 6,
                                                    SettingsCategory::Theme => 1,
                                                };
                                                if is_down {
                                                    if app.settings.selected_field + 1 < count {
                                                        app.settings.selected_field += 1;
                                                    }
                                                } else {
                                                    if app.settings.selected_field > 0 {
                                                        app.settings.selected_field -= 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            } else if x >= right_layout.x && x < right_layout.x + right_layout.width {
                                if is_down { if app.selected_history + 1 < app.visual_history.len() { app.selected_history += 1; } }
                                else { if app.selected_history > 0 { app.selected_history -= 1; } }
                            }
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





fn adjust_layout_dimension(config: &mut Arc<Mutex<Config>>, target: LayoutTarget, delta: i16) {
    let mut cfg = if let Ok(c) = lock_mutex(config) { c } else { return };
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
    let mut cfg = if let Ok(c) = lock_mutex(config) { c } else { return };
    match target {
        LayoutTarget::Hopper | LayoutTarget::Queue => cfg.hopper_width_percent = 50,
        LayoutTarget::History => cfg.history_width = 60,
    }
}

fn reset_all_layout_dimensions(config: &mut Arc<Mutex<Config>>) {
    if let Ok(mut cfg) = lock_mutex(config) {
        cfg.hopper_width_percent = 50;
        cfg.history_width = 60;
    }
}

fn update_layout_message(app: &mut App, target: LayoutTarget) {
    if let Ok(cfg) = lock_mutex(&app.config) {
        app.layout_adjust_message = match target {
            LayoutTarget::Hopper => format!("Hopper Width: {}% (20-80)", cfg.hopper_width_percent),
            LayoutTarget::Queue => format!("Queue Width: {}% (20-80)", 100 - cfg.hopper_width_percent),
            LayoutTarget::History => format!("History Width: {} chars (40-100)", cfg.history_width),
        };
    }
}
