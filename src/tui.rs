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
use ratatui::widgets::{Block, Borders};


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
    AppTab, AppFocus,
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

        // Check for async messages (e.g. connection tests)
        if let Ok(msg) = app.async_rx.try_recv() {
            app.status_message = msg;
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
                                                    let _ = tx.send(format!("Runtime error: {}", e));
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

                                                        let filter = app.input_buffer.clone();
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
                                                            } else if let Ok(conn) = conn_mutex.lock()
                                                                && let Ok(cfg_guard) = cfg.lock()
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

                                                    let filter = app.input_buffer.clone();
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
                                                    let height = max_height.clamp(8, 14);
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
                                                                && max_visible > 0
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
                                                                        lock_mutex(&app.config)?;
                                                                    app.settings
                                                                        .apply_to_config(&mut cfg);
                                                                    let conn =
                                                                        lock_mutex(&conn_mutex)?;
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
                                                        let mut cfg = lock_mutex(&app.config)?;
                                                        app.settings.apply_to_config(&mut cfg);
                                                        let conn = lock_mutex(&conn_mutex)?;
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
                                                        let mut cfg = lock_mutex(&app.config)?;
                                                        app.settings.apply_to_config(&mut cfg);
                                                        let conn = lock_mutex(&conn_mutex)?;
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
