use crate::core::config::Config;
use crate::core::transfer::{EndpointKind, TransferDirection};
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

use self::helpers::{
    adjust_layout_dimension, config_for_endpoint, prepare_remote_delete, queue_remote_download,
    queue_remote_download_batch, queue_remote_s3_copy, queue_remote_s3_copy_batch,
    request_remote_list, request_secondary_remote_list, reset_all_layout_dimensions,
    reset_layout_dimension, selected_remote_object, selected_secondary_remote_object,
    start_remote_download, update_layout_message,
};
use crate::app::settings::{SettingsCategory, SettingsFieldDef, SettingsFieldId, SettingsState};
use crate::app::state::{
    App, AppEvent, AppFocus, AppTab, HistoryFilter, InputMode, LayoutTarget, ModalAction,
    RemoteTarget, ViewMode,
};
use crate::components::file_picker::PickerView;
use crate::components::wizard::{WizardState, WizardStep};
use crate::ui::util::{calculate_list_offset, centered_window_bounds, fuzzy_match};
use crate::utils::lock_mutex;

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

fn transfer_panel_indices(direction: TransferDirection) -> (usize, usize) {
    match direction {
        TransferDirection::S3ToLocal => (1, 0),
        TransferDirection::LocalToS3 => (0, 1),
        TransferDirection::S3ToS3 => (1, 0),
    }
}

fn transfer_left_focus(direction: TransferDirection) -> AppFocus {
    match direction {
        TransferDirection::LocalToS3 => AppFocus::Browser,
        TransferDirection::S3ToLocal | TransferDirection::S3ToS3 => AppFocus::Remote,
    }
}

fn transfer_right_focus(direction: TransferDirection) -> AppFocus {
    match direction {
        TransferDirection::LocalToS3 => AppFocus::Remote,
        TransferDirection::S3ToLocal | TransferDirection::S3ToS3 => AppFocus::Browser,
    }
}

fn toggle_primary_remote_selection(app: &mut App, obj: S3Object) {
    if obj.is_parent {
        app.set_status("Cannot select parent entry.".to_string());
        return;
    }

    if app.selected_remote_items.remove(&obj.key).is_some() {
        app.set_status(format!(
            "Unselected '{}'. {} selected",
            obj.name,
            app.selected_remote_items.len()
        ));
    } else {
        app.selected_remote_items
            .insert(obj.key.clone(), obj.clone());
        app.set_status(format!(
            "Selected '{}'. {} selected",
            obj.name,
            app.selected_remote_items.len()
        ));
    }
}

fn toggle_secondary_remote_selection(app: &mut App, obj: S3Object) {
    if obj.is_parent {
        app.set_status("Cannot select parent entry.".to_string());
        return;
    }

    if app
        .selected_remote_items_secondary
        .remove(&obj.key)
        .is_some()
    {
        app.set_status(format!(
            "Unselected '{}'. {} selected",
            obj.name,
            app.selected_remote_items_secondary.len()
        ));
    } else {
        app.selected_remote_items_secondary
            .insert(obj.key.clone(), obj.clone());
        app.set_status(format!(
            "Selected '{}'. {} selected",
            obj.name,
            app.selected_remote_items_secondary.len()
        ));
    }
}

fn profile_index_for_endpoint_id(app: &App, endpoint_id: Option<i64>) -> usize {
    endpoint_id
        .and_then(|id| app.s3_profiles.iter().position(|profile| profile.id == id))
        .unwrap_or(0)
}

fn open_transfer_endpoint_selector(app: &mut App, target: RemoteTarget) {
    if app.s3_profiles.is_empty() {
        app.set_status("No S3 endpoint profiles configured.".to_string());
        return;
    }

    app.transfer_endpoint_select_target = Some(target);
    app.transfer_endpoint_select_index = match target {
        RemoteTarget::Primary => {
            profile_index_for_endpoint_id(app, app.primary_remote_endpoint_id())
        }
        RemoteTarget::Secondary => {
            profile_index_for_endpoint_id(app, app.transfer_destination_endpoint_id)
        }
    };
    app.input_mode = InputMode::EndpointSelect;
    let label = match target {
        RemoteTarget::Primary => "primary",
        RemoteTarget::Secondary => "secondary",
    };
    app.set_status(format!(
        "Select {} endpoint profile (Enter to apply)",
        label
    ));
}

fn move_transfer_endpoint_selector(app: &mut App, forward: bool) {
    if app.s3_profiles.is_empty() {
        return;
    }
    let len = app.s3_profiles.len();
    app.transfer_endpoint_select_index = if forward {
        (app.transfer_endpoint_select_index + 1) % len
    } else {
        (app.transfer_endpoint_select_index + len - 1) % len
    };
}

async fn apply_transfer_endpoint_selector(app: &mut App) {
    let Some(target) = app.transfer_endpoint_select_target else {
        app.input_mode = InputMode::Normal;
        return;
    };
    if app.s3_profiles.is_empty() {
        app.input_mode = InputMode::Normal;
        app.transfer_endpoint_select_target = None;
        app.transfer_endpoint_select_index = 0;
        app.set_status("No S3 endpoint profiles configured.".to_string());
        return;
    }

    let idx = app
        .transfer_endpoint_select_index
        .min(app.s3_profiles.len().saturating_sub(1));
    let endpoint_id = Some(app.s3_profiles[idx].id);
    let endpoint_name = app.endpoint_profile_name(endpoint_id);

    match target {
        RemoteTarget::Primary => {
            match app.transfer_direction {
                TransferDirection::LocalToS3 => {
                    app.transfer_destination_endpoint_id = endpoint_id;
                }
                TransferDirection::S3ToLocal | TransferDirection::S3ToS3 => {
                    app.transfer_source_endpoint_id = endpoint_id;
                }
            }
            app.remote_current_path.clear();
            app.selected_remote = 0;
            app.selected_remote_items.clear();
            app.remote_cache.clear();
            request_remote_list(app, true).await;
            app.set_status(format!("Primary remote endpoint: {}", endpoint_name));
        }
        RemoteTarget::Secondary => {
            app.transfer_destination_endpoint_id = endpoint_id;
            app.remote_secondary_current_path.clear();
            app.selected_remote_secondary = 0;
            app.selected_remote_items_secondary.clear();
            app.remote_secondary_cache.clear();
            request_secondary_remote_list(app, true).await;
            app.set_status(format!("Secondary remote endpoint: {}", endpoint_name));
        }
    }

    app.input_mode = InputMode::Normal;
    app.transfer_endpoint_select_target = None;
}

fn active_settings_field_def(app: &App) -> Option<&'static SettingsFieldDef> {
    app.settings.active_field_def()
}

fn settings_field_def_at(app: &App, index: usize) -> Option<&'static SettingsFieldDef> {
    SettingsState::field_def(app.settings.active_category, index)
}

fn active_settings_field_id(app: &App) -> Option<SettingsFieldId> {
    active_settings_field_def(app).map(|field| field.id)
}

fn cycle_theme_selection(app: &mut App, forward: bool) {
    let current = app.settings.theme.as_str();
    if let Some(idx) = app.theme_names.iter().position(|&name| name == current) {
        let next_idx = if forward {
            (idx + 1) % app.theme_names.len()
        } else {
            (idx + app.theme_names.len() - 1) % app.theme_names.len()
        };
        app.settings.theme = app.theme_names[next_idx].to_string();
        app.theme = Theme::from_name(&app.settings.theme);
    }
}

async fn open_wizard_from_settings(app: &mut App) {
    let cfg = app.config.lock().await.clone();
    app.wizard = WizardState::from_config(&cfg);
    app.show_wizard = true;
    app.wizard_from_settings = true;
    app.settings.editing = false;
}

async fn persist_settings_state(
    app: &mut App,
    conn_mutex: &Arc<Mutex<Connection>>,
) -> Result<Option<String>> {
    let mut cfg = app.config.lock().await;
    app.settings.apply_to_config(&mut cfg);

    // Apply theme immediately
    app.theme = Theme::from_name(&cfg.theme);
    let bucket_name = cfg.s3_bucket.clone();

    let conn = lock_mutex(conn_mutex)?;
    crate::core::config::save_config_to_db(&conn, &cfg)?;
    app.settings.save_secondary_profile_to_db(&conn)?;
    app.settings.load_secondary_profile_from_db(&conn)?;
    drop(cfg);
    app.refresh_s3_profiles(&conn)?;
    Ok(bucket_name)
}

fn cycle_settings_s3_profile(app: &mut App, forward: bool) {
    if forward {
        app.settings.cycle_s3_profile_selection();
    } else {
        app.settings.cycle_s3_profile_selection_prev();
    }
    app.set_status(format!(
        "S3 profile: {}",
        app.settings.selected_s3_profile_label()
    ));
}

async fn create_settings_s3_profile(app: &mut App, conn_mutex: &Arc<Mutex<Connection>>) {
    let res = {
        let conn = lock_mutex(conn_mutex);
        match conn {
            Ok(conn) => {
                let created = app.settings.create_s3_profile(&conn);
                match created {
                    Ok(()) => app.refresh_s3_profiles(&conn),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    };
    match res {
        Ok(()) => app.set_status(format!(
            "Created profile '{}'",
            app.settings.selected_s3_profile_label()
        )),
        Err(e) => app.set_status(format!("Failed to create profile: {}", e)),
    }
}

async fn delete_settings_s3_profile(app: &mut App, conn_mutex: &Arc<Mutex<Connection>>) {
    let res = {
        let conn = lock_mutex(conn_mutex);
        match conn {
            Ok(conn) => {
                let deleted = app.settings.delete_current_s3_profile(&conn);
                match deleted {
                    Ok(deleted) => {
                        let refresh = app.refresh_s3_profiles(&conn);
                        match refresh {
                            Ok(()) => Ok(deleted),
                            Err(e) => Err(e),
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    };
    match res {
        Ok(true) => app.set_status("Deleted S3 profile".to_string()),
        Ok(false) => app.set_status("No S3 profile selected to delete.".to_string()),
        Err(e) => app.set_status(format!("Failed to delete profile: {}", e)),
    }
}

async fn save_settings_with_status(app: &mut App, conn_mutex: &Arc<Mutex<Connection>>) {
    match persist_settings_state(app, conn_mutex).await {
        Ok(bucket_name) => app.set_status(format!(
            "Configuration saved. Bucket: {}",
            bucket_name.as_deref().unwrap_or("None")
        )),
        Err(e) => app.set_status(format!("Save error: {}", e)),
    }
}

async fn launch_settings_connection_test(app: &mut App) {
    let cat = app.settings.active_category;
    if cat != SettingsCategory::S3 && cat != SettingsCategory::Scanner {
        return;
    }

    app.set_status("Testing connection...");
    let tx = app.async_tx.clone();
    let mut config_clone = app.config.lock().await.clone();
    if cat == SettingsCategory::S3 {
        app.settings
            .apply_selected_s3_profile_to_config(&mut config_clone);
    }

    tokio::spawn(async move {
        let res = if cat == SettingsCategory::S3 {
            crate::services::uploader::Uploader::check_connection(&config_clone).await
        } else {
            crate::services::scanner::Scanner::new(&config_clone)
                .check_connection()
                .await
        };

        let msg = match res {
            Ok(s) => s,
            Err(e) => format!("Connection Failed: {}", e),
        };
        if let Err(send_err) = tx.send(AppEvent::Notification(msg)) {
            warn!(
                "Failed to send scanner connection notification: {}",
                send_err
            );
        }
    });
}

async fn handle_settings_category_key(
    app: &mut App,
    key: KeyCode,
    conn_mutex: &Arc<Mutex<Connection>>,
) -> Result<()> {
    match key {
        KeyCode::Up | KeyCode::Char('k') => {
            app.settings.active_category = app.settings.active_category.prev();
            app.settings.selected_field = 0;
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.settings.active_category = app.settings.active_category.next();
            app.settings.selected_field = 0;
        }
        KeyCode::Right | KeyCode::Enter => {
            app.focus = AppFocus::SettingsFields;
        }
        KeyCode::Char(']') if app.settings.active_category == SettingsCategory::S3 => {
            cycle_settings_s3_profile(app, true);
        }
        KeyCode::Char('[') if app.settings.active_category == SettingsCategory::S3 => {
            cycle_settings_s3_profile(app, false);
        }
        KeyCode::Char('n') if app.settings.active_category == SettingsCategory::S3 => {
            create_settings_s3_profile(app, conn_mutex).await;
        }
        KeyCode::Char('x') if app.settings.active_category == SettingsCategory::S3 => {
            delete_settings_s3_profile(app, conn_mutex).await;
        }
        KeyCode::Char('s') => {
            save_settings_with_status(app, conn_mutex).await;
        }
        KeyCode::Char('t') => {
            launch_settings_connection_test(app).await;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_settings_field_enter_key(app: &mut App, conn_mutex: &Arc<Mutex<Connection>>) {
    let Some(active_field) = active_settings_field_def(app) else {
        return;
    };

    if active_field.id == SettingsFieldId::SetupWizardAction {
        open_wizard_from_settings(app).await;
        return;
    }

    let mut skip_save_on_exit = false;
    if active_field.id == SettingsFieldId::S3Profile {
        if app.settings.editing {
            app.settings.editing = false;
            app.status_message =
                format!("S3 profile: {}", app.settings.selected_s3_profile_label());
        } else if app.settings.s3_profile_count() == 0 {
            app.set_status("No S3 endpoint profiles configured.".to_string());
        } else {
            app.settings.editing = true;
            app.set_status("Select S3 profile (Enter to apply)".to_string());
        }
        skip_save_on_exit = true;
    } else if active_field.kind.is_toggle() {
        if let Some(message) = app.settings.toggle_field(active_field.id) {
            app.status_message = message;
        }
        if let Err(e) = persist_settings_state(app, conn_mutex).await {
            app.set_status(format!("Save error: {}", e));
        }
        skip_save_on_exit = true;
    } else {
        app.settings.editing = !app.settings.editing;
    }

    if app.settings.editing {
        if active_field.id == SettingsFieldId::Theme {
            app.settings.original_theme = Some(app.settings.theme.clone());
        }
        return;
    }

    if skip_save_on_exit {
        return;
    }

    if active_field.id == SettingsFieldId::Theme {
        app.settings.original_theme = None;
    }
    if let Err(e) = persist_settings_state(app, conn_mutex).await {
        app.set_status(format!("Save error: {}", e));
    } else {
        app.set_status(format!("Saved: {}", active_field.save_label));
    }
}

fn handle_settings_field_edit_char(app: &mut App, c: char) {
    if let Some(field_id) = active_settings_field_id(app) {
        match field_id {
            SettingsFieldId::Theme => {
                if c == 'j' {
                    cycle_theme_selection(app, true);
                } else if c == 'k' {
                    cycle_theme_selection(app, false);
                }
            }
            SettingsFieldId::S3Profile => {
                if c == 'j' || c == ']' {
                    app.settings.cycle_s3_profile_selection();
                } else if c == 'k' || c == '[' {
                    app.settings.cycle_s3_profile_selection_prev();
                }
            }
            _ => {
                let _ = app.settings.push_char_to_field(field_id, c);
            }
        }
    }
}

async fn handle_settings_field_key(
    app: &mut App,
    key: KeyCode,
    conn_mutex: &Arc<Mutex<Connection>>,
) {
    let field_count = app.settings.active_category.field_count();

    match key {
        KeyCode::Enter => {
            handle_settings_field_enter_key(app, conn_mutex).await;
        }
        KeyCode::Esc => {
            app.settings.editing = false;
            if active_settings_field_id(app) == Some(SettingsFieldId::Theme) {
                if let Some(orig) = &app.settings.original_theme {
                    app.settings.theme = orig.clone();
                    app.theme = Theme::from_name(orig);
                }
                app.settings.original_theme = None;
            }
        }
        KeyCode::Right | KeyCode::Down
            if app.settings.editing
                && active_settings_field_id(app) == Some(SettingsFieldId::Theme) =>
        {
            cycle_theme_selection(app, true);
        }
        KeyCode::Right | KeyCode::Down
            if app.settings.editing
                && active_settings_field_id(app) == Some(SettingsFieldId::S3Profile) =>
        {
            app.settings.cycle_s3_profile_selection();
        }
        KeyCode::Left | KeyCode::Up
            if app.settings.editing
                && active_settings_field_id(app) == Some(SettingsFieldId::Theme) =>
        {
            cycle_theme_selection(app, false);
        }
        KeyCode::Left | KeyCode::Up
            if app.settings.editing
                && active_settings_field_id(app) == Some(SettingsFieldId::S3Profile) =>
        {
            app.settings.cycle_s3_profile_selection_prev();
        }
        KeyCode::Char(c) if app.settings.editing => {
            handle_settings_field_edit_char(app, c);
        }
        KeyCode::Backspace if app.settings.editing => {
            if let Some(field_id) = active_settings_field_id(app) {
                let _ = app.settings.pop_char_from_field(field_id);
            }
        }
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
                || active_settings_field_id(app) == Some(SettingsFieldId::Theme) =>
        {
            if app.settings.editing && active_settings_field_id(app) == Some(SettingsFieldId::Theme)
            {
                app.settings.editing = false;
                app.settings.original_theme = None;
            }
            save_settings_with_status(app, conn_mutex).await;
        }
        KeyCode::Char('n')
            if !app.settings.editing && app.settings.active_category == SettingsCategory::S3 =>
        {
            create_settings_s3_profile(app, conn_mutex).await;
        }
        KeyCode::Char('x')
            if !app.settings.editing && app.settings.active_category == SettingsCategory::S3 =>
        {
            delete_settings_s3_profile(app, conn_mutex).await;
        }
        KeyCode::Char(']')
            if !app.settings.editing && app.settings.active_category == SettingsCategory::S3 =>
        {
            cycle_settings_s3_profile(app, true);
        }
        KeyCode::Char('[')
            if !app.settings.editing && app.settings.active_category == SettingsCategory::S3 =>
        {
            cycle_settings_s3_profile(app, false);
        }
        KeyCode::Char('t') if !app.settings.editing => {
            launch_settings_connection_test(app).await;
        }
        _ => {}
    }
}

async fn handle_settings_center_click(
    app: &mut App,
    center_layout: Rect,
    x: u16,
    y: u16,
    conn_mutex: &Arc<Mutex<Connection>>,
) {
    let sidebar_width = 16;
    if x < center_layout.x + sidebar_width {
        app.focus = AppFocus::SettingsCategory;
        let rel_y = y.saturating_sub(center_layout.y + 1);
        if rel_y < 4 {
            let new_cat = SettingsCategory::from_sidebar_index(rel_y as usize);
            if app.settings.active_category != new_cat {
                app.settings.active_category = new_cat;
                app.settings.selected_field = 0;
                app.settings.editing = false;
            }
        }
        return;
    }

    app.focus = AppFocus::SettingsFields;
    let fields_area_x = center_layout.x + sidebar_width;

    if app.settings.editing && active_settings_field_id(app) == Some(SettingsFieldId::Theme) {
        let rel_x = x.saturating_sub(fields_area_x + 1);
        let rel_y = y.saturating_sub(center_layout.y + 2);

        let max_width = center_layout.width.saturating_sub(sidebar_width + 2);
        let max_height = center_layout.height.saturating_sub(2);
        let theme_list_width = max_width.min(62);
        let theme_list_height = max_height.clamp(8, 14);

        if rel_x < theme_list_width && rel_y < theme_list_height {
            let display_height = theme_list_height.saturating_sub(2) as usize;
            if rel_y >= 1 && (rel_y as usize) < display_height + 1 {
                let inner_rel_y = (rel_y - 1) as usize;
                let total = app.theme_names.len();
                let current_theme = app.settings.theme.as_str();

                let mut offset = 0;
                if let Some(idx) = app.theme_names.iter().position(|&n| n == current_theme) {
                    let (start, _) = centered_window_bounds(idx, total, display_height);
                    offset = start;
                }

                let target_idx = offset + inner_rel_y;
                if target_idx < total {
                    let now = Instant::now();
                    let is_double_click = if let (Some(last_time), Some(last_pos)) =
                        (app.last_click_time, app.last_click_pos)
                    {
                        now.duration_since(last_time) < Duration::from_millis(500)
                            && last_pos == (x, y)
                    } else {
                        false
                    };

                    app.settings.theme = app.theme_names[target_idx].to_string();
                    app.theme = Theme::from_name(&app.settings.theme);

                    if is_double_click {
                        app.settings.editing = false;
                        app.settings.original_theme = None;
                        if let Err(e) = persist_settings_state(app, conn_mutex).await {
                            app.status_message = format!("Failed to save theme: {}", e);
                        } else {
                            app.status_message = "Theme saved".to_string();
                        }
                        app.last_click_time = None;
                    } else {
                        app.last_click_time = Some(now);
                        app.last_click_pos = Some((x, y));
                    }
                }
            }
        } else {
            app.settings.editing = false;
            if let Some(orig) = app.settings.original_theme.take() {
                app.settings.theme = orig;
                app.theme = Theme::from_name(&app.settings.theme);
            }
        }
        return;
    }

    if app.settings.editing && active_settings_field_id(app) == Some(SettingsFieldId::S3Profile) {
        let rel_x = x.saturating_sub(fields_area_x + 1);
        let rel_y = y.saturating_sub(center_layout.y + 2);
        let max_width = center_layout.width.saturating_sub(sidebar_width + 2);
        let max_height = center_layout.height.saturating_sub(2);
        let list_width = max_width.min(42);
        let list_height = max_height.clamp(6, 14);

        if rel_x < list_width && rel_y < list_height {
            let display_height = list_height.saturating_sub(2) as usize;
            let total = app.settings.s3_profile_count();
            if total == 0 {
                return;
            }
            if rel_y >= 1 && (rel_y as usize) < display_height + 1 {
                let inner_rel_y = (rel_y - 1) as usize;
                let selected = app.settings.s3_profile_index;
                let (offset, _) = centered_window_bounds(selected, total, display_height);

                let target_idx = offset + inner_rel_y;
                if target_idx < total {
                    let now = Instant::now();
                    let is_double_click = if let (Some(last_time), Some(last_pos)) =
                        (app.last_click_time, app.last_click_pos)
                    {
                        now.duration_since(last_time) < Duration::from_millis(500)
                            && last_pos == (x, y)
                    } else {
                        false
                    };

                    app.settings.set_s3_profile_selection_index(target_idx);
                    if is_double_click {
                        app.settings.editing = false;
                        app.last_click_time = None;
                        app.status_message =
                            format!("S3 profile: {}", app.settings.selected_s3_profile_label());
                    } else {
                        app.last_click_time = Some(now);
                        app.last_click_pos = Some((x, y));
                    }
                }
            }
        } else {
            app.settings.editing = false;
        }
        return;
    }

    let rel_y = y.saturating_sub(center_layout.y + 1);
    let clicked_idx = rel_y / 3;
    let count = app.settings.active_category.field_count();

    let display_height = center_layout.height.saturating_sub(2) as usize;
    let fields_per_view = display_height / 3;
    let (offset, _) = centered_window_bounds(app.settings.selected_field, count, fields_per_view);

    let target_idx = offset + clicked_idx as usize;
    if target_idx >= count {
        return;
    }

    app.settings.selected_field = target_idx;
    let target_field = settings_field_def_at(app, target_idx).copied();

    if target_field.map(|field| field.id) == Some(SettingsFieldId::SetupWizardAction) {
        open_wizard_from_settings(app).await;
        return;
    }

    if target_field
        .map(|field| field.kind.is_toggle())
        .unwrap_or(false)
    {
        if let Some(field) = target_field {
            let _ = app.settings.toggle_field(field.id);
        }
        if let Err(e) = persist_settings_state(app, conn_mutex).await {
            app.status_message = format!("Failed to save settings: {}", e);
        }
        return;
    }

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

    if target_field
        .map(|field| field.kind.is_selector())
        .unwrap_or(false)
    {
        app.settings.editing = true;
        if target_field.map(|field| field.id) == Some(SettingsFieldId::Theme) {
            app.settings.original_theme = Some(app.settings.theme.clone());
        }
    }
}

fn handle_settings_center_scroll(
    app: &mut App,
    center_layout: Rect,
    x: u16,
    y: u16,
    is_down: bool,
) {
    let sidebar_width = 16;
    if x < center_layout.x + sidebar_width {
        if is_down {
            app.settings.active_category = app.settings.active_category.next();
        } else {
            app.settings.active_category = app.settings.active_category.prev();
        }
        app.settings.selected_field = 0;
        app.settings.editing = false;
        return;
    }

    if app.settings.editing && active_settings_field_id(app) == Some(SettingsFieldId::Theme) {
        let fields_area_x = center_layout.x + sidebar_width;
        let rel_x = x.saturating_sub(fields_area_x + 1);
        let rel_y = y.saturating_sub(center_layout.y + 2);

        let max_width = center_layout.width.saturating_sub(sidebar_width + 2);
        let max_height = center_layout.height.saturating_sub(2);
        let theme_list_width = max_width.min(62);
        let theme_list_height = max_height.clamp(8, 14);

        if rel_x < theme_list_width && rel_y < theme_list_height {
            cycle_theme_selection(app, is_down);
        }
    } else if app.settings.editing
        && active_settings_field_id(app) == Some(SettingsFieldId::S3Profile)
    {
        if is_down {
            app.settings.cycle_s3_profile_selection();
        } else {
            app.settings.cycle_s3_profile_selection_prev();
        }
    } else {
        let count = app.settings.active_category.field_count();
        if is_down {
            if app.settings.selected_field + 1 < count {
                app.settings.selected_field += 1;
            }
        } else if app.settings.selected_field > 0 {
            app.settings.selected_field -= 1;
        }
    }
}

fn resolve_local_source_endpoint_id(conn: &Connection) -> Option<i64> {
    if let Ok(Some(profile)) = crate::db::get_default_source_endpoint_profile(conn)
        && profile.kind == EndpointKind::Local
    {
        return Some(profile.id);
    }
    if let Ok(Some(profile)) = crate::db::get_default_destination_endpoint_profile(conn)
        && profile.kind == EndpointKind::Local
    {
        return Some(profile.id);
    }
    crate::db::list_endpoint_profiles(conn)
        .ok()?
        .into_iter()
        .find(|profile| profile.kind == EndpointKind::Local)
        .map(|profile| profile.id)
}

fn local_to_s3_metadata(
    conn: &Connection,
    destination_endpoint_id: Option<i64>,
) -> Option<crate::db::JobTransferMetadata> {
    let destination_endpoint_id = destination_endpoint_id?;
    Some(crate::db::JobTransferMetadata {
        source_endpoint_id: resolve_local_source_endpoint_id(conn),
        destination_endpoint_id: Some(destination_endpoint_id),
        transfer_direction: Some("local_to_s3".to_string()),
        conflict_policy: Some("overwrite".to_string()),
        scan_policy: Some("upload_only".to_string()),
    })
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
    app.wizard_from_settings = false;

    {
        let conn = lock_mutex(&conn_mutex)?;
        app.refresh_jobs(&conn)?;
    }

    if app.primary_remote_endpoint_id().is_some() {
        request_remote_list(&mut app, false).await;
    }
    if app.secondary_remote_endpoint_id().is_some() {
        request_secondary_remote_list(&mut app, false).await;
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
                AppEvent::RemoteFileList(endpoint_id, path, files) => {
                    let request_key = match endpoint_id {
                        Some(id) => format!("{id}::{path}"),
                        None => format!("none::{path}"),
                    };
                    // Clear loading state if this was the pending request
                    if app.remote_request_pending.as_ref() == Some(&request_key) {
                        app.remote_loading = false;
                        app.remote_request_pending = None;
                    }

                    // Store in cache (without parent entry)
                    app.remote_cache
                        .insert(request_key, (files.clone(), Instant::now()));

                    // Only update display if still viewing this endpoint/path.
                    if app.remote_current_path == path
                        && app.primary_remote_endpoint_id() == endpoint_id
                    {
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
                AppEvent::RemoteFileListSecondary(endpoint_id, path, files) => {
                    let request_key = match endpoint_id {
                        Some(id) => format!("{id}::{path}"),
                        None => format!("none::{path}"),
                    };
                    if app.remote_secondary_request_pending.as_ref() == Some(&request_key) {
                        app.remote_secondary_loading = false;
                        app.remote_secondary_request_pending = None;
                    }

                    app.remote_secondary_cache
                        .insert(request_key, (files.clone(), Instant::now()));

                    if app.remote_secondary_current_path == path
                        && app.secondary_remote_endpoint_id() == endpoint_id
                    {
                        let mut display_files = files;
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
                        app.s3_objects_secondary = display_files;
                        app.selected_remote_secondary = 0;
                        let msg = format!(
                            "Loaded {} items from secondary remote",
                            app.s3_objects_secondary.len()
                        );
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
                    if app.primary_remote_endpoint_id().is_some() {
                        request_remote_list(&mut app, true).await;
                    }
                    if app.secondary_remote_endpoint_id().is_some() {
                        request_secondary_remote_list(&mut app, true).await;
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
                                    match app.pending_action.clone() {
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
                                                app.status_message =
                                                    format!("Cancelled job {}", id);
                                                // Refresh to update list
                                                if let Err(e) = app.refresh_jobs(&conn) {
                                                    app.status_message =
                                                        format!("Refresh failed: {}", e);
                                                }
                                            }
                                        }
                                        ModalAction::DeleteRemoteObject(
                                            key,
                                            path_context,
                                            is_dir,
                                            target,
                                        ) => {
                                            app.status_message = format!("Deleting {}...", key);
                                            let tx = app.async_tx.clone();
                                            let endpoint_id = match target {
                                                RemoteTarget::Primary => {
                                                    app.primary_remote_endpoint_id()
                                                }
                                                RemoteTarget::Secondary => {
                                                    app.secondary_remote_endpoint_id()
                                                }
                                            };
                                            let config_clone = match config_for_endpoint(
                                                &app,
                                                endpoint_id,
                                            )
                                            .await
                                            {
                                                Ok(cfg) => cfg,
                                                Err(e) => {
                                                    app.status_message = format!(
                                                        "Delete failed: endpoint not configured ({})",
                                                        e
                                                    );
                                                    continue;
                                                }
                                            };

                                            tokio::spawn(async move {
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
                                                                let event = match target {
                                                                    RemoteTarget::Primary => {
                                                                        AppEvent::RemoteFileList(
                                                                            endpoint_id,
                                                                            path_context.clone(),
                                                                            files,
                                                                        )
                                                                    }
                                                                    RemoteTarget::Secondary => {
                                                                        AppEvent::RemoteFileListSecondary(
                                                                            endpoint_id,
                                                                            path_context.clone(),
                                                                            files,
                                                                        )
                                                                    }
                                                                };
                                                                if let Err(send_err) =
                                                                    tx.send(event)
                                                                {
                                                                    warn!(
                                                                        "Failed to send remote refresh after delete: {}",
                                                                        send_err
                                                                    );
                                                                }
                                                            }
                                                            Err(e) => {
                                                                if let Err(send_err) =
                                                                    tx.send(AppEvent::Notification(
                                                                        format!(
                                                                            "Refresh Failed: {}",
                                                                            e
                                                                        ),
                                                                    ))
                                                                {
                                                                    warn!(
                                                                        "Failed to send remote refresh error notification: {}",
                                                                        send_err
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        if let Err(send_err) =
                                                            tx.send(AppEvent::Notification(
                                                                format!("Delete Failed: {}", e),
                                                            ))
                                                        {
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
                                    app.input_mode = app
                                        .confirmation_return_mode
                                        .take()
                                        .unwrap_or(InputMode::Normal);
                                    app.pending_action = ModalAction::None;
                                    app.confirmation_msg.clear();
                                }
                                KeyCode::Char('n') | KeyCode::Esc => {
                                    // Cancel
                                    app.input_mode = app
                                        .confirmation_return_mode
                                        .take()
                                        .unwrap_or(InputMode::Normal);
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
                                    if app.wizard_from_settings {
                                        app.show_wizard = false;
                                        app.wizard_from_settings = false;
                                        app.status_message = "Closed setup wizard.".to_string();
                                    } else {
                                        app.confirmation_return_mode = None;
                                        app.input_mode = InputMode::Confirmation;
                                        app.pending_action = ModalAction::QuitApp;
                                        app.confirmation_msg = "Quit Drifter?".to_string();
                                    }
                                }
                                KeyCode::Char('s')
                                    if !app.wizard.editing
                                        && app.wizard.step != WizardStep::Done =>
                                {
                                    let skip_result: Result<()> = (|| {
                                        let conn = lock_mutex(&conn_mutex)?;
                                        let status = crate::db::get_wizard_status(&conn)?;
                                        if status != crate::db::WizardStatus::Complete {
                                            crate::db::set_wizard_status(
                                                &conn,
                                                crate::db::WizardStatus::Skipped,
                                            )?;
                                        }
                                        Ok(())
                                    })(
                                    );

                                    match skip_result {
                                        Ok(()) => {
                                            app.show_wizard = false;
                                            app.wizard_from_settings = false;
                                            app.status_message = "Setup skipped.".to_string();
                                        }
                                        Err(e) => {
                                            app.status_message =
                                                format!("Failed to skip setup: {}", e);
                                        }
                                    }
                                }
                                KeyCode::Esc if app.wizard.editing => {
                                    app.wizard.editing = false;
                                }
                                KeyCode::Enter => {
                                    if app.wizard.step == WizardStep::Done {
                                        // Save config and close wizard
                                        let defaults = Config::default();
                                        let cfg = Config {
                                            quarantine_dir: app
                                                .wizard
                                                .quarantine_dir
                                                .trim()
                                                .to_string(),
                                            reports_dir: app.wizard.reports_dir.trim().to_string(),
                                            state_dir: app.wizard.state_dir.trim().to_string(),
                                            scanner_enabled: app.wizard.scanner_enabled,
                                            clamd_host: app.wizard.clamd_host.clone(),
                                            clamd_port: app
                                                .wizard
                                                .clamd_port
                                                .parse()
                                                .unwrap_or(defaults.clamd_port),
                                            scan_chunk_size_mb: app
                                                .wizard
                                                .scan_chunk_size
                                                .parse()
                                                .unwrap_or(defaults.scan_chunk_size_mb),
                                            s3_bucket: if app.wizard.bucket.trim().is_empty() {
                                                None
                                            } else {
                                                Some(app.wizard.bucket.trim().to_string())
                                            },
                                            s3_prefix: if app.wizard.prefix.trim().is_empty() {
                                                None
                                            } else {
                                                Some(app.wizard.prefix.trim().to_string())
                                            },
                                            s3_region: if app.wizard.region.trim().is_empty() {
                                                None
                                            } else {
                                                Some(app.wizard.region.trim().to_string())
                                            },
                                            s3_endpoint: if app.wizard.endpoint.trim().is_empty() {
                                                None
                                            } else {
                                                Some(app.wizard.endpoint.trim().to_string())
                                            },
                                            s3_access_key: if app
                                                .wizard
                                                .access_key
                                                .trim()
                                                .is_empty()
                                            {
                                                None
                                            } else {
                                                Some(app.wizard.access_key.trim().to_string())
                                            },
                                            s3_secret_key: if app
                                                .wizard
                                                .secret_key
                                                .trim()
                                                .is_empty()
                                            {
                                                None
                                            } else {
                                                Some(app.wizard.secret_key.trim().to_string())
                                            },
                                            part_size_mb: app
                                                .wizard
                                                .part_size
                                                .parse()
                                                .unwrap_or(defaults.part_size_mb),
                                            concurrency_upload_global: app
                                                .wizard
                                                .concurrency_upload_global
                                                .parse()
                                                .unwrap_or(defaults.concurrency_upload_global),
                                            concurrency_scan_global: app
                                                .wizard
                                                .concurrency_scan_global
                                                .parse()
                                                .unwrap_or(defaults.concurrency_scan_global),
                                            concurrency_upload_parts: app
                                                .wizard
                                                .concurrency_upload_parts
                                                .parse()
                                                .unwrap_or(defaults.concurrency_upload_parts),
                                            concurrency_scan_parts: app
                                                .wizard
                                                .concurrency_scan_parts
                                                .parse()
                                                .unwrap_or(defaults.concurrency_scan_parts),
                                            delete_source_after_upload: app
                                                .wizard
                                                .delete_source_after_upload,
                                            host_metrics_enabled: app.wizard.host_metrics_enabled,
                                            ..defaults
                                        };

                                        let save_result: Result<()> = (|| {
                                            let conn = lock_mutex(&conn_mutex)?;
                                            crate::core::config::save_config_to_db(&conn, &cfg)?;
                                            crate::db::set_wizard_status(
                                                &conn,
                                                crate::db::WizardStatus::Complete,
                                            )?;
                                            Ok(())
                                        })(
                                        );

                                        if let Err(e) = save_result {
                                            app.status_message =
                                                format!("Failed to save setup config: {}", e);
                                            continue;
                                        }

                                        // Create directories if they don't exist
                                        if let Err(e) = std::fs::create_dir_all(&cfg.quarantine_dir)
                                        {
                                            app.status_message = format!(
                                                "Failed to create quarantine directory: {}",
                                                e
                                            );
                                        }
                                        if let Err(e) = std::fs::create_dir_all(&cfg.reports_dir) {
                                            app.status_message = format!(
                                                "Failed to create reports directory: {}",
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
                                        app.wizard_from_settings = false;
                                        app.status_message = "Setup complete!".to_string();
                                    } else if app.wizard.editing {
                                        app.wizard.editing = false;
                                    } else if !app.wizard.toggle_current_field() {
                                        app.wizard.editing = true;
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
                                KeyCode::Left | KeyCode::Right
                                    if !app.wizard.editing && app.wizard.is_toggle_field() =>
                                {
                                    app.wizard.toggle_current_field();
                                }
                                KeyCode::Char(' ') if !app.wizard.editing => {
                                    app.wizard.toggle_current_field();
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
                                    if app.current_tab == AppTab::Transfers
                                        && app.transfer_direction == TransferDirection::S3ToS3
                                    {
                                        app.input_mode == InputMode::RemoteBrowsing
                                            || app.input_mode == InputMode::EndpointSelect
                                    } else {
                                        app.input_mode == InputMode::Filter
                                            || app.input_mode == InputMode::Browsing
                                    }
                                }
                                AppFocus::Logs => app.input_mode == InputMode::LogSearch,
                                AppFocus::Remote => {
                                    app.input_mode == InputMode::RemoteBrowsing
                                        || app.input_mode == InputMode::EndpointSelect
                                }
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
                                    app.confirmation_return_mode = None;
                                    app.input_mode = InputMode::Confirmation;
                                    app.pending_action = ModalAction::QuitApp;
                                    app.confirmation_msg = "Quit Drifter?".to_string();
                                }
                                KeyCode::Char('v') if app.current_tab == AppTab::Transfers => {
                                    let was_left_panel_focus =
                                        app.focus == transfer_left_focus(app.transfer_direction);
                                    let was_right_panel_focus =
                                        app.focus == transfer_right_focus(app.transfer_direction);
                                    app.toggle_transfer_direction();
                                    app.selected_remote_items.clear();
                                    app.selected_remote_items_secondary.clear();
                                    if was_left_panel_focus {
                                        app.focus = transfer_left_focus(app.transfer_direction);
                                    } else if was_right_panel_focus {
                                        app.focus = transfer_right_focus(app.transfer_direction);
                                    }
                                    let mode_label = match app.transfer_direction {
                                        TransferDirection::LocalToS3 => "Local -> S3 (upload mode)",
                                        TransferDirection::S3ToLocal => {
                                            "S3 -> Local (download mode)"
                                        }
                                        TransferDirection::S3ToS3 => {
                                            "S3 -> S3 (Primary to Secondary)"
                                        }
                                    };
                                    app.set_status(format!("Transfer mode: {}", mode_label));
                                    if matches!(
                                        app.transfer_direction,
                                        TransferDirection::S3ToLocal | TransferDirection::S3ToS3
                                    ) {
                                        request_remote_list(&mut app, false).await;
                                    }
                                    if app.transfer_direction == TransferDirection::S3ToS3 {
                                        request_secondary_remote_list(&mut app, false).await;
                                    }
                                    continue;
                                }
                                KeyCode::Tab => {
                                    app.focus = match app.focus {
                                        AppFocus::Rail => match app.current_tab {
                                            AppTab::Transfers => {
                                                transfer_left_focus(app.transfer_direction)
                                            }
                                            AppTab::Quarantine => AppFocus::Quarantine,
                                            AppTab::Logs => AppFocus::Logs,
                                            AppTab::Settings => AppFocus::SettingsCategory,
                                        },
                                        focus
                                            if app.current_tab == AppTab::Transfers
                                                && focus
                                                    == transfer_left_focus(
                                                        app.transfer_direction,
                                                    ) =>
                                        {
                                            transfer_right_focus(app.transfer_direction)
                                        }
                                        focus
                                            if app.current_tab == AppTab::Transfers
                                                && focus
                                                    == transfer_right_focus(
                                                        app.transfer_direction,
                                                    ) =>
                                        {
                                            AppFocus::Queue
                                        }
                                        AppFocus::Browser => AppFocus::Queue,
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
                                        AppFocus::Queue if app.current_tab == AppTab::Transfers => {
                                            transfer_right_focus(app.transfer_direction)
                                        }
                                        AppFocus::Queue => AppFocus::Browser,
                                        focus
                                            if app.current_tab == AppTab::Transfers
                                                && focus
                                                    == transfer_right_focus(
                                                        app.transfer_direction,
                                                    ) =>
                                        {
                                            transfer_left_focus(app.transfer_direction)
                                        }
                                        focus
                                            if app.current_tab == AppTab::Transfers
                                                && focus
                                                    == transfer_left_focus(
                                                        app.transfer_direction,
                                                    ) =>
                                        {
                                            AppFocus::Rail
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
                                            AppTab::Transfers => {
                                                transfer_left_focus(app.transfer_direction)
                                            }
                                            AppTab::Quarantine => AppFocus::Quarantine,
                                            AppTab::Logs => AppFocus::Logs,
                                            AppTab::Settings => AppFocus::SettingsCategory,
                                        },
                                        focus
                                            if app.current_tab == AppTab::Transfers
                                                && focus
                                                    == transfer_left_focus(
                                                        app.transfer_direction,
                                                    ) =>
                                        {
                                            transfer_right_focus(app.transfer_direction)
                                        }
                                        focus
                                            if app.current_tab == AppTab::Transfers
                                                && focus
                                                    == transfer_right_focus(
                                                        app.transfer_direction,
                                                    ) =>
                                        {
                                            AppFocus::Queue
                                        }
                                        AppFocus::Browser => AppFocus::Queue,
                                        AppFocus::Remote => AppFocus::Queue,
                                        AppFocus::Queue => AppFocus::History,
                                        AppFocus::SettingsCategory => AppFocus::SettingsFields,
                                        _ => app.focus,
                                    };
                                }
                                KeyCode::Left => {
                                    app.focus = match app.focus {
                                        AppFocus::History => AppFocus::Queue,
                                        AppFocus::Queue if app.current_tab == AppTab::Transfers => {
                                            transfer_right_focus(app.transfer_direction)
                                        }
                                        AppFocus::Queue => AppFocus::Browser,
                                        focus
                                            if app.current_tab == AppTab::Transfers
                                                && focus
                                                    == transfer_right_focus(
                                                        app.transfer_direction,
                                                    ) =>
                                        {
                                            transfer_left_focus(app.transfer_direction)
                                        }
                                        focus
                                            if app.current_tab == AppTab::Transfers
                                                && focus
                                                    == transfer_left_focus(
                                                        app.transfer_direction,
                                                    ) =>
                                        {
                                            AppFocus::Rail
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
                                            AppTab::Transfers => {
                                                transfer_left_focus(app.transfer_direction)
                                            }
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
                            if app.focus != old_focus
                                && app.current_tab == AppTab::Transfers
                                && app.focus == AppFocus::Browser
                                && app.transfer_direction == TransferDirection::S3ToS3
                            {
                                request_secondary_remote_list(&mut app, false).await;
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
                                        let config = match config_for_endpoint(
                                            &app,
                                            app.primary_remote_endpoint_id(),
                                        )
                                        .await
                                        {
                                            Ok(cfg) => cfg,
                                            Err(e) => {
                                                app.status_message = format!(
                                                    "Create folder failed: endpoint not configured ({})",
                                                    e
                                                );
                                                continue;
                                            }
                                        };
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

                        if app.input_mode == InputMode::EndpointSelect {
                            match key.code {
                                KeyCode::Esc | KeyCode::Char('q') => {
                                    app.input_mode = InputMode::Normal;
                                    app.transfer_endpoint_select_target = None;
                                    app.set_status("Endpoint selection cancelled".to_string());
                                }
                                KeyCode::Enter => {
                                    apply_transfer_endpoint_selector(&mut app).await;
                                }
                                KeyCode::Up | KeyCode::Char('k') => {
                                    move_transfer_endpoint_selector(&mut app, false);
                                }
                                KeyCode::Down | KeyCode::Char('j') => {
                                    move_transfer_endpoint_selector(&mut app, true);
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
                                if app.current_tab == AppTab::Transfers
                                    && app.transfer_direction == TransferDirection::S3ToS3
                                {
                                    match app.input_mode {
                                        InputMode::Normal => match key.code {
                                            KeyCode::Char('e') => {
                                                open_transfer_endpoint_selector(
                                                    &mut app,
                                                    RemoteTarget::Secondary,
                                                );
                                            }
                                            KeyCode::Char('a') | KeyCode::Enter => {
                                                app.input_mode = InputMode::RemoteBrowsing;
                                                app.status_message =
                                                    "Browsing destination remote...".to_string();
                                                request_secondary_remote_list(&mut app, false)
                                                    .await;
                                            }
                                            KeyCode::Up | KeyCode::Char('k') => {
                                                if app.selected_remote_secondary > 0 {
                                                    app.selected_remote_secondary -= 1;
                                                }
                                            }
                                            KeyCode::Down | KeyCode::Char('j') => {
                                                if app.selected_remote_secondary + 1
                                                    < app.s3_objects_secondary.len()
                                                {
                                                    app.selected_remote_secondary += 1;
                                                }
                                            }
                                            KeyCode::Char('r') => {
                                                request_secondary_remote_list(&mut app, true).await;
                                            }
                                            KeyCode::Char(' ') => {
                                                if let Some(obj) =
                                                    selected_secondary_remote_object(&app)
                                                {
                                                    toggle_secondary_remote_selection(
                                                        &mut app, obj,
                                                    );
                                                }
                                            }
                                            KeyCode::Char('c') => {
                                                if app.selected_remote_items_secondary.is_empty() {
                                                    app.set_status(
                                                        "No remote selections to clear."
                                                            .to_string(),
                                                    );
                                                } else {
                                                    app.selected_remote_items_secondary.clear();
                                                    app.set_status(
                                                        "Cleared remote selections.".to_string(),
                                                    );
                                                }
                                            }
                                            KeyCode::Char('x') => {
                                                if let Some(obj) =
                                                    selected_secondary_remote_object(&app)
                                                {
                                                    app.selected_remote_items_secondary
                                                        .remove(&obj.key);
                                                    prepare_remote_delete(
                                                        &mut app,
                                                        obj.key,
                                                        obj.name,
                                                        obj.is_dir,
                                                        RemoteTarget::Secondary,
                                                    )
                                                    .await;
                                                }
                                            }
                                            _ => {}
                                        },
                                        InputMode::RemoteBrowsing => match key.code {
                                            KeyCode::Esc | KeyCode::Char('q') => {
                                                app.input_mode = InputMode::Normal;
                                                app.status_message = "Ready".to_string();
                                            }
                                            KeyCode::Up | KeyCode::Char('k') => {
                                                if app.selected_remote_secondary > 0 {
                                                    app.selected_remote_secondary -= 1;
                                                }
                                            }
                                            KeyCode::Down | KeyCode::Char('j') => {
                                                if app.selected_remote_secondary + 1
                                                    < app.s3_objects_secondary.len()
                                                {
                                                    app.selected_remote_secondary += 1;
                                                }
                                            }
                                            KeyCode::Right | KeyCode::Enter => {
                                                if let Some(obj) =
                                                    selected_secondary_remote_object(&app)
                                                    && obj.is_dir
                                                {
                                                    if obj.is_parent {
                                                        let current = app
                                                            .remote_secondary_current_path
                                                            .trim_end_matches('/');
                                                        if let Some(idx) = current.rfind('/') {
                                                            app.remote_secondary_current_path
                                                                .truncate(idx + 1);
                                                        } else {
                                                            app.remote_secondary_current_path
                                                                .clear();
                                                        }
                                                    } else {
                                                        app.remote_secondary_current_path
                                                            .push_str(&obj.name);
                                                    }
                                                    request_secondary_remote_list(&mut app, false)
                                                        .await;
                                                }
                                            }
                                            KeyCode::Left | KeyCode::Backspace => {
                                                if !app.remote_secondary_current_path.is_empty() {
                                                    let current = app
                                                        .remote_secondary_current_path
                                                        .trim_end_matches('/');
                                                    if let Some(idx) = current.rfind('/') {
                                                        app.remote_secondary_current_path
                                                            .truncate(idx + 1);
                                                    } else {
                                                        app.remote_secondary_current_path.clear();
                                                    }
                                                    request_secondary_remote_list(&mut app, false)
                                                        .await;
                                                }
                                            }
                                            KeyCode::Char('r') => {
                                                request_secondary_remote_list(&mut app, true).await;
                                            }
                                            KeyCode::Char(' ') => {
                                                if let Some(obj) =
                                                    selected_secondary_remote_object(&app)
                                                {
                                                    toggle_secondary_remote_selection(
                                                        &mut app, obj,
                                                    );
                                                }
                                            }
                                            KeyCode::Char('c') => {
                                                if app.selected_remote_items_secondary.is_empty() {
                                                    app.set_status(
                                                        "No remote selections to clear."
                                                            .to_string(),
                                                    );
                                                } else {
                                                    app.selected_remote_items_secondary.clear();
                                                    app.set_status(
                                                        "Cleared remote selections.".to_string(),
                                                    );
                                                }
                                            }
                                            KeyCode::Char('x') => {
                                                if let Some(obj) =
                                                    selected_secondary_remote_object(&app)
                                                {
                                                    app.selected_remote_items_secondary
                                                        .remove(&obj.key);
                                                    prepare_remote_delete(
                                                        &mut app,
                                                        obj.key,
                                                        obj.name,
                                                        obj.is_dir,
                                                        RemoteTarget::Secondary,
                                                    )
                                                    .await;
                                                }
                                            }
                                            _ => {}
                                        },
                                        _ => {}
                                    }
                                } else {
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
                                                            if let Some(parent) =
                                                                entry.path.parent()
                                                            {
                                                                app.picker.try_set_cwd(
                                                                    parent.to_path_buf(),
                                                                );
                                                                app.input_buffer.clear();
                                                                app.picker.is_searching = false;
                                                                app.picker.refresh();

                                                                // Find the index of the file we were just looking at
                                                                if let Some(idx) = app
                                                                    .picker
                                                                    .entries
                                                                    .iter()
                                                                    .position(|e| {
                                                                        e.path == entry.path
                                                                    })
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
                                                                app.picker.try_set_cwd(
                                                                    entry.path.clone(),
                                                                );
                                                                app.input_buffer.clear();
                                                                app.picker.is_searching = false;
                                                            }
                                                        } else if key.code == KeyCode::Enter
                                                            || key.code == KeyCode::Char('l')
                                                        {
                                                            if app.transfer_direction
                                                                != TransferDirection::LocalToS3
                                                            {
                                                                app.set_status(
                                                                "Upload queueing is disabled in this transfer mode.",
                                                            );
                                                            } else {
                                                                if app
                                                                    .transfer_destination_endpoint_id
                                                                    .is_none()
                                                                {
                                                                    app.set_status(
                                                                        "No S3 destination endpoint selected."
                                                                            .to_string(),
                                                                    );
                                                                    continue;
                                                                }
                                                                // Only queue files on Enter or 'l', not Right arrow
                                                                let conn_clone = conn_mutex.clone();
                                                                let tx = app.async_tx.clone();
                                                                let path = entry
                                                                    .path
                                                                    .to_string_lossy()
                                                                    .to_string();
                                                                let transfer_metadata = {
                                                                    if let Ok(conn) =
                                                                        lock_mutex(&conn_mutex)
                                                                    {
                                                                        local_to_s3_metadata(
                                                                            &conn,
                                                                            app.transfer_destination_endpoint_id,
                                                                        )
                                                                    } else {
                                                                        None
                                                                    }
                                                                };

                                                                tokio::spawn(async move {
                                                                    let session_id =
                                                                        Uuid::new_v4().to_string();
                                                                    if let Err(e) = ingest_path(
                                                                    conn_clone,
                                                                    &path,
                                                                    &session_id,
                                                                    None,
                                                                    transfer_metadata,
                                                                )
                                                                .await
                                                                    && let Err(send_err) = tx.send(
                                                                        AppEvent::Notification(
                                                                            format!(
                                                                                "Failed to queue file: {}",
                                                                                e
                                                                            ),
                                                                        ),
                                                                    )
                                                                {
                                                                    warn!(
                                                                        "Failed to send queue failure notification: {}",
                                                                        send_err
                                                                    );
                                                                }
                                                                });

                                                                app.status_message =
                                                                    "File added to queue"
                                                                        .to_string();
                                                            }
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
                                                    app.status_message =
                                                        if app.picker.search_recursive {
                                                            "Recursive search enabled"
                                                        } else {
                                                            "Recursive search disabled"
                                                        }
                                                        .to_string();
                                                }
                                                KeyCode::Char('c') => app.picker.clear_selected(),
                                                KeyCode::Char('s') => {
                                                    if app.transfer_direction
                                                        != TransferDirection::LocalToS3
                                                    {
                                                        app.set_status(
                                                        "Upload queueing is disabled in this transfer mode.",
                                                    );
                                                        continue;
                                                    }
                                                    let paths: Vec<PathBuf> = app
                                                        .picker
                                                        .selected_paths
                                                        .iter()
                                                        .cloned()
                                                        .collect();
                                                    if !paths.is_empty() {
                                                        // Check if S3 is configured
                                                        let s3_configured = app
                                                            .transfer_destination_endpoint_id
                                                            .is_some();

                                                        if s3_configured {
                                                            // S3 configured, ingest with current remote path
                                                            let destination = if app
                                                                .remote_current_path
                                                                .is_empty()
                                                            {
                                                                None
                                                            } else {
                                                                Some(
                                                                    app.remote_current_path.clone(),
                                                                )
                                                            };

                                                            let paths_count = paths.len();
                                                            let conn_clone = conn_mutex.clone();
                                                            let dest_clone = destination.clone();
                                                            let transfer_metadata = {
                                                                if let Ok(conn) =
                                                                    lock_mutex(&conn_mutex)
                                                                {
                                                                    local_to_s3_metadata(
                                                                        &conn,
                                                                        app.transfer_destination_endpoint_id,
                                                                    )
                                                                } else {
                                                                    None
                                                                }
                                                            };

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
                                                                        transfer_metadata.clone(),
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
                                                app.picker.is_searching =
                                                    !app.input_buffer.is_empty();
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
                                        | InputMode::RemoteFolderCreate
                                        | InputMode::EndpointSelect => {}
                                    }
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
                                                        || job.status == "transferring"
                                                        || job.status == "scanning"
                                                        || job.status == "pending"
                                                        || job.status == "queued";

                                                    if is_active {
                                                        // Require confirmation
                                                        app.confirmation_return_mode = None;
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
                                                app.confirmation_return_mode = None;
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
                                                    } else if let Err(e) = app.refresh_jobs(&conn) {
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
                                        if let Err(e) =
                                            crate::db::update_scan_status_with_job_status(
                                                &conn,
                                                id,
                                                "removed",
                                                crate::db::JobStatus::QuarantinedRemoved,
                                            )
                                        {
                                            quarantine_removed = false;
                                            app.status_message = format!(
                                                "Failed to update quarantine status: {}",
                                                e
                                            );
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
                                        KeyCode::Char('e') => {
                                            open_transfer_endpoint_selector(
                                                &mut app,
                                                RemoteTarget::Primary,
                                            );
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
                                        KeyCode::Char('s') => {
                                            if let Some(obj) = selected_remote_object(&app) {
                                                match app.transfer_direction {
                                                    TransferDirection::S3ToLocal => {
                                                        let destination_root =
                                                            app.picker.cwd.clone();
                                                        let selected: Vec<S3Object> = app
                                                            .selected_remote_items
                                                            .values()
                                                            .cloned()
                                                            .collect();
                                                        if selected.is_empty() {
                                                            queue_remote_download(
                                                                &mut app,
                                                                obj,
                                                                destination_root,
                                                            )
                                                            .await;
                                                        } else {
                                                            queue_remote_download_batch(
                                                                &mut app,
                                                                selected,
                                                                destination_root,
                                                            )
                                                            .await;
                                                            app.selected_remote_items.clear();
                                                        }
                                                    }
                                                    TransferDirection::S3ToS3 => {
                                                        let selected: Vec<S3Object> = app
                                                            .selected_remote_items
                                                            .values()
                                                            .cloned()
                                                            .collect();
                                                        if selected.is_empty() {
                                                            queue_remote_s3_copy(&mut app, obj)
                                                                .await;
                                                        } else {
                                                            queue_remote_s3_copy_batch(
                                                                &mut app, selected,
                                                            )
                                                            .await;
                                                            app.selected_remote_items.clear();
                                                        }
                                                    }
                                                    TransferDirection::LocalToS3 => {
                                                        app.set_status(
                                                            "Switch to S3 -> Local or S3 -> S3 mode (press 'v') to queue from remote.",
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        KeyCode::Char(' ') => {
                                            if let Some(obj) = selected_remote_object(&app) {
                                                toggle_primary_remote_selection(&mut app, obj);
                                            }
                                        }
                                        KeyCode::Char('c') => {
                                            if app.selected_remote_items.is_empty() {
                                                app.set_status(
                                                    "No remote selections to clear.".to_string(),
                                                );
                                            } else {
                                                app.selected_remote_items.clear();
                                                app.set_status(
                                                    "Cleared remote selections.".to_string(),
                                                );
                                            }
                                        }
                                        KeyCode::Char('x') => {
                                            if let Some(obj) = selected_remote_object(&app) {
                                                app.selected_remote_items.remove(&obj.key);
                                                prepare_remote_delete(
                                                    &mut app,
                                                    obj.key,
                                                    obj.name,
                                                    obj.is_dir,
                                                    RemoteTarget::Primary,
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
                                            KeyCode::Char('s') => {
                                                if let Some(obj) = selected_remote_object(&app) {
                                                    match app.transfer_direction {
                                                        TransferDirection::S3ToLocal => {
                                                            let destination_root =
                                                                app.picker.cwd.clone();
                                                            let selected: Vec<S3Object> = app
                                                                .selected_remote_items
                                                                .values()
                                                                .cloned()
                                                                .collect();
                                                            if selected.is_empty() {
                                                                queue_remote_download(
                                                                    &mut app,
                                                                    obj,
                                                                    destination_root,
                                                                )
                                                                .await;
                                                            } else {
                                                                queue_remote_download_batch(
                                                                    &mut app,
                                                                    selected,
                                                                    destination_root,
                                                                )
                                                                .await;
                                                                app.selected_remote_items.clear();
                                                            }
                                                        }
                                                        TransferDirection::S3ToS3 => {
                                                            let selected: Vec<S3Object> = app
                                                                .selected_remote_items
                                                                .values()
                                                                .cloned()
                                                                .collect();
                                                            if selected.is_empty() {
                                                                queue_remote_s3_copy(&mut app, obj)
                                                                    .await;
                                                            } else {
                                                                queue_remote_s3_copy_batch(
                                                                    &mut app, selected,
                                                                )
                                                                .await;
                                                                app.selected_remote_items.clear();
                                                            }
                                                        }
                                                        TransferDirection::LocalToS3 => {
                                                            app.set_status(
                                                                "Switch to S3 -> Local or S3 -> S3 mode (press 'v') to queue from remote.",
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            KeyCode::Char(' ') => {
                                                if let Some(obj) = selected_remote_object(&app) {
                                                    toggle_primary_remote_selection(&mut app, obj);
                                                }
                                            }
                                            KeyCode::Char('c') => {
                                                if app.selected_remote_items.is_empty() {
                                                    app.set_status(
                                                        "No remote selections to clear."
                                                            .to_string(),
                                                    );
                                                } else {
                                                    app.selected_remote_items.clear();
                                                    app.set_status(
                                                        "Cleared remote selections.".to_string(),
                                                    );
                                                }
                                            }
                                            KeyCode::Char('x') => {
                                                if let Some(obj) = selected_remote_object(&app) {
                                                    app.selected_remote_items.remove(&obj.key);
                                                    prepare_remote_delete(
                                                        &mut app,
                                                        obj.key,
                                                        obj.name,
                                                        obj.is_dir,
                                                        RemoteTarget::Primary,
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
                                handle_settings_category_key(&mut app, key.code, &conn_mutex)
                                    .await?;
                            }
                            AppFocus::SettingsFields => {
                                handle_settings_field_key(&mut app, key.code, &conn_mutex).await;
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
                                                let (browser_idx, remote_idx) =
                                                    transfer_panel_indices(app.transfer_direction);
                                                let browser_chunk = chunks[browser_idx];
                                                let remote_chunk = chunks[remote_idx];

                                                if x >= browser_chunk.x
                                                    && x < browser_chunk.x + browser_chunk.width
                                                {
                                                    app.focus = AppFocus::Browser;
                                                    if app.transfer_direction
                                                        == TransferDirection::S3ToS3
                                                    {
                                                        if app.s3_objects_secondary.is_empty() {
                                                            request_secondary_remote_list(
                                                                &mut app, false,
                                                            )
                                                            .await;
                                                        }

                                                        let inner_y = browser_chunk.y + 2;
                                                        if y >= inner_y {
                                                            let relative_row =
                                                                (y - inner_y) as usize;
                                                            let display_height = browser_chunk
                                                                .height
                                                                .saturating_sub(3)
                                                                as usize;
                                                            let total_rows =
                                                                app.s3_objects_secondary.len();
                                                            let offset = calculate_list_offset(
                                                                app.selected_remote_secondary,
                                                                total_rows,
                                                                display_height,
                                                            );
                                                            if relative_row < display_height
                                                                && offset + relative_row
                                                                    < total_rows
                                                            {
                                                                let new_idx = offset + relative_row;
                                                                app.selected_remote_secondary =
                                                                    new_idx;

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
                                                                    let obj = &app
                                                                        .s3_objects_secondary
                                                                        [new_idx];
                                                                    if obj.is_dir {
                                                                        if obj.is_parent {
                                                                            let current = app
                                                                                .remote_secondary_current_path
                                                                                .trim_end_matches('/');
                                                                            if let Some(idx) =
                                                                                current.rfind('/')
                                                                            {
                                                                                app.remote_secondary_current_path
                                                                                    .truncate(idx + 1);
                                                                            } else {
                                                                                app.remote_secondary_current_path
                                                                                    .clear();
                                                                            }
                                                                        } else {
                                                                            app.remote_secondary_current_path
                                                                                .push_str(&obj.name);
                                                                        }
                                                                        app.input_mode =
                                                                            InputMode::RemoteBrowsing;
                                                                        request_secondary_remote_list(
                                                                            &mut app, false,
                                                                        )
                                                                        .await;
                                                                    }
                                                                } else {
                                                                    app.last_click_time = Some(now);
                                                                    app.last_click_pos =
                                                                        Some((x, y));
                                                                }
                                                            }
                                                        }
                                                    } else {
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
                                                            if app.input_mode == InputMode::Normal {
                                                                app.input_mode =
                                                                    InputMode::Browsing;
                                                            }
                                                        } else {
                                                            app.last_click_time = Some(now);
                                                            app.last_click_pos = Some((x, y));
                                                        }

                                                        let inner_y = browser_chunk.y + 1;
                                                        let has_filter = !app
                                                            .input_buffer
                                                            .is_empty()
                                                            || app.input_mode == InputMode::Filter;
                                                        let table_y = inner_y
                                                            + if has_filter { 1 } else { 0 };
                                                        let content_y = table_y + 1; // +1 for header

                                                        if y >= content_y {
                                                            let relative_row =
                                                                (y - content_y) as usize;
                                                            let display_height = browser_chunk
                                                                .height
                                                                .saturating_sub(if has_filter {
                                                                    4
                                                                } else {
                                                                    3
                                                                })
                                                                as usize; // Border + table_y + header
                                                            let filter = app
                                                                .input_buffer
                                                                .trim()
                                                                .to_lowercase();
                                                            let filtered_entries: Vec<usize> = app
                                                                .picker
                                                                .entries
                                                                .iter()
                                                                .enumerate()
                                                                .filter(|(_, e)| {
                                                                    e.is_parent
                                                                        || filter.is_empty()
                                                                        || fuzzy_match(
                                                                            &filter, &e.name,
                                                                        )
                                                                })
                                                                .map(|(i, _)| i)
                                                                .collect();

                                                            if relative_row < display_height {
                                                                let total_rows =
                                                                    filtered_entries.len();
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
                                                                if offset + relative_row
                                                                    < total_rows
                                                                {
                                                                    let target_real_idx =
                                                                        filtered_entries
                                                                            [offset + relative_row];
                                                                    app.picker.selected =
                                                                        target_real_idx;
                                                                    if is_double_click {
                                                                        let entry =
                                                                            app.picker.entries
                                                                                [target_real_idx]
                                                                                .clone();
                                                                        if entry.is_dir {
                                                                            if entry.is_parent {
                                                                                app.picker
                                                                                    .go_parent();
                                                                            } else {
                                                                                app.picker
                                                                                    .try_set_cwd(
                                                                                        entry.path,
                                                                                    );
                                                                            }
                                                                        } else if app
                                                                            .transfer_direction
                                                                            != TransferDirection::LocalToS3
                                                                        {
                                                                            app.set_status(
                                                                                "Upload queueing is disabled in this transfer mode.",
                                                                            );
                                                                        } else {
                                                                            if app
                                                                                .transfer_destination_endpoint_id
                                                                                .is_none()
                                                                            {
                                                                                app.set_status(
                                                                                    "No S3 destination endpoint selected."
                                                                                        .to_string(),
                                                                                );
                                                                                continue;
                                                                            }
                                                                            let conn_clone =
                                                                                conn_mutex.clone();
                                                                            let tx = app
                                                                                .async_tx
                                                                                .clone();
                                                                            let path = entry
                                                                                .path
                                                                                .to_string_lossy()
                                                                                .to_string();
                                                                            let transfer_metadata = {
                                                                                if let Ok(conn) =
                                                                                    lock_mutex(
                                                                                        &conn_mutex,
                                                                                    )
                                                                                {
                                                                                    local_to_s3_metadata(
                                                                                        &conn,
                                                                                        app.transfer_destination_endpoint_id,
                                                                                    )
                                                                                } else {
                                                                                    None
                                                                                }
                                                                            };
                                                                            tokio::spawn(async move {
                                                                                let session_id =
                                                                                    Uuid::new_v4()
                                                                                        .to_string();
                                                                                if let Err(e) = ingest_path(
                                                                                    conn_clone,
                                                                                    &path,
                                                                                    &session_id,
                                                                                    None,
                                                                                    transfer_metadata,
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
                                                    }
                                                } else {
                                                    app.focus = AppFocus::Remote;
                                                    if app.s3_objects.is_empty() {
                                                        request_remote_list(&mut app, false).await;
                                                    }

                                                    let inner_y = remote_chunk.y + 2;
                                                    if y >= inner_y {
                                                        let relative_row = (y - inner_y) as usize;
                                                        let display_height =
                                                            remote_chunk.height.saturating_sub(3)
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
                                            handle_settings_center_click(
                                                &mut app,
                                                center_layout,
                                                x,
                                                y,
                                                &conn_mutex,
                                            )
                                            .await;
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
                                                let (browser_idx, remote_idx) =
                                                    transfer_panel_indices(app.transfer_direction);
                                                let browser_chunk = chunks[browser_idx];

                                                if x >= browser_chunk.x
                                                    && x < browser_chunk.x + browser_chunk.width
                                                {
                                                    if app.transfer_direction
                                                        == TransferDirection::S3ToS3
                                                    {
                                                        if is_down {
                                                            if app.selected_remote_secondary + 1
                                                                < app.s3_objects_secondary.len()
                                                            {
                                                                app.selected_remote_secondary += 1;
                                                            }
                                                        } else if app.selected_remote_secondary > 0
                                                        {
                                                            app.selected_remote_secondary -= 1;
                                                        }
                                                    } else if is_down {
                                                        app.picker.move_down();
                                                    } else {
                                                        app.picker.move_up();
                                                    }
                                                } else if x >= chunks[remote_idx].x
                                                    && x < chunks[remote_idx].x
                                                        + chunks[remote_idx].width
                                                {
                                                    if is_down {
                                                        if app.selected_remote + 1
                                                            < app.s3_objects.len()
                                                        {
                                                            app.selected_remote += 1;
                                                        }
                                                    } else if app.selected_remote > 0 {
                                                        app.selected_remote -= 1;
                                                    }
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
                                            handle_settings_center_scroll(
                                                &mut app,
                                                center_layout,
                                                x,
                                                y,
                                                is_down,
                                            );
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
