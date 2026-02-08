use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::{
        Block, BorderType, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, Wrap,
    },
};

use crate::app::settings::SettingsCategory;
use crate::app::state::{App, AppFocus, AppTab, InputMode, LayoutTarget, RemoteTarget};
use crate::components::file_picker::FileEntry;
use crate::components::wizard::WizardStep;
use crate::ui::key_hints::footer_hints;
use crate::ui::theme::{StatusKind, Theme};
use crate::utils::lock_mutex;

use crate::ui::util::{
    calculate_list_offset, centered_fixed_rect, centered_rect, extract_threat_name, format_bytes,
    format_bytes_rate, format_duration_ms, format_modified, format_relative_time, format_size,
    fuzzy_match, status_kind, status_kind_for_message, truncate_with_ellipsis,
};

fn draw_shadow(f: &mut Frame, area: Rect, app: &App) {
    if area.width < 2 || area.height < 2 {
        return;
    }
    let shadow_style = app.theme.shadow_style();
    let max = f.size();

    // Drop shadow: one-cell offset to the right and bottom.
    let right_x = area.x.saturating_add(area.width);
    let bottom_y = area.y.saturating_add(area.height);

    if right_x < max.width {
        let height = area
            .height
            .min(max.height.saturating_sub(area.y.saturating_add(1)));
        if height > 0 {
            let right_shadow = Rect {
                x: right_x,
                y: area.y.saturating_add(1),
                width: 1,
                height,
            };
            f.render_widget(Block::default().style(shadow_style), right_shadow);
        }
    }

    if bottom_y < max.height {
        let width = area
            .width
            .min(max.width.saturating_sub(area.x.saturating_add(1)));
        if width > 0 {
            let bottom_shadow = Rect {
                x: area.x.saturating_add(1),
                y: bottom_y,
                width,
                height: 1,
            };
            f.render_widget(Block::default().style(shadow_style), bottom_shadow);
        }
    }
}

fn transfer_phase_label(app: &App, job: &crate::db::JobRow) -> String {
    if job.status == "uploading" || job.status == "transferring" {
        return match job.upload_status.as_deref() {
            Some("copying") => "copying".to_string(),
            Some("downloading") => "downloading".to_string(),
            _ => {
                if job.status == "transferring" {
                    "transferring".to_string()
                } else {
                    "uploading".to_string()
                }
            }
        };
    }

    if job.status == "complete" {
        return match app.transfer_direction_for_job(job.id) {
            Some("s3_to_s3") => "copied".to_string(),
            Some("s3_to_local") => "downloaded".to_string(),
            Some("local_to_s3") => "uploaded".to_string(),
            _ => "complete".to_string(),
        };
    }

    job.status.clone()
}

fn queue_status_label(app: &App, job: &crate::db::JobRow) -> String {
    if job.status == "retry_pending" {
        if let Some(next_retry) = &job.next_retry_at
            && let Ok(target) = chrono::DateTime::parse_from_rfc3339(next_retry)
        {
            let diff = target
                .signed_duration_since(chrono::Utc::now())
                .num_seconds()
                .max(0);
            return format!("retry {}s", diff);
        }
        return "retrying".to_string();
    }
    transfer_phase_label(app, job)
}

fn history_status_label(app: &App, job: &crate::db::JobRow) -> String {
    match job.status.as_str() {
        "quarantined" => {
            if let Some(err) = &job.error
                && let Some(name) = err.strip_prefix("Infected: ")
            {
                return truncate_with_ellipsis(name, 15);
            }
            "Threat Detected".to_string()
        }
        "quarantined_removed" => "Threat Removed".to_string(),
        "complete" => match app.transfer_direction_for_job(job.id) {
            Some("s3_to_s3") => "Copied".to_string(),
            Some("s3_to_local") => "Downloaded".to_string(),
            Some("local_to_s3") => "Uploaded".to_string(),
            _ => "Done".to_string(),
        },
        other => other.to_string(),
    }
}

fn transfer_route_label(direction: Option<&str>) -> Option<&'static str> {
    match direction {
        Some("local_to_s3") => Some("Local -> S3"),
        Some("s3_to_local") => Some("S3 -> Local"),
        Some("s3_to_s3") => Some("S3 -> S3 (Server-Side Copy)"),
        _ => None,
    }
}

fn transfer_route_short(direction: Option<&str>) -> &'static str {
    match direction {
        Some("local_to_s3") => "L->S3",
        Some("s3_to_local") => "S3->L",
        Some("s3_to_s3") => "S3->S3",
        _ => "-",
    }
}

fn transfer_operation_label(direction: Option<&str>) -> &'static str {
    match direction {
        Some("local_to_s3") => "Upload",
        Some("s3_to_local") => "Download",
        Some("s3_to_s3") => "Copy",
        _ => "Transfer",
    }
}

fn transfer_field_labels(direction: Option<&str>, phase: &str) -> (&'static str, &'static str) {
    if phase == "copying" || direction == Some("s3_to_s3") {
        return ("Copy:   ", "Copy Time:   ");
    }
    if phase == "downloading" || direction == Some("s3_to_local") {
        return ("Download:", "Download Time:");
    }
    ("Upload: ", "Upload Time: ")
}

fn job_endpoints(app: &App, job: &crate::db::JobRow) -> (String, String) {
    let metadata = app.transfer_metadata_for_job(job.id);
    let direction = metadata.and_then(|m| m.transfer_direction.as_deref());
    let source_id = metadata.and_then(|m| m.source_endpoint_id);
    let destination_id = metadata.and_then(|m| m.destination_endpoint_id);

    match direction {
        Some("local_to_s3") => (
            "Local".to_string(),
            app.endpoint_profile_name(destination_id),
        ),
        Some("s3_to_local") => (app.endpoint_profile_name(source_id), "Local".to_string()),
        Some("s3_to_s3") => (
            app.endpoint_profile_name(source_id),
            app.endpoint_profile_name(destination_id),
        ),
        _ => ("-".to_string(), "-".to_string()),
    }
}

fn job_source_destination_values(app: &App, job: &crate::db::JobRow) -> (String, String) {
    let direction = app.transfer_direction_for_job(job.id);
    match direction {
        Some("local_to_s3") => {
            let destination = job
                .s3_key
                .clone()
                .filter(|v| !v.trim().is_empty())
                .or_else(|| job.staged_path.clone())
                .unwrap_or_else(|| "-".to_string());
            (job.source_path.clone(), destination)
        }
        Some("s3_to_local") => {
            let source = job
                .s3_key
                .clone()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| job.source_path.clone());
            let destination = job.staged_path.clone().unwrap_or_else(|| "-".to_string());
            (source, destination)
        }
        Some("s3_to_s3") => {
            let source = job
                .s3_key
                .clone()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| job.source_path.clone());
            let destination = job
                .staged_path
                .clone()
                .filter(|v| !v.trim().is_empty())
                .or_else(|| job.s3_key.clone())
                .unwrap_or_else(|| "-".to_string());
            (source, destination)
        }
        _ => (
            job.source_path.clone(),
            job.staged_path.clone().unwrap_or_else(|| "-".to_string()),
        ),
    }
}

fn format_eta_short(total_secs: u64) -> String {
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    if hours > 0 {
        format!("{hours}h{mins}m")
    } else if mins > 0 {
        format!("{mins}m{secs}s")
    } else {
        format!("{secs}s")
    }
}

fn queue_avg_eta(app: &App, job: &crate::db::JobRow) -> (String, String) {
    if job.status != "uploading" {
        return ("-".to_string(), "-".to_string());
    }
    let Some(info) = app.cached_progress.get(&job.id) else {
        return ("-".to_string(), "-".to_string());
    };
    if info.elapsed_secs < 3 || info.bytes_total == 0 || info.bytes_done < 1024 * 1024 {
        return ("-".to_string(), "-".to_string());
    }

    let rate_bps = info.bytes_done / info.elapsed_secs.max(1);
    if rate_bps == 0 {
        return ("-".to_string(), "-".to_string());
    }
    let remaining_bytes = info.bytes_total.saturating_sub(info.bytes_done);
    let eta_secs = remaining_bytes / rate_bps.max(1);
    (format_bytes_rate(rate_bps), format_eta_short(eta_secs))
}

pub fn ui(f: &mut Frame, app: &App) {
    if app.show_wizard {
        draw_wizard(f, app);
        draw_confirmation_modal(f, app, f.size());
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

    let cfg_guard = &app.cached_config;
    let main_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(16),
            Constraint::Min(0),
            Constraint::Length(cfg_guard.history_width),
        ])
        .split(root[0]);

    draw_rail(f, app, main_layout[0]);

    match app.current_tab {
        AppTab::Transfers => draw_transfers(f, app, main_layout[1]),
        AppTab::Quarantine => draw_quarantine(f, app, main_layout[1]),
        AppTab::Logs => crate::ui::logs::render_logs(f, app, main_layout[1], &app.theme),
        AppTab::Settings => draw_settings(f, app, main_layout[1]),
    }

    // Render History / Details panel (Right side) - With Metrics support
    let right_panel_full = main_layout[2];

    // Check if metrics enabled
    let metrics_enabled = app.cached_config.host_metrics_enabled;
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
            false,
        );
    } else if app.focus == AppFocus::Queue
        && !app.visual_jobs.is_empty()
        && app.selected < app.visual_jobs.len()
    {
        // Queue focused: Show selected job details in full right panel
        let visual_item = &app.visual_jobs[app.selected];
        if let Some(idx) = visual_item.index_in_jobs.or(visual_item.first_job_index) {
            draw_job_details(f, app, right_panel, &app.jobs[idx], false);
        }
    } else if app.focus == AppFocus::History
        && !app.visual_history.is_empty()
        && app.selected_history < app.visual_history.len()
    {
        // History focused: Split view
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(42), Constraint::Percentage(58)])
            .split(right_panel);

        draw_history(f, app, chunks[0]);
        let visual_item = &app.visual_history[app.selected_history];
        if let Some(idx) = visual_item.index_in_jobs.or(visual_item.first_job_index) {
            draw_job_details(f, app, chunks[1], &app.history[idx], true);
        }
    } else {
        // Default: Full history list
        draw_history(f, app, right_panel);
    }
    draw_footer(f, app, root[1]);

    // Render Modals last (on top)
    draw_layout_adjustment_overlay(f, app, f.size());
    draw_confirmation_modal(f, app, f.size());
    draw_remote_folder_create_modal(f, app, f.size());
}

fn draw_rail(f: &mut Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::Rail {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let items = [
        ("Transfers", AppTab::Transfers),
        ("Quarantine", AppTab::Quarantine),
        ("Logs", AppTab::Logs),
        ("Settings", AppTab::Settings),
    ];

    let rows: Vec<Row> = items
        .iter()
        .map(|(label, tab)| {
            let is_selected = app.current_tab == *tab;
            let style = if is_selected {
                app.theme.selection_style()
            } else {
                app.theme.text_style()
            };

            // Add selection indicator prefix
            let display_label = if is_selected {
                format!("▌ {}", label)
            } else {
                format!("  {}", label)
            };

            Row::new(vec![Cell::from(display_label)]).style(style)
        })
        .collect();

    let is_focused = app.focus == AppFocus::Rail;
    let panel_style = if is_focused {
        app.theme.panel_style()
    } else {
        app.theme.panel_style().patch(app.theme.dim_style())
    };

    // Remove right border to avoid double border with main content
    let table = Table::new(rows, [Constraint::Fill(1)]).block(
        Block::default()
            .borders(Borders::TOP | Borders::BOTTOM | Borders::LEFT)
            .border_type(app.theme.border_type)
            .title(" Navigation ")
            .border_style(focus_style)
            .style(panel_style),
    );
    f.render_widget(table, area);
}

fn draw_transfers(f: &mut Frame, app: &App, area: Rect) {
    // Commander Layout: Split vertically - (Local|Remote) on top, Queue on bottom
    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),     // Top: Local | Remote browsers
            Constraint::Length(20), // Bottom: Queue (18 rows + 2 borders)
        ])
        .split(area);

    // Split top area horizontally: Local | Remote
    let local_percent = app.cached_config.local_width_percent;
    let horizontal_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(local_percent),
            Constraint::Percentage(100 - local_percent),
        ])
        .split(vertical_chunks[0]);

    let (primary_remote_area, secondary_remote_area) = match app.transfer_direction {
        crate::core::transfer::TransferDirection::S3ToLocal => {
            let source_label = format!(
                "Remote Source ({})",
                app.endpoint_profile_name(app.transfer_source_endpoint_id)
            );
            crate::ui::remote::render_remote(
                f,
                app,
                horizontal_chunks[0],
                &app.theme,
                &source_label,
                true,
                &app.s3_objects,
                &app.remote_current_path,
                app.selected_remote,
                app.remote_loading,
                app.focus == AppFocus::Remote,
                Some(&app.selected_remote_items),
            );
            draw_browser(f, app, horizontal_chunks[1], "Local Destination", false);
            (Some(horizontal_chunks[0]), None)
        }
        crate::core::transfer::TransferDirection::S3ToS3 => {
            let source_label = format!(
                "Remote Source ({})",
                app.endpoint_profile_name(app.transfer_source_endpoint_id)
            );
            let destination_label = format!(
                "Remote Destination ({})",
                app.endpoint_profile_name(app.transfer_destination_endpoint_id)
            );
            crate::ui::remote::render_remote(
                f,
                app,
                horizontal_chunks[0],
                &app.theme,
                &source_label,
                true,
                &app.s3_objects,
                &app.remote_current_path,
                app.selected_remote,
                app.remote_loading,
                app.focus == AppFocus::Remote,
                Some(&app.selected_remote_items),
            );
            crate::ui::remote::render_remote(
                f,
                app,
                horizontal_chunks[1],
                &app.theme,
                &destination_label,
                false,
                &app.s3_objects_secondary,
                &app.remote_secondary_current_path,
                app.selected_remote_secondary,
                app.remote_secondary_loading,
                app.focus == AppFocus::Browser,
                Some(&app.selected_remote_items_secondary),
            );
            (Some(horizontal_chunks[0]), Some(horizontal_chunks[1]))
        }
        crate::core::transfer::TransferDirection::LocalToS3 => {
            let destination_label = format!(
                "Remote Destination ({})",
                app.endpoint_profile_name(app.transfer_destination_endpoint_id)
            );
            draw_browser(f, app, horizontal_chunks[0], "Local Source", true);
            crate::ui::remote::render_remote(
                f,
                app,
                horizontal_chunks[1],
                &app.theme,
                &destination_label,
                false,
                &app.s3_objects,
                &app.remote_current_path,
                app.selected_remote,
                app.remote_loading,
                app.focus == AppFocus::Remote,
                Some(&app.selected_remote_items),
            );
            (Some(horizontal_chunks[1]), None)
        }
    };

    if app.input_mode == InputMode::EndpointSelect
        && let Some(target) = app.transfer_endpoint_select_target
    {
        match target {
            RemoteTarget::Primary => {
                if let Some(panel_area) = primary_remote_area {
                    draw_transfer_endpoint_selector(f, app, panel_area, target);
                }
            }
            RemoteTarget::Secondary => {
                if let Some(panel_area) = secondary_remote_area {
                    draw_transfer_endpoint_selector(f, app, panel_area, target);
                }
            }
        }
    }

    // Render Queue (bottom)
    draw_jobs(f, app, vertical_chunks[1]);
}

fn draw_transfer_endpoint_selector(
    f: &mut Frame,
    app: &App,
    panel_area: Rect,
    target: RemoteTarget,
) {
    if panel_area.width < 18 || panel_area.height < 6 {
        return;
    }

    let max_width = panel_area.width.saturating_sub(4);
    let width = max_width.min(34);
    if width < 16 {
        return;
    }

    let max_height = panel_area.height.saturating_sub(2);
    if max_height < 4 {
        return;
    }

    let title = match (app.transfer_direction, target) {
        (crate::core::transfer::TransferDirection::LocalToS3, RemoteTarget::Primary) => {
            " Select Destination "
        }
        (crate::core::transfer::TransferDirection::S3ToLocal, RemoteTarget::Primary) => {
            " Select Source "
        }
        (crate::core::transfer::TransferDirection::S3ToS3, RemoteTarget::Primary) => {
            " Select Source "
        }
        (crate::core::transfer::TransferDirection::S3ToS3, RemoteTarget::Secondary) => {
            " Select Destination "
        }
        _ => " Select Endpoint ",
    };

    let items_len = app.s3_profiles.len().max(1);
    let height = ((items_len as u16).saturating_add(2)).clamp(4, max_height.min(12));
    let area = Rect {
        x: panel_area.x + 2,
        y: panel_area.y + 1,
        width,
        height,
    };
    f.render_widget(Clear, area);

    let list_block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .title(title)
        .border_style(app.theme.border_active_style())
        .style(app.theme.panel_style());

    if app.s3_profiles.is_empty() {
        let list = List::new(vec![ListItem::new(" No S3 profiles configured ")])
            .block(list_block)
            .style(app.theme.text_style());
        f.render_widget(list, area);
        return;
    }

    let max_visible = area.height.saturating_sub(2) as usize;
    let selected = app
        .transfer_endpoint_select_index
        .min(app.s3_profiles.len().saturating_sub(1));
    let mut start = 0usize;
    if max_visible > 0 && app.s3_profiles.len() > max_visible {
        start = selected.saturating_sub(max_visible / 2);
        if start + max_visible > app.s3_profiles.len() {
            start = app.s3_profiles.len().saturating_sub(max_visible);
        }
    }
    let end = if max_visible == 0 {
        app.s3_profiles.len()
    } else {
        (start + max_visible).min(app.s3_profiles.len())
    };

    let visible_items: Vec<ListItem> = app
        .s3_profiles
        .iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .enumerate()
        .map(|(idx, profile)| {
            let absolute = start + idx;
            let style = if absolute == selected {
                app.theme.selection_style()
            } else {
                app.theme.text_style()
            };
            ListItem::new(format!(" {} ", profile.name)).style(style)
        })
        .collect();

    let mut list_state = ratatui::widgets::ListState::default();
    list_state.select(Some(selected.saturating_sub(start)));
    let list = List::new(visible_items).block(list_block);
    f.render_stateful_widget(list, area, &mut list_state);
}

fn draw_browser(f: &mut Frame, app: &App, area: Rect, role_label: &str, is_left_panel: bool) {
    let is_focused = app.focus == AppFocus::Browser;
    let focus_style = if is_focused {
        if app.input_mode == InputMode::Browsing || app.input_mode == InputMode::Filter {
            app.theme.status_style(StatusKind::Success)
        } else {
            app.theme.border_active_style()
        }
    } else {
        app.theme.border_style()
    };

    let path_str = app.picker.cwd.to_string_lossy();
    let title_prefix = match app.input_mode {
        InputMode::Browsing => format!(" {} (Browsing) ", role_label),
        InputMode::Filter => format!(" {} (Filter) ", role_label),
        InputMode::Normal if is_focused => format!(" {} (Press 'a' to browse) ", role_label),
        InputMode::Normal => format!(" {} ", role_label),
        InputMode::Confirmation => format!(" {} ", role_label),
        InputMode::LayoutAdjust => " Layout ".to_string(),
        InputMode::LogSearch => " Search ".to_string(),
        InputMode::QueueSearch => " Queue (Search) ".to_string(),
        InputMode::HistorySearch => " History (Search) ".to_string(),
        _ => format!(" {} ", role_label),
    };
    let title = format!("{}{}", title_prefix, path_str);

    let panel_style = if is_focused {
        app.theme.panel_style()
    } else {
        app.theme.panel_style().patch(app.theme.dim_style())
    };

    let borders = if is_left_panel {
        Borders::ALL
    } else {
        Borders::TOP | Borders::BOTTOM | Borders::RIGHT
    };

    let block = Block::default()
        .borders(borders)
        .border_type(app.theme.border_type)
        .title(title)
        .border_style(focus_style)
        .style(panel_style);

    let inner_area = block.inner(area);
    f.render_widget(block, area);

    if inner_area.height < 3 {
        return;
    }

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
        let filter_area = Rect::new(inner_area.x, inner_area.y, inner_area.width, 1);
        f.render_widget(Paragraph::new(filter_line), filter_area);
    }

    let has_filter = !app.input_buffer.is_empty() || app.input_mode == InputMode::Filter;
    let table_area = Rect::new(
        inner_area.x,
        inner_area.y + if has_filter { 1 } else { 0 },
        inner_area.width,
        inner_area
            .height
            .saturating_sub(if has_filter { 1 } else { 0 }),
    );

    // Live Filtering
    let filter = app.input_buffer.clone();

    let filtered_entries: Vec<(usize, &FileEntry)> = app
        .picker
        .entries
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            if e.is_parent {
                return true;
            }
            if filter.is_empty() {
                return true;
            }

            if fuzzy_match(&filter, &e.name) {
                return true;
            }

            if app.picker.search_recursive
                && let Ok(rel_path) = e.path.strip_prefix(&app.picker.cwd)
            {
                return fuzzy_match(&filter, &rel_path.to_string_lossy());
            }

            false
        })
        .collect();

    let total_rows = filtered_entries.len();
    let display_height = table_area.height.saturating_sub(1) as usize; // -1 for header

    let filtered_selected = filtered_entries
        .iter()
        .position(|(i, _)| *i == app.picker.selected)
        .unwrap_or(0);
    let offset = calculate_list_offset(filtered_selected, total_rows, display_height);

    // Calculate hover row index if mouse is hovering over this panel
    let hover_row_idx = app.hover_pos.and_then(|(hx, hy)| {
        // Check if hover is within table area (accounting for header and borders)
        let table_start_y = area.y + 1 + if has_filter { 1 } else { 0 } + 1; // border + filter? + header
        let table_end_y = area.y + area.height.saturating_sub(1);
        if hx >= area.x && hx < area.x + area.width && hy >= table_start_y && hy < table_end_y {
            Some((hy - table_start_y) as usize + offset)
        } else {
            None
        }
    });

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

            let display_name = if app.picker.search_recursive && app.picker.is_searching {
                if let Ok(rel_path) = entry.path.strip_prefix(&app.picker.cwd) {
                    rel_path.to_string_lossy().to_string()
                } else {
                    entry.name.clone()
                }
            } else {
                entry.name.clone()
            };

            let name_styled =
                format!("{}{}{}", "  ".repeat(entry.depth), expand, prefix) + &display_name;
            let size_str = if entry.is_dir {
                "-".to_string()
            } else {
                format_size(entry.size)
            };
            let mod_str = format_modified(entry.modified);

            let is_selected =
                (*orig_idx == app.picker.selected) && (app.focus == AppFocus::Browser);
            let is_staged = app.picker.selected_paths.contains(&entry.path);
            let is_hovered = hover_row_idx == Some(*orig_idx);

            let mut style = if is_selected {
                app.theme.selection_style()
            } else if is_hovered && !is_focused {
                // Subtle hover highlight when not focused
                app.theme.row_style(false).add_modifier(Modifier::BOLD)
            } else if (visible_idx + offset).is_multiple_of(2) {
                app.theme.row_style(false)
            } else {
                app.theme.row_style(true)
            };

            if is_staged {
                if is_selected {
                    style = style.fg(ratatui::style::Color::Green);
                } else {
                    style = app.theme.status_style(StatusKind::Success);
                }
            }

            // Apply dimming if panel is not focused
            if !is_focused {
                style = style.patch(app.theme.dim_style());
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

fn draw_jobs(f: &mut Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::Queue {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let header_cells = [
        "File", "Op", "Route", "From", "To", "Status", "Size", "Progress", "Avg", "ETA",
    ]
    .iter()
    .map(|h| Cell::from(*h).style(app.theme.header_style()));
    let header = Row::new(header_cells)
        .style(app.theme.header_style())
        .height(1);

    let is_focused = app.focus == AppFocus::Queue;
    let panel_style = if is_focused {
        app.theme.panel_style()
    } else {
        app.theme.panel_style().patch(app.theme.dim_style())
    };

    let block = Block::default()
        .borders(Borders::LEFT | Borders::RIGHT | Borders::BOTTOM)
        .border_type(app.theme.border_type)
        .title(" Queue (Jobs) ")
        .border_style(focus_style)
        .style(panel_style);

    let inner_area = block.inner(area);

    let has_filter = !app.queue_search_query.is_empty() || app.input_mode == InputMode::QueueSearch;

    if has_filter {
        let filter_line = Line::from(vec![
            Span::styled(" 🔍 Filter: ", app.theme.muted_style()),
            Span::styled(&app.queue_search_query, app.theme.accent_style()),
            Span::styled(
                if app.input_mode == InputMode::QueueSearch {
                    " █"
                } else {
                    ""
                },
                app.theme.accent_style(),
            ),
        ]);
        let filter_area = Rect::new(inner_area.x, inner_area.y, inner_area.width, 1);
        f.render_widget(Paragraph::new(filter_line), filter_area);
    }

    let table_area = Rect::new(
        inner_area.x,
        inner_area.y + if has_filter { 1 } else { 0 },
        inner_area.width,
        inner_area
            .height
            .saturating_sub(if has_filter { 1 } else { 0 }),
    );

    let total_rows = app.visual_jobs.len();
    let display_height = table_area.height.saturating_sub(2) as usize; // Header + 1 padding for table border?
    // Actually, Ratatui Table.block handles its own border.
    // Wait, draw_jobs currently defines its own header row and creates a Table.

    let offset = calculate_list_offset(app.selected, total_rows, display_height);

    // Calculate hover row index if mouse is hovering over this panel
    let hover_row_idx = app.hover_pos.and_then(|(hx, hy)| {
        let table_start_y = table_area.y + 1; // header
        let table_end_y = table_area.y + table_area.height;
        if hx >= area.x && hx < area.x + area.width && hy >= table_start_y && hy < table_end_y {
            Some((hy - table_start_y) as usize + offset)
        } else {
            None
        }
    });

    let rows = app
        .visual_jobs
        .iter()
        .enumerate()
        .skip(offset)
        .take(display_height)
        .map(|(idx, item)| {
            let is_selected = (idx == app.selected) && (app.focus == AppFocus::Queue);
            let is_hovered = hover_row_idx == Some(idx);
            let mut row_style = if is_selected {
                app.theme.selection_style()
            } else if is_hovered && app.focus != AppFocus::Queue {
                app.theme
                    .row_style(idx % 2 != 0)
                    .add_modifier(Modifier::BOLD)
            } else {
                app.theme.row_style(idx % 2 != 0)
            };

            // Apply dimming if panel is not focused
            let is_queue_focused = app.focus == AppFocus::Queue;
            if !is_queue_focused {
                row_style = row_style.patch(app.theme.dim_style());
            }

            let indent = " ".repeat(item.depth * 2);
            let text = truncate_with_ellipsis(&item.text, 35);

            let display_name = if item.is_folder {
                format!("{}📁 {}", indent, text)
            } else {
                format!("{}📄 {}", indent, text)
            };

            if let Some(job_idx) = item.index_in_jobs {
                let job = &app.jobs[job_idx];
                let status_label = queue_status_label(app, job);
                let direction = app.transfer_direction_for_job(job.id);
                let operation_label = transfer_operation_label(direction);
                let route_label = transfer_route_short(direction);
                let (source_endpoint, destination_endpoint) = job_endpoints(app, job);
                let source_short = truncate_with_ellipsis(&source_endpoint, 10);
                let destination_short = truncate_with_ellipsis(&destination_endpoint, 10);

                let p_str = {
                    if job.status == "uploading" || job.status == "transferring" {
                        if status_label == "copying" {
                            "SERVER COPY".to_string()
                        } else if status_label == "downloading" {
                            "DOWNLOADING".to_string()
                        } else {
                            let p_map = &app.cached_progress;
                            let entry = p_map.get(&job.id).cloned();
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
                                "░░░░░░░░░░   0%".to_string()
                            }
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
                let (avg_rate, eta) = queue_avg_eta(app, job);

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
                    Cell::from(operation_label),
                    Cell::from(route_label),
                    Cell::from(source_short),
                    Cell::from(destination_short),
                    Cell::from(status_label).style(status_style),
                    Cell::from(format_bytes(job.size_bytes as u64)),
                    Cell::from(p_str).style(progress_style),
                    Cell::from(avg_rate),
                    Cell::from(eta),
                ])
                .style(row_style)
            } else {
                Row::new(vec![
                    Cell::from(display_name),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                ])
                .style(row_style)
            }
        });

    // Join with the panel above to avoid double borders
    f.render_widget(block, area);

    let table = Table::new(
        rows,
        [
            Constraint::Min(16),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(9),
            Constraint::Length(16),
            Constraint::Length(10),
            Constraint::Length(8),
        ],
    )
    .header(header);

    f.render_widget(table, table_area);
}

fn draw_history(f: &mut Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::History {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let header_cells = ["File", "Result", "Route", "Size", "Duration"]
        .iter()
        .map(|h| Cell::from(*h).style(app.theme.header_style()));
    let header = Row::new(header_cells)
        .style(app.theme.header_style())
        .height(1);

    let is_focused = app.focus == AppFocus::History;
    let panel_style = if is_focused {
        app.theme.panel_style()
    } else {
        app.theme.panel_style().patch(app.theme.dim_style())
    };

    // Join with main panel to avoid double borders on the left edge
    let block = Block::default()
        .borders(Borders::TOP | Borders::BOTTOM | Borders::RIGHT)
        .border_type(app.theme.border_type)
        .title(format!(" History [{}] ", app.history_filter.as_str()))
        .border_style(focus_style)
        .style(panel_style);

    let inner_area = block.inner(area);
    f.render_widget(block, area);

    let has_filter =
        !app.history_search_query.is_empty() || app.input_mode == InputMode::HistorySearch;

    if has_filter {
        let filter_line = Line::from(vec![
            Span::styled(" 🔍 Filter: ", app.theme.muted_style()),
            Span::styled(&app.history_search_query, app.theme.accent_style()),
            Span::styled(
                if app.input_mode == InputMode::HistorySearch {
                    " █"
                } else {
                    ""
                },
                app.theme.accent_style(),
            ),
        ]);
        let filter_area = Rect::new(inner_area.x, inner_area.y, inner_area.width, 1);
        f.render_widget(Paragraph::new(filter_line), filter_area);
    }

    let table_area = Rect::new(
        inner_area.x,
        inner_area.y + if has_filter { 1 } else { 0 },
        inner_area.width,
        inner_area
            .height
            .saturating_sub(if has_filter { 1 } else { 0 }),
    );

    let total_rows = app.visual_history.len();
    let display_height = table_area.height.saturating_sub(2) as usize; // Header + 1 padding?
    let offset = calculate_list_offset(app.selected_history, total_rows, display_height);

    let rows = app
        .visual_history
        .iter()
        .enumerate()
        .skip(offset)
        .take(display_height)
        .map(|(idx, item)| {
            let is_selected = (idx == app.selected_history) && (app.focus == AppFocus::History);
            let is_focused = app.focus == AppFocus::History;
            let mut row_style = if is_selected {
                app.theme.selection_style()
            } else {
                app.theme.row_style(idx % 2 != 0)
            };

            // Apply dimming if panel is not focused
            if !is_focused {
                row_style = row_style.patch(app.theme.dim_style());
            }

            let indent = " ".repeat(item.depth * 2);
            let text = truncate_with_ellipsis(&item.text, 30);

            let display_name = if item.is_folder {
                format!("{}📁 {}", indent, text)
            } else {
                format!("{}📄 {}", indent, text)
            };

            if let Some(job_idx) = item.index_in_jobs {
                let job = &app.history[job_idx];
                let status_str = history_status_label(app, job);
                let direction = app.transfer_direction_for_job(job.id);
                let route_str = transfer_route_short(direction);
                let duration = job
                    .upload_duration_ms
                    .or(job.scan_duration_ms)
                    .map(format_duration_ms)
                    .unwrap_or_else(|| "-".to_string());

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
                    Cell::from(route_str),
                    Cell::from(format_bytes(job.size_bytes as u64)),
                    Cell::from(duration),
                ])
                .style(row_style)
            } else {
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
            Constraint::Min(24),
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(10),
        ],
    )
    .header(header);

    f.render_widget(table, table_area);
}

fn draw_job_details(
    f: &mut Frame,
    app: &App,
    area: Rect,
    job: &crate::db::JobRow,
    join_top_border: bool,
) {
    let borders = if join_top_border {
        Borders::RIGHT | Borders::BOTTOM
    } else {
        Borders::TOP | Borders::RIGHT | Borders::BOTTOM
    };

    // Join with main panel on the left; optionally join with panel above
    let block = Block::default()
        .borders(borders)
        .border_type(app.theme.border_type)
        .title(format!(" Job #{} Details ", job.id))
        .border_style(app.theme.border_style())
        .style(app.theme.panel_style());

    let metadata = app.transfer_metadata_for_job(job.id);
    let transfer_direction = metadata.and_then(|m| m.transfer_direction.as_deref());
    let conflict_policy = metadata
        .and_then(|m| m.conflict_policy.as_deref())
        .unwrap_or("-");
    let scan_policy = metadata
        .and_then(|m| m.scan_policy.as_deref())
        .unwrap_or("-");
    let scan_status = job.scan_status.as_deref().unwrap_or("none");
    let transfer_status = job.upload_status.as_deref().unwrap_or("none");
    let error = job.error.as_deref().unwrap_or("none");
    let phase_label = transfer_phase_label(app, job);
    let operation = transfer_operation_label(transfer_direction);
    let (transfer_label, duration_label) = transfer_field_labels(transfer_direction, &phase_label);
    let transfer_label_name = transfer_label.trim().trim_end_matches(':');
    let duration_label_name = duration_label.trim().trim_end_matches(':');
    let (source_endpoint, destination_endpoint) = job_endpoints(app, job);
    let (source_value, destination_value) = job_source_destination_values(app, job);

    let avg_rate = if let Some(ms) = job.upload_duration_ms {
        if ms > 0 && job.size_bytes > 0 {
            format_bytes_rate((job.size_bytes as u64).saturating_mul(1000) / ms as u64)
        } else {
            "-".to_string()
        }
    } else {
        "-".to_string()
    };

    let recent_events = {
        let conn = match lock_mutex(&app.conn) {
            Ok(conn) => conn,
            Err(_) => {
                let p = Paragraph::new("Unable to load details")
                    .block(block)
                    .wrap(Wrap { trim: true });
                f.render_widget(p, area);
                return;
            }
        };
        crate::db::list_job_events(&conn, job.id, 6).unwrap_or_default()
    };
    let finished_at = if matches!(
        job.status.as_str(),
        "complete" | "failed" | "cancelled" | "quarantined" | "quarantined_removed"
    ) {
        recent_events.first().map(|event| event.created_at.as_str())
    } else {
        None
    };

    let local_checksum = job.checksum.as_deref().unwrap_or("Calculating...");
    let remote_checksum = job.remote_checksum.as_deref().unwrap_or("Not uploaded");
    let remote_style = if job.status == "complete" && job.remote_checksum.is_some() {
        app.theme.status_style(StatusKind::Success)
    } else {
        app.theme.text_style()
    };
    let checksum_verdict = match (job.checksum.as_deref(), job.remote_checksum.as_deref()) {
        (Some(local), Some(remote)) if local == remote => ("Match", StatusKind::Success),
        (Some(_), Some(_)) => ("Mismatch", StatusKind::Error),
        _ => ("Not Available", StatusKind::Warning),
    };

    let value_max = area.width.saturating_sub(16) as usize;
    let compact_details = join_top_border || area.height <= 22;
    let mut text: Vec<Line> = Vec::new();
    macro_rules! push_heading {
        ($title:expr $(,)?) => {{
            if !text.is_empty() {
                text.push(Line::from(""));
            }
            text.push(Line::from(Span::styled(
                $title.to_string(),
                app.theme
                    .highlight_style()
                    .add_modifier(Modifier::UNDERLINED),
            )));
        }};
    }
    macro_rules! push_field {
        ($label:expr, $value:expr, $style:expr $(,)?) => {{
            let display = truncate_with_ellipsis($value, value_max.max(8));
            text.push(Line::from(vec![
                Span::styled(format!("{:<11}: ", $label), app.theme.highlight_style()),
                Span::styled(display, $style),
            ]));
        }};
    }

    push_heading!("SUMMARY");
    push_field!(
        "Result",
        &phase_label,
        app.theme.status_style(status_kind(job.status.as_str())),
    );
    push_field!("Operation", operation, app.theme.text_style());
    if let Some(route) = transfer_route_label(transfer_direction) {
        push_field!("Route", route, app.theme.text_style());
    }
    push_field!(
        "Size",
        &format_bytes(job.size_bytes as u64),
        app.theme.text_style()
    );
    if let Some(ms) = job.upload_duration_ms {
        push_field!(
            duration_label_name,
            &format_duration_ms(ms),
            app.theme.text_style(),
        );
    }
    push_field!("Avg Rate", &avg_rate, app.theme.text_style());
    if !compact_details {
        push_field!("Session", &job.session_id, app.theme.text_style());
        push_field!("Started", &job.created_at, app.theme.text_style());
        push_field!(
            "Finished",
            finished_at.unwrap_or("-"),
            app.theme.text_style()
        );
    }

    push_heading!("TRANSFER");
    push_field!("Source", &source_value, app.theme.text_style());
    push_field!("Dest", &destination_value, app.theme.text_style());
    if !compact_details {
        push_field!("Source Ep", &source_endpoint, app.theme.text_style());
        push_field!("Dest Ep", &destination_endpoint, app.theme.text_style());
    }
    push_field!(transfer_label_name, transfer_status, app.theme.text_style());
    push_field!("Scan", scan_status, app.theme.text_style());

    if compact_details {
        push_heading!("POLICY");
        push_field!(
            "Rules",
            &format!("{conflict_policy} / {scan_policy}"),
            app.theme.text_style(),
        );
    } else {
        push_heading!("POLICY");
        push_field!("Conflict", conflict_policy, app.theme.text_style());
        push_field!("Scan Mode", scan_policy, app.theme.text_style());
        if let Some(ms) = job.scan_duration_ms {
            push_field!("Scan Time", &format_duration_ms(ms), app.theme.text_style());
        }
        push_field!(
            "Retry",
            &format!(
                "attempt {}{}",
                job.retry_count + 1,
                job.next_retry_at
                    .as_deref()
                    .map(|v| format!(", next={v}"))
                    .unwrap_or_default()
            ),
            app.theme.text_style(),
        );
    }

    if job.status == "uploading" && phase_label == "uploading" {
        if let Some(info) = app.cached_progress.get(&job.id)
            && info.parts_total > 0
        {
            push_heading!("MULTIPART");
            let bar_width = 20;
            let filled = (info.parts_done * bar_width) / info.parts_total.max(1);
            let bar = format!(
                "[{}{}] {}/{}",
                "█".repeat(filled),
                "░".repeat(bar_width - filled),
                info.parts_done,
                info.parts_total
            );
            push_field!("Parts", &bar, app.theme.progress_style());
            if !compact_details {
                push_field!("Detail", &info.details, app.theme.text_style());
            }
        }
    } else if job.status == "retry_pending" {
        push_heading!("RETRY");
        if let Some(next_retry) = &job.next_retry_at
            && let Ok(target) = chrono::DateTime::parse_from_rfc3339(next_retry)
        {
            let now = chrono::Utc::now();
            let diff = target.signed_duration_since(now).num_seconds();
            let wait_msg = if diff > 0 {
                format!(
                    "Retrying in {}s (attempt {}/{})",
                    diff,
                    job.retry_count + 1,
                    5
                )
            } else {
                "Retrying momentarily...".to_string()
            };
            push_field!(
                "State",
                &wait_msg,
                app.theme.status_style(StatusKind::Warning)
            );
        }
    }

    push_heading!("INTEGRITY");
    push_field!(
        "Verdict",
        checksum_verdict.0,
        app.theme.status_style(checksum_verdict.1),
    );
    push_field!("Remote", remote_checksum, remote_style);
    if !compact_details {
        push_field!("Local", local_checksum, app.theme.text_style());
    }

    if !compact_details || error != "none" {
        push_heading!("ERROR");
        push_field!(
            "Message",
            error,
            if error != "none" {
                app.theme.status_style(StatusKind::Error)
            } else {
                app.theme.text_style()
            },
        );
    }

    if compact_details {
        push_heading!("LATEST EVENT");
        if let Some(event) = recent_events.first() {
            let when = format_relative_time(&event.created_at);
            let msg = truncate_with_ellipsis(&event.message, value_max.saturating_sub(14).max(12));
            text.push(Line::from(vec![
                Span::styled(format!("{when:>8} "), app.theme.muted_style()),
                Span::styled(msg, app.theme.text_style()),
            ]));
        } else {
            push_field!("Events", "none", app.theme.text_style());
        }
    } else {
        push_heading!("RECENT EVENTS");
        if recent_events.is_empty() {
            push_field!("Events", "none", app.theme.text_style());
        } else {
            for event in &recent_events {
                let when = format_relative_time(&event.created_at);
                let msg =
                    truncate_with_ellipsis(&event.message, value_max.saturating_sub(20).max(12));
                text.push(Line::from(vec![
                    Span::styled(format!("{when:>8} "), app.theme.muted_style()),
                    Span::styled(
                        format!("{:<10}", event.event_type),
                        app.theme.highlight_style(),
                    ),
                    Span::styled(msg, app.theme.text_style()),
                ]));
            }
        }
    }

    let p = Paragraph::new(text).block(block).wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

fn draw_quarantine(f: &mut Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::Quarantine {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let header_cells = ["File", "Threat", "Status", "Time"]
        .iter()
        .map(|h| Cell::from(*h).style(app.theme.header_style()));
    let header = Row::new(header_cells)
        .style(app.theme.header_style())
        .height(1);

    let total_rows = app.quarantine.len();
    let display_height = area.height.saturating_sub(4) as usize;
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
            let is_focused = app.focus == AppFocus::Quarantine;
            let mut row_style = if is_selected {
                app.theme.selection_style()
            } else {
                app.theme.row_style(idx % 2 != 0)
            };

            // Apply dimming if panel is not focused
            if !is_focused {
                row_style = row_style.patch(app.theme.dim_style());
            }

            let filename = std::path::Path::new(&job.source_path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| job.source_path.clone());
            let display_name = truncate_with_ellipsis(&filename, 25);

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
                    app.theme.selection_style()
                } else {
                    app.theme.status_style(StatusKind::Error)
                }),
                Cell::from(status).style(status_style),
                Cell::from(time_str),
            ])
            .style(row_style)
        });

    let is_focused = app.focus == AppFocus::Quarantine;
    let panel_style = if is_focused {
        app.theme.panel_style()
    } else {
        app.theme.panel_style().patch(app.theme.dim_style())
    };

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
            .style(panel_style),
    );

    f.render_widget(table, area);
}

fn draw_settings(f: &mut Frame, app: &App, area: Rect) {
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

    let categories = ["S3 Storage", "Scanner", "Performance", "General"];
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

    let fields_area = main_layout[1];
    let display_height = fields_area.height as usize;
    let fields_per_view = display_height / 3;

    let fields = match app.settings.active_category {
        SettingsCategory::S3 => vec![
            ("Profile", app.settings.selected_s3_profile_label()),
            ("Profile Name", app.settings.selected_s3_profile_name()),
            ("S3 Endpoint", app.settings.selected_s3_endpoint()),
            ("S3 Bucket", app.settings.selected_s3_bucket()),
            ("S3 Region", app.settings.selected_s3_region()),
            ("S3 Prefix", app.settings.selected_s3_prefix()),
            ("S3 Access Key", app.settings.selected_s3_access_key()),
            (
                "S3 Secret Key",
                app.settings.selected_s3_secret_key_display(
                    app.settings.editing && app.settings.selected_field == 7,
                ),
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
                "Global Upload Concurrency (Files)",
                app.settings.concurrency_global.as_str(),
            ),
            (
                "Global Scan Concurrency (Files)",
                app.settings.concurrency_scan_global.as_str(),
            ),
            (
                "Upload Part Concurrency (Streams/File)",
                app.settings.concurrency_upload_parts.as_str(),
            ),
            (
                "Scanner Concurrency (Chunks/File)",
                app.settings.concurrency_scan_parts.as_str(),
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
        SettingsCategory::General => vec![
            ("Theme", app.settings.theme.as_str()),
            ("Setup Wizard", "Run guided setup"),
        ],
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
        let is_text_edit_field = matches!(
            (app.settings.active_category, i),
            (SettingsCategory::S3, 1..=7)
                | (SettingsCategory::Scanner, 0..=2)
                | (SettingsCategory::Performance, 0..=4)
        );
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

        let value_style = if is_selected && app.settings.editing && is_text_edit_field {
            app.theme.input_style(true)
        } else if is_selected {
            app.theme.text_style().add_modifier(Modifier::BOLD)
        } else {
            app.theme.text_style()
        };

        let display_value = if app.settings.active_category == SettingsCategory::S3 && i == 0 {
            format!("▼ {}", value)
        } else if app.settings.active_category == SettingsCategory::General && i == 0 {
            format!("▼ {}", value)
        } else {
            (*value).to_string()
        };
        let p = Paragraph::new(display_value.clone())
            .block(block)
            .style(value_style);
        f.render_widget(p, field_chunks[chunk_idx]);

        if is_selected && app.settings.editing && is_text_edit_field {
            f.set_cursor(
                field_chunks[chunk_idx].x + 1 + display_value.len() as u16,
                field_chunks[chunk_idx].y + 1,
            );
        }
    }

    if app.settings.editing
        && app.settings.active_category == SettingsCategory::General
        && app.settings.selected_field == 0
    {
        let max_width = fields_area.width.saturating_sub(2);
        let max_height = fields_area.height.saturating_sub(2);
        if max_width < 22 || max_height < 8 {
            return;
        }

        let width = max_width.min(62);
        let height = max_height.clamp(8, 14);
        let theme_area = Rect {
            x: fields_area.x + 1,
            y: fields_area.y + 1,
            width,
            height,
        };

        let theme_area = theme_area.intersection(fields_area);
        f.render_widget(Clear, theme_area);

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

    if app.settings.editing
        && app.settings.active_category == SettingsCategory::S3
        && app.settings.selected_field == 0
    {
        let max_width = fields_area.width.saturating_sub(2);
        let max_height = fields_area.height.saturating_sub(2);
        if max_width < 22 || max_height < 6 {
            return;
        }

        let width = max_width.min(42);
        let height = max_height.clamp(6, 14);
        let profile_area = Rect {
            x: fields_area.x + 1,
            y: fields_area.y + 1,
            width,
            height,
        };
        let profile_area = profile_area.intersection(fields_area);
        f.render_widget(Clear, profile_area);

        let list_block = Block::default()
            .borders(Borders::ALL)
            .border_type(app.theme.border_type)
            .title(" Select S3 Profile ")
            .border_style(app.theme.border_active_style())
            .style(app.theme.panel_style());

        let total = app.settings.s3_profile_count();
        if total == 0 {
            let list = List::new(vec![ListItem::new(" No S3 profiles configured ")])
                .block(list_block)
                .style(app.theme.text_style());
            f.render_widget(list, profile_area);
            return;
        }

        let selected = app.settings.s3_profile_index.min(total.saturating_sub(1));
        let max_visible = profile_area.height.saturating_sub(2) as usize;
        let mut start = 0usize;
        if max_visible > 0 && total > max_visible {
            start = selected.saturating_sub(max_visible / 2);
            if start + max_visible > total {
                start = total.saturating_sub(max_visible);
            }
        }
        let end = if max_visible == 0 {
            total
        } else {
            (start + max_visible).min(total)
        };

        let visible_items: Vec<ListItem> = app
            .settings
            .s3_profiles
            .iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .enumerate()
            .map(|(idx, profile)| {
                let absolute = start + idx;
                let style = if absolute == selected {
                    app.theme.selection_style()
                } else {
                    app.theme.text_style()
                };
                ListItem::new(format!(" {} ", profile.name)).style(style)
            })
            .collect();

        let mut list_state = ratatui::widgets::ListState::default();
        list_state.select(Some(selected.saturating_sub(start)));
        let list = List::new(visible_items).block(list_block);
        f.render_stateful_widget(list, profile_area, &mut list_state);
    }
}

fn render_theme_preview(f: &mut Frame, theme: &Theme, area: Rect) {
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

fn draw_wizard(f: &mut Frame, app: &App) {
    struct WizardFieldSpec<'a> {
        label: &'a str,
        value: String,
        required: bool,
        secret: bool,
        toggle: bool,
        help: &'a str,
    }

    let area = f.size();
    let wizard_area = centered_rect(area, 74, 78);

    draw_shadow(f, wizard_area, app);
    f.render_widget(Clear, wizard_area);
    f.render_widget(Block::default().style(app.theme.base_style()), wizard_area);

    let (step_idx, step_title, step_subtitle) = match app.wizard.step {
        WizardStep::Paths => (
            1usize,
            "Storage Paths",
            "Choose where quarantine, reports, and state data are stored.",
        ),
        WizardStep::Scanner => (
            2usize,
            "Scanner",
            "Configure scanner connectivity and scanning controls.",
        ),
        WizardStep::S3 => (
            3usize,
            "S3 Credentials",
            "Set destination bucket, path prefix, and credentials.",
        ),
        WizardStep::Performance => (
            4usize,
            "Performance",
            "Tune throughput and runtime behavior flags.",
        ),
        WizardStep::Done => (
            5usize,
            "Review & Finish",
            "Setup is complete. Save and start using Drifter.",
        ),
    };
    let setup_steps = 4usize;
    let progress_step = step_idx.min(setup_steps);
    let bar_width = 28usize;
    let filled = ((progress_step * bar_width) + (setup_steps / 2)) / setup_steps;
    let progress_bar = format!(
        "{}{}",
        "█".repeat(filled),
        "░".repeat(bar_width.saturating_sub(filled))
    );

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .title(" Setup Wizard ")
        .border_style(app.theme.modal_border_style());

    let inner = block.inner(wizard_area);
    f.render_widget(block, wizard_area);

    let root_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(inner);

    let header_lines = vec![
        Line::from(vec![
            Span::styled("Initial Setup", app.theme.highlight_style()),
            Span::styled(
                format!("  ·  Step {}/{}", progress_step, setup_steps),
                app.theme.muted_style(),
            ),
        ]),
        Line::from(Span::styled(
            format!("[{}] {}", progress_bar, step_title),
            app.theme.text_style(),
        )),
        Line::from(Span::styled(step_subtitle, app.theme.dim_style())),
    ];
    let header = Paragraph::new(header_lines).style(app.theme.text_style());
    f.render_widget(header, root_chunks[0]);

    let body_shell = Block::default()
        .borders(Borders::ALL)
        .border_type(app.theme.border_type)
        .border_style(app.theme.border_style())
        .style(app.theme.panel_style());
    let body_inner = body_shell.inner(root_chunks[1]);
    f.render_widget(body_shell, root_chunks[1]);

    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(22), Constraint::Min(0)])
        .split(body_inner);

    let step_rows = [
        (WizardStep::Paths, "1  Paths"),
        (WizardStep::Scanner, "2  Scanner"),
        (WizardStep::S3, "3  S3"),
        (WizardStep::Performance, "4  Performance"),
        (WizardStep::Done, "5  Finish"),
    ];
    let step_items: Vec<ListItem> = step_rows
        .iter()
        .map(|(step, label)| {
            let style = if *step == app.wizard.step {
                app.theme.selection_style()
            } else if (*step as usize) < (app.wizard.step as usize) {
                app.theme.status_style(StatusKind::Success)
            } else {
                app.theme.text_style()
            };
            ListItem::new(format!(" {} ", label)).style(style)
        })
        .collect();
    let step_block = Block::default()
        .borders(Borders::RIGHT)
        .border_type(app.theme.border_type)
        .title(" Steps ")
        .border_style(app.theme.border_style())
        .style(app.theme.base_style());
    let step_list = List::new(step_items).block(step_block);
    f.render_widget(step_list, body_chunks[0]);

    let content_area = Rect {
        x: body_chunks[1].x + 1,
        y: body_chunks[1].y,
        width: body_chunks[1].width.saturating_sub(1),
        height: body_chunks[1].height,
    };
    if app.wizard.step == WizardStep::Done {
        let secret_display = if app.wizard.secret_key.trim().is_empty() {
            "(empty)".to_string()
        } else {
            "********".to_string()
        };
        let summary_lines = vec![
            Line::from(Span::styled(
                "Configuration summary:",
                app.theme.highlight_style(),
            )),
            Line::from(""),
            Line::from(format!(
                "  Quarantine: {}",
                app.wizard.quarantine_dir.trim()
            )),
            Line::from(format!("  Reports:    {}", app.wizard.reports_dir.trim())),
            Line::from(format!("  State:      {}", app.wizard.state_dir.trim())),
            Line::from(format!(
                "  Scanner:    {}",
                if app.wizard.scanner_enabled {
                    "Enabled"
                } else {
                    "Disabled"
                }
            )),
            Line::from(format!(
                "  ClamAV:     {}:{}",
                app.wizard.clamd_host, app.wizard.clamd_port
            )),
            Line::from(format!("  Scan Chunk: {} MB", app.wizard.scan_chunk_size)),
            Line::from(format!(
                "  S3 Bucket:  {}",
                if app.wizard.bucket.trim().is_empty() {
                    "(empty)"
                } else {
                    app.wizard.bucket.as_str()
                }
            )),
            Line::from(format!(
                "  S3 Prefix:  {}",
                if app.wizard.prefix.trim().is_empty() {
                    "(none)"
                } else {
                    app.wizard.prefix.as_str()
                }
            )),
            Line::from(format!("  S3 Region:  {}", app.wizard.region)),
            Line::from(format!(
                "  Endpoint:   {}",
                if app.wizard.endpoint.trim().is_empty() {
                    "(default)"
                } else {
                    app.wizard.endpoint.as_str()
                }
            )),
            Line::from(format!(
                "  Access Key: {}",
                if app.wizard.access_key.trim().is_empty() {
                    "(empty)"
                } else {
                    app.wizard.access_key.as_str()
                }
            )),
            Line::from(format!("  Secret:     {}", secret_display)),
            Line::from(format!("  Part Size:  {} MB", app.wizard.part_size)),
            Line::from(format!(
                "  Upload Concurrency: {}",
                app.wizard.concurrency_upload_global
            )),
            Line::from(format!(
                "  Scan Concurrency:   {}",
                app.wizard.concurrency_scan_global
            )),
            Line::from(format!(
                "  Upload Streams:     {}",
                app.wizard.concurrency_upload_parts
            )),
            Line::from(format!(
                "  Scan Streams:       {}",
                app.wizard.concurrency_scan_parts
            )),
            Line::from(format!(
                "  Delete Source:      {}",
                if app.wizard.delete_source_after_upload {
                    "Enabled"
                } else {
                    "Disabled"
                }
            )),
            Line::from(format!(
                "  Metrics:            {}",
                if app.wizard.host_metrics_enabled {
                    "Enabled"
                } else {
                    "Disabled"
                }
            )),
            Line::from(""),
            Line::from(Span::styled(
                "Press Enter to save configuration and start.",
                app.theme.status_style(StatusKind::Success),
            )),
        ];
        let summary = Paragraph::new(summary_lines)
            .wrap(Wrap { trim: true })
            .style(app.theme.text_style());
        f.render_widget(summary, content_area);
    } else {
        let specs: Vec<WizardFieldSpec<'_>> = match app.wizard.step {
            WizardStep::Paths => vec![
                WizardFieldSpec {
                    label: "Quarantine Directory",
                    value: app.wizard.quarantine_dir.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Infected files are moved here after a positive scan.",
                },
                WizardFieldSpec {
                    label: "Reports Directory",
                    value: app.wizard.reports_dir.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Session reports and transfer summaries are written here.",
                },
                WizardFieldSpec {
                    label: "State Directory",
                    value: app.wizard.state_dir.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Database and runtime state are stored here.",
                },
            ],
            WizardStep::Scanner => vec![
                WizardFieldSpec {
                    label: "Enable Scanner",
                    value: if app.wizard.scanner_enabled {
                        "Enabled".to_string()
                    } else {
                        "Disabled".to_string()
                    },
                    required: true,
                    secret: false,
                    toggle: true,
                    help: "Enable or disable malware scanning before transfer.",
                },
                WizardFieldSpec {
                    label: "ClamAV Host",
                    value: app.wizard.clamd_host.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Host or IP for the ClamAV daemon.",
                },
                WizardFieldSpec {
                    label: "ClamAV Port",
                    value: app.wizard.clamd_port.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "TCP port for ClamAV (default 3310).",
                },
                WizardFieldSpec {
                    label: "Scan Chunk Size (MB)",
                    value: app.wizard.scan_chunk_size.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Chunk size used while streaming data to scanner.",
                },
            ],
            WizardStep::S3 => vec![
                WizardFieldSpec {
                    label: "Bucket Name",
                    value: app.wizard.bucket.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Target bucket for uploads.",
                },
                WizardFieldSpec {
                    label: "Prefix",
                    value: app.wizard.prefix.clone(),
                    required: false,
                    secret: false,
                    toggle: false,
                    help: "Optional path prefix prepended to object keys.",
                },
                WizardFieldSpec {
                    label: "Region",
                    value: app.wizard.region.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "AWS/S3 region for API requests.",
                },
                WizardFieldSpec {
                    label: "Endpoint",
                    value: app.wizard.endpoint.clone(),
                    required: false,
                    secret: false,
                    toggle: false,
                    help: "Custom endpoint URL for S3-compatible providers.",
                },
                WizardFieldSpec {
                    label: "Access Key",
                    value: app.wizard.access_key.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Access key ID used for S3 authentication.",
                },
                WizardFieldSpec {
                    label: "Secret Key",
                    value: app.wizard.secret_key.clone(),
                    required: true,
                    secret: true,
                    toggle: false,
                    help: "Secret key used to sign S3 requests.",
                },
            ],
            WizardStep::Performance => vec![
                WizardFieldSpec {
                    label: "Part Size (MB)",
                    value: app.wizard.part_size.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Multipart part size; impacts memory and retry overhead.",
                },
                WizardFieldSpec {
                    label: "Global Upload Concurrency",
                    value: app.wizard.concurrency_upload_global.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Number of files uploaded in parallel.",
                },
                WizardFieldSpec {
                    label: "Global Scan Concurrency",
                    value: app.wizard.concurrency_scan_global.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Number of files scanned in parallel.",
                },
                WizardFieldSpec {
                    label: "Upload Part Concurrency",
                    value: app.wizard.concurrency_upload_parts.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Multipart upload streams per file.",
                },
                WizardFieldSpec {
                    label: "Scanner Chunk Concurrency",
                    value: app.wizard.concurrency_scan_parts.clone(),
                    required: true,
                    secret: false,
                    toggle: false,
                    help: "Scanner workers per file for chunked scanning.",
                },
                WizardFieldSpec {
                    label: "Delete Source After Upload",
                    value: if app.wizard.delete_source_after_upload {
                        "Enabled".to_string()
                    } else {
                        "Disabled".to_string()
                    },
                    required: false,
                    secret: false,
                    toggle: true,
                    help: "Delete local files after successful upload.",
                },
                WizardFieldSpec {
                    label: "Show Host Metrics",
                    value: if app.wizard.host_metrics_enabled {
                        "Enabled".to_string()
                    } else {
                        "Disabled".to_string()
                    },
                    required: false,
                    secret: false,
                    toggle: true,
                    help: "Show or hide CPU, RAM, and network stats in footer.",
                },
            ],
            WizardStep::Done => Vec::new(),
        };

        let content_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Min(0),
                Constraint::Length(2),
            ])
            .split(content_area);

        let intro = Paragraph::new(Span::styled(step_subtitle, app.theme.dim_style()));
        f.render_widget(intro, content_chunks[0]);

        let fields_area = content_chunks[1];
        let display_height = fields_area.height as usize;
        let fields_per_view = (display_height / 3).max(1);
        let mut offset = 0usize;
        if app.wizard.field >= fields_per_view {
            offset = app.wizard.field.saturating_sub(fields_per_view / 2);
        }
        if specs.len() > fields_per_view && offset + fields_per_view > specs.len() {
            offset = specs.len() - fields_per_view;
        }

        let field_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![Constraint::Length(3); fields_per_view])
            .split(fields_area);

        let mut active_help = "";
        for (visible_idx, (i, spec)) in specs
            .iter()
            .enumerate()
            .skip(offset)
            .take(fields_per_view)
            .enumerate()
        {
            let is_selected = i == app.wizard.field;
            let is_editing = is_selected && app.wizard.editing && !spec.toggle;
            if is_selected {
                active_help = spec.help;
            }

            let title = if spec.required {
                format!("{} *", spec.label)
            } else {
                spec.label.to_string()
            };
            let border_style = if is_selected {
                if is_editing {
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
                .title(title)
                .border_style(border_style)
                .style(app.theme.panel_style());

            let display_value = if spec.toggle {
                if spec.value == "Enabled" {
                    "[X] Enabled".to_string()
                } else {
                    "[ ] Disabled".to_string()
                }
            } else if spec.secret && !is_editing {
                if spec.value.trim().is_empty() {
                    "(required)".to_string()
                } else {
                    "********".to_string()
                }
            } else if spec.value.trim().is_empty() {
                if spec.required {
                    "(required)".to_string()
                } else {
                    "(optional)".to_string()
                }
            } else if is_editing {
                format!("{}█", spec.value)
            } else {
                spec.value.clone()
            };
            let value_style = if is_editing {
                app.theme.input_style(true)
            } else if is_selected {
                app.theme.text_style().add_modifier(Modifier::BOLD)
            } else {
                app.theme.text_style()
            };
            let field_widget = Paragraph::new(display_value)
                .block(block)
                .style(value_style);
            f.render_widget(field_widget, field_chunks[visible_idx]);

            if is_editing {
                f.set_cursor(
                    field_chunks[visible_idx].x + 1 + spec.value.len() as u16,
                    field_chunks[visible_idx].y + 1,
                );
            }
        }

        let help_text = Paragraph::new(active_help)
            .style(app.theme.muted_style())
            .wrap(Wrap { trim: true });
        f.render_widget(help_text, content_chunks[2]);
    }

    let nav_text = if app.wizard.step == WizardStep::Done {
        "Enter Save & Continue  │  Shift+Tab Back  │  q Quit"
    } else if app.wizard.editing {
        "Type value  │  Enter Save Field  │  Esc Cancel Edit"
    } else {
        "↑/↓ Field  │  Enter Edit/Toggle  │  Space Toggle  │  s Skip  │  Tab/Shift+Tab Step  │  q Quit"
    };
    let nav = Paragraph::new(nav_text)
        .style(app.theme.dim_style())
        .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(nav, root_chunks[2]);
}

fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    let sep = || Span::styled("  │  ".to_string(), app.theme.muted_style());
    let key = |k: &str| {
        Span::styled(
            k.to_string(),
            app.theme.accent_style().add_modifier(Modifier::BOLD),
        )
    };
    let act = |a: &str| Span::styled(a.to_string(), app.theme.muted_style());

    let hints = footer_hints(app);
    let mut footer_spans = Vec::new();
    for (idx, hint) in hints.iter().enumerate() {
        if idx > 0 {
            footer_spans.push(sep());
        }

        if let Some(binding) = hint.key {
            footer_spans.push(key(binding));
            footer_spans.push(act(&format!(" {}", hint.action)));
        } else {
            footer_spans.push(act(hint.action));
        }
    }

    let av_status = lock_mutex(&app.clamav_status)
        .map(|s| s.clone())
        .unwrap_or("Unknown".to_string());
    let s3_status = if app.settings.bucket.is_empty() {
        "Not Configured"
    } else {
        "Ready"
    };

    let status_style = app
        .theme
        .status_style(status_kind_for_message(&app.status_message));

    let mut right_spans = vec![];

    if !app.status_message.is_empty() && app.status_message != "Ready" {
        right_spans.push(Span::styled("● ", status_style));
        right_spans.push(Span::styled(&app.status_message, app.theme.text_style()));
        right_spans.push(Span::styled("  │  ", app.theme.muted_style()));
    }

    right_spans.extend(vec![
        Span::styled("ClamAV: ", app.theme.muted_style()),
        Span::styled(
            av_status,
            if lock_mutex(&app.clamav_status)
                .map(|s| s.contains("Ready"))
                .unwrap_or(false)
            {
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

    let left_content = Line::from(footer_spans);
    let right_content = Line::from(right_spans);

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

fn draw_layout_adjustment_overlay(f: &mut Frame, app: &App, area: Rect) {
    if app.input_mode != InputMode::LayoutAdjust {
        return;
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(" Layout Adjustment Mode ")
        .style(app.theme.modal_style())
        .border_style(app.theme.modal_border_style());

    let popup_area = centered_fixed_rect(area, 108, 17);

    draw_shadow(f, popup_area, app);
    f.render_widget(Clear, popup_area);
    f.render_widget(block.clone(), popup_area);

    let inner = block.inner(popup_area);

    let cfg = &app.cached_config;
    let current = app.layout_adjust_target;

    let highlight = app.theme.selection_style();
    let normal = app.theme.text_style();

    let queue_width = 100 - cfg.local_width_percent;

    let lines = vec![
        Line::from("Select a panel to adjust:"),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                "1. ",
                if current == Some(LayoutTarget::Local) {
                    highlight
                } else {
                    normal
                },
            ),
            Span::styled(format!("Local: {}%", cfg.local_width_percent), normal),
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

fn draw_confirmation_modal(f: &mut Frame, app: &App, area: Rect) {
    if app.input_mode != InputMode::Confirmation {
        return;
    }

    let area = centered_fixed_rect(area, 60, 8);

    draw_shadow(f, area, app);
    f.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(" Confirmation ")
        .style(app.theme.modal_style())
        .border_style(app.theme.modal_border_style());

    let content_area = block.inner(area);
    f.render_widget(block, area);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(1),
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

fn draw_metrics_panel(f: &mut Frame, app: &App, area: Rect) {
    let m = &app.last_metrics;
    let read_fmt = format_bytes_rate(m.disk_read_bytes_sec);
    let write_fmt = format_bytes_rate(m.disk_write_bytes_sec);
    let rx_fmt = format_bytes_rate(m.net_rx_bytes_sec);
    let tx_fmt = format_bytes_rate(m.net_tx_bytes_sec);

    let mk_style = app.theme.accent_style().add_modifier(Modifier::BOLD);
    let mk_val = app.theme.text_style();

    // Join with the panel above and the main panel on the left
    let block = Block::default()
        .borders(Borders::RIGHT | Borders::BOTTOM)
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

fn draw_remote_folder_create_modal(f: &mut Frame, app: &App, area: Rect) {
    if app.input_mode != InputMode::RemoteFolderCreate {
        return;
    }

    let area = centered_fixed_rect(area, 60, 10);

    draw_shadow(f, area, app);
    f.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(app.theme.modal_border_style())
        .title(" Create Remote Folder ")
        .style(app.theme.modal_style());

    f.render_widget(block, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(2)
        .constraints([
            Constraint::Length(1), // Prompt
            Constraint::Length(3), // Input box
            Constraint::Length(1), // Help
        ])
        .split(area);

    let prompt = Paragraph::new("Enter folder name:").style(app.theme.text_style());
    f.render_widget(prompt, chunks[0]);

    let input = Paragraph::new(app.creating_folder_name.as_str())
        .style(app.theme.text_style().add_modifier(Modifier::BOLD))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(app.theme.border_active_style()),
        );
    f.render_widget(input, chunks[1]);

    // Cursor
    f.set_cursor(
        chunks[1].x + 1 + app.creating_folder_name.len() as u16,
        chunks[1].y + 1,
    );

    let help = Paragraph::new("Enter: Create | Esc: Cancel")
        .style(app.theme.text_style().add_modifier(Modifier::DIM))
        .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(help, chunks[2]);
}
