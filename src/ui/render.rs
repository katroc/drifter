use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Wrap, Clear},
    Frame,
};

use crate::app::state::{App, AppFocus, AppTab, InputMode, LayoutTarget};
use crate::app::settings::SettingsCategory;
use crate::components::file_picker::FileEntry;
use crate::components::wizard::WizardStep;
use crate::ui::theme::{StatusKind, Theme};
use crate::utils::lock_mutex;

use crate::ui::util::{
    centered_rect, centered_fixed_rect, format_bytes, format_bytes_rate,
    format_relative_time, format_size, format_modified, fuzzy_match,
    status_kind, extract_threat_name, calculate_list_offset
};

pub fn ui(f: &mut Frame, app: &App) {
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

    let cfg_guard = if let Ok(c) = lock_mutex(&app.config) { c } else { return };
    let main_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(14),
            Constraint::Min(0),
            Constraint::Length(cfg_guard.history_width),
        ])
        .split(root[0]);
    drop(cfg_guard);

    draw_rail(f, app, main_layout[0]);

    match app.current_tab {
        AppTab::Transfers => draw_transfers(f, app, main_layout[1]),
        AppTab::Quarantine => draw_quarantine(f, app, main_layout[1]),
        AppTab::Remote => crate::ui::remote::render_remote(f, app, main_layout[1], &app.theme),
        AppTab::Logs => crate::ui::logs::render_logs(f, app, main_layout[1], &app.theme),
        AppTab::Settings => draw_settings(f, app, main_layout[1]),
    }

    // Render History / Details panel (Right side) - With Metrics support
    let right_panel_full = main_layout[2];

    // Check if metrics enabled
    let metrics_enabled = lock_mutex(&app.config).map(|c| c.host_metrics_enabled).unwrap_or(false);
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

fn draw_rail(f: &mut Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::Rail {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let items = [" Transfers ", " Quarantine ", " Remote    ", " Logs      ", " Settings  "];
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

fn draw_transfers(f: &mut Frame, app: &App, area: Rect) {
    let hopper_percent = lock_mutex(&app.config).map(|c| c.hopper_width_percent).unwrap_or(50);
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

fn draw_browser(f: &mut Frame, app: &App, area: Rect) {
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

    let title = match app.input_mode {
        InputMode::Browsing => " Hopper (Browsing) ",
        InputMode::Filter => " Hopper (Filter) ",
        InputMode::Normal if is_focused => " Hopper (Press 'a' to browse) ",
        InputMode::Normal => " Hopper ",
        InputMode::Confirmation => " Hopper ",
        InputMode::LayoutAdjust => " Layout ",
        InputMode::LogSearch => " Search ",
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

    let has_filter = !app.input_buffer.is_empty() || app.input_mode == InputMode::Filter;
    let table_area = Rect::new(
        inner_area.x,
        inner_area.y + if has_filter { 2 } else { 1 },
        inner_area.width,
        inner_area.height.saturating_sub(if has_filter { 2 } else { 1 }),
    );

    // Live Filtering
    let filter = app.input_buffer.clone();

    let filtered_entries: Vec<(usize, &FileEntry)> = app
        .picker
        .entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.is_parent || filter.is_empty() || fuzzy_match(&filter, &e.name))
        .collect();

    let total_rows = filtered_entries.len();
    let display_height = table_area.height.saturating_sub(1) as usize; // -1 for header

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

fn draw_jobs(f: &mut Frame, app: &App, area: Rect) {
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
                let job = &app.jobs[job_idx];

                let p_str = {
                    if job.status == "uploading" {
                        if let Ok(p_map) = lock_mutex(&app.progress) {
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
                        } else {
                            "----------".to_string()
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

fn draw_history(f: &mut Frame, app: &App, area: Rect) {
    let focus_style = if app.focus == AppFocus::History {
        app.theme.border_active_style()
    } else {
        app.theme.border_style()
    };

    let header_cells = ["File", "Status", "Size", "Time"]
        .iter()
        .map(|h| Cell::from(*h).style(app.theme.header_style()));
    let header = Row::new(header_cells)
        .style(app.theme.header_style())
        .height(1);

    let total_rows = app.visual_history.len();
    let display_height = area.height.saturating_sub(4) as usize; 
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

                let status_str = match job.status.as_str() {
                    "quarantined" => "Threat Detected",
                    "quarantined_removed" => "Threat Removed",
                    "complete" => "Done",
                    s => s,
                };

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

fn draw_job_details(f: &mut Frame, app: &App, area: Rect, job: &crate::db::JobRow) {
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
        if let Ok(progress_map) = lock_mutex(&app.progress) {
        if let Some(info) = progress_map.get(&job.id)
            && info.parts_total > 0
        {
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

    let categories = ["S3 Storage", "Scanner", "Performance", "Theme"];
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

    if app.settings.editing && app.settings.active_category == SettingsCategory::Theme {
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
    let area = f.size();
    let wizard_area = centered_rect(area, 60, 70);

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
            Constraint::Length(2),
            Constraint::Min(0),
            Constraint::Length(2),
        ])
        .split(inner);

    let title = Paragraph::new(step_title)
        .style(app.theme.highlight_style())
        .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(title, chunks[0]);

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

fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    let sep = || Span::styled("  │  ".to_string(), app.theme.muted_style());
    let key = |k: &str| {
        Span::styled(
            k.to_string(),
            app.theme.accent_style().add_modifier(Modifier::BOLD),
        )
    };
    let act = |a: &str| Span::styled(format!(" {}", a), app.theme.muted_style());

    let footer_spans = match app.input_mode {


        InputMode::LogSearch => vec![
            key("Enter"), act("Search"), sep(),
            key("Esc"), act("Cancel"),
        ],
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
        InputMode::Normal => if app.current_tab == AppTab::Remote {
             vec![
                 key("r"), act("Refresh"), sep(),
                 key("d"), act("Download"), sep(),
                 key("x"), act("Delete"), sep(),
                 key("Tab"), act("Nav"),
             ]
        } else { match app.focus {
            AppFocus::Logs => vec![
                key("q"), act("Back"),
            ],
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
                act("Rail"),
            ],
            AppFocus::History => vec![key("Tab"), act("Rail"), sep(), key("←"), act("Queue")],
            AppFocus::Remote => vec![ // Handled by AppTab::Remote above mostly, but kept for completeness if needed
                 key("Tab"), act("Rail"),
            ],
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
        }
        },
    };

    let av_status = lock_mutex(&app.clamav_status).map(|s| s.clone()).unwrap_or("Unknown".to_string());
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
            if lock_mutex(&app.clamav_status).map(|s| s.contains("Ready")).unwrap_or(false) {
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

fn draw_layout_adjustment_overlay(f: &mut Frame, app: &App, area: Rect) {
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
    f.render_widget(Clear, popup_area);
    f.render_widget(block.clone(), popup_area);

    let inner = block.inner(popup_area);

    let cfg = if let Ok(c) = lock_mutex(&app.config) { c } else { return };
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

fn draw_confirmation_modal(f: &mut Frame, app: &App, area: Rect) {
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
    f.render_widget(Clear, area);
    f.render_widget(block.clone(), area);

    let content_area = block.inner(area);
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
