use crate::app::state::{App, InputMode};
use crate::ui::theme::StatusKind;
use crate::ui::theme::Theme;
use crate::ui::util::{format_bytes, truncate_with_ellipsis};
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Row, Table, TableState},
};

#[allow(clippy::too_many_arguments)]
pub fn render_remote(
    f: &mut Frame,
    app: &App,
    area: Rect,
    theme: &Theme,
    role_label: &str,
    is_left_panel: bool,
    objects: &[crate::services::uploader::S3Object],
    current_path: &str,
    selected: usize,
    loading: bool,
    focused: bool,
    selected_items: Option<&std::collections::HashMap<String, crate::services::uploader::S3Object>>,
) {
    let border_style = if focused {
        if app.input_mode == InputMode::RemoteBrowsing {
            theme.status_style(StatusKind::Success)
        } else {
            theme.border_active_style()
        }
    } else {
        theme.border_style()
    };

    let p = if current_path.is_empty() {
        "Root".to_string()
    } else {
        current_path.to_string()
    };

    let mode_indicator = if loading {
        " [Loading...]"
    } else if app.input_mode == InputMode::RemoteBrowsing {
        " [BROWSING]"
    } else {
        ""
    };

    let title = format!(
        " {}: {} [Path: {}]{} ",
        role_label,
        objects.len(),
        p,
        mode_indicator
    );

    let panel_style = if focused {
        theme.panel_style()
    } else {
        theme.panel_style().patch(theme.dim_style())
    };

    let borders = if is_left_panel {
        Borders::ALL
    } else {
        Borders::TOP | Borders::BOTTOM | Borders::RIGHT
    };

    // Keep left border off to avoid double center divider with Local panel.
    let block = Block::default()
        .borders(borders)
        .border_type(theme.border_type)
        .border_style(border_style)
        .title(title)
        .style(panel_style);

    let inner_area = block.inner(area);
    f.render_widget(block, area);

    if inner_area.width < 12 || inner_area.height < 2 {
        return;
    }

    let column_mode = if inner_area.width < 28 {
        1usize
    } else if inner_area.width < 42 {
        2usize
    } else {
        3usize
    };
    let max_name_chars = match column_mode {
        1 => usize::from(inner_area.width.saturating_sub(4)).max(6),
        2 => usize::from(inner_area.width.saturating_sub(14)).max(8),
        _ => usize::from(inner_area.width.saturating_sub(27)).max(8),
    };
    let rows: Vec<Row> = objects
        .iter()
        .map(|obj| {
            let is_marked =
                selected_items.is_some_and(|items| !obj.is_parent && items.contains_key(&obj.key));

            let (icon, mut name_style) = if obj.is_dir && !is_marked {
                ("📁", theme.text_style().fg(theme.info))
            } else if obj.is_dir {
                ("📁", theme.status_style(StatusKind::Success))
            } else if is_marked {
                ("📄", theme.status_style(StatusKind::Success))
            } else {
                ("📄", theme.text_style())
            };

            if !focused {
                name_style = name_style.patch(theme.dim_style());
            }

            let mut meta_style = if is_marked {
                theme
                    .status_style(StatusKind::Success)
                    .add_modifier(Modifier::DIM)
            } else {
                theme.text_style().add_modifier(Modifier::DIM)
            };
            if !focused {
                meta_style = meta_style.patch(theme.dim_style());
            }

            let size_str = if obj.is_parent {
                String::new()
            } else if obj.is_dir {
                "DIR".to_string()
            } else {
                format_bytes(obj.size as u64)
            };

            let display_name = if obj.is_parent {
                "..".to_string()
            } else if obj.is_dir && !obj.name.ends_with('/') {
                format!("{}/", obj.name)
            } else {
                obj.name.clone()
            };
            let clipped_name = truncate_with_ellipsis(&display_name, max_name_chars);

            let mut cells = vec![Cell::from(Line::from(vec![Span::styled(
                format!("{icon} {clipped_name}"),
                name_style,
            )]))];

            if column_mode >= 2 {
                cells.push(Cell::from(size_str).style(meta_style));
            }
            if column_mode >= 3 {
                cells.push(Cell::from(obj.last_modified.clone()).style(meta_style));
            }

            Row::new(cells)
        })
        .collect();

    let header = match column_mode {
        1 => Row::new(vec![Cell::from("Name").style(theme.header_style())]),
        2 => Row::new(vec![
            Cell::from("Name").style(theme.header_style()),
            Cell::from("Size").style(theme.header_style()),
        ]),
        _ => Row::new(vec![
            Cell::from("Name").style(theme.header_style()),
            Cell::from("Size").style(theme.header_style()),
            Cell::from("Modified").style(theme.header_style()),
        ]),
    };

    let mut state = TableState::default();
    if !objects.is_empty() {
        state.select(Some(selected.min(objects.len() - 1)));
    }

    let table = match column_mode {
        1 => Table::new(rows, [Constraint::Min(6)]),
        2 => Table::new(rows, [Constraint::Min(8), Constraint::Length(10)]),
        _ => Table::new(
            rows,
            [
                Constraint::Min(8),
                Constraint::Length(10),
                Constraint::Length(16),
            ],
        ),
    }
    .header(header)
    // Keep selected-row background emphasis without forcing a foreground color,
    // so multi-selected items remain visibly marked even when cursor-selected.
    .highlight_style(
        Style::default()
            .bg(theme.selection_bg)
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("> ");

    f.render_stateful_widget(table, inner_area, &mut state);
}
