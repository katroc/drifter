use crate::app::state::{App, AppFocus, InputMode};
use crate::ui::theme::Theme;
use crate::ui::util::format_bytes;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState},
};

pub fn render_remote(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0)])
        .split(area);

    // S3 Object List
    let items: Vec<ListItem> = app
        .s3_objects
        .iter()
        .map(|obj| {
            let (icon, style) = if obj.is_dir {
                ("📁", theme.text_style().fg(theme.info))
            } else {
                ("📄", theme.text_style())
            };

            let size_str = if obj.is_dir {
                "DIR".to_string()
            } else {
                format_bytes(obj.size as u64)
            };

            let display_name = if obj.is_dir && !obj.name.ends_with('/') {
                format!("{}/", obj.name)
            } else {
                obj.name.clone()
            };

            let content = Line::from(vec![
                Span::styled(format!("{} {:<60}", icon, display_name), style),
                Span::styled(
                    format!("{:>10}", size_str),
                    theme.text_style().add_modifier(Modifier::DIM),
                ),
                Span::styled(
                    format!("  {}", obj.last_modified),
                    theme.text_style().add_modifier(Modifier::DIM),
                ),
            ]);
            ListItem::new(content)
        })
        .collect();

    let border_style = if app.focus == AppFocus::Remote {
        if app.input_mode == InputMode::RemoteBrowsing {
            theme.highlight_style() // Indicate active browsing
        } else {
            theme.border_active_style()
        }
    } else {
        theme.border_style()
    };

    let p = if app.remote_current_path.is_empty() {
        "Root".to_string()
    } else {
        app.remote_current_path.clone()
    };

    let mode_indicator = if app.input_mode == InputMode::RemoteBrowsing {
        " [BROWSING]"
    } else {
        ""
    };

    let title = format!(
        " Remote (S3): {} [Path: {}]{} ",
        app.s3_objects.len(),
        p,
        mode_indicator
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .title(Span::styled(title, theme.header_style()));

    let mut state = ListState::default();
    state.select(Some(app.selected_remote));

    f.render_stateful_widget(
        List::new(items)
            .block(block)
            .highlight_style(theme.selection_style())
            .highlight_symbol("> "),
        chunks[0],
        &mut state,
    );
}
