use crate::app::state::{App, AppFocus, InputMode};
use crate::ui::theme::Theme;
use crate::ui::util::format_bytes;
use ratatui::{
    Frame,
    layout::Rect,
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};

pub fn render_remote(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    // S3 Object List
    let is_focused = app.focus == AppFocus::Remote;
    let items: Vec<ListItem> = app
        .s3_objects
        .iter()
        .map(|obj| {
            let (icon, base_style) = if obj.is_dir {
                ("📁", theme.text_style().fg(theme.info))
            } else {
                ("📄", theme.text_style())
            };

            // Apply dimming if panel is not focused
            let style = if !is_focused {
                base_style.patch(theme.dim_style())
            } else {
                base_style
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

    let is_focused = app.focus == AppFocus::Remote;
    let panel_style = if is_focused {
        theme.panel_style()
    } else {
        theme.panel_style().patch(theme.dim_style())
    };

    let borders = Borders::TOP | Borders::BOTTOM | Borders::RIGHT;

    // Complete borders for consistency
    let block = Block::default()
        .borders(borders)
        .border_type(theme.border_type)
        .border_style(border_style)
        .title(title)
        .style(panel_style);

    let inner_area = block.inner(area);
    f.render_widget(block, area);

    let header_area = Rect::new(inner_area.x, inner_area.y, inner_area.width, 1);
    let list_area = Rect::new(
        inner_area.x,
        inner_area.y + 1,
        inner_area.width,
        inner_area.height.saturating_sub(1),
    );

    let header_line = format!("{:<62}{:>10}  {}", "Name", "Size", "Modified");
    f.render_widget(
        Paragraph::new(header_line).style(theme.header_style()),
        header_area,
    );

    let mut state = ListState::default();
    state.select(Some(app.selected_remote));

    f.render_stateful_widget(
        List::new(items)
            .highlight_style(theme.selection_style())
            .highlight_symbol("> "),
        list_area,
        &mut state,
    );
}
