use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState},
    Frame,
};
use crate::app::state::{App, AppFocus};
use crate::ui::theme::Theme;
use crate::ui::util::format_bytes;

pub fn render_remote(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),
        ])
        .split(area);



    // S3 Object List
    let items: Vec<ListItem> = app.s3_objects
        .iter()
        .map(|obj| {
            let style = if obj.key.ends_with('/') {
                theme.text_style().add_modifier(Modifier::BOLD) 
            } else {
                theme.text_style()
            };
            
            let size_str = format_bytes(obj.size as u64);
            // Just display key and size for now
            let content = Line::from(vec![
                Span::styled(format!("{:<60}", obj.key), style),
                Span::styled(format!("{:>10}", size_str), theme.text_style().add_modifier(Modifier::DIM)),
                Span::styled(format!("  {}", obj.last_modified), theme.text_style().add_modifier(Modifier::DIM)),
            ]);
             ListItem::new(content)
        })
        .collect();

    let border_style = if app.focus == AppFocus::Remote {
         theme.border_active_style()
    } else {
         theme.border_style() // Use border_style instead of border_dim
    };

    let title = format!(" Remote (S3): {} items ", app.s3_objects.len());
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
        &mut state
    );
}
