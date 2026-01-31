use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Frame,
};
use crate::app::state::{App, InputMode};
use crate::ui::theme::Theme;

pub fn render_logs(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let (list_area, search_area) = if app.input_mode == InputMode::LogSearch || app.log_search_active {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(0),
                Constraint::Length(3),
            ])
            .split(area);
        (chunks[0], Some(chunks[1]))
    } else {
        (area, None)
    };

    let block_style = if app.focus == crate::app::state::AppFocus::Logs || app.log_search_active {
         theme.border_active_style()
    } else {
         // When selected in Rail but not focused, show brighter than default
         theme.text_style() 
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" System Logs ")
        .border_style(block_style)
        .style(theme.panel_style());

    let _inner_area = block.inner(list_area);

    let items: Vec<ListItem> = app.logs
        .iter()
        .enumerate()
        .map(|(i, line)| {
            // VecDeque access by index is O(1)
            let is_match = app.log_search_results.contains(&i);
            // Current match is only highlighted if we have results and index matches
            // However, typical 'less' behavior is to just scroll to it. 
            // Better to highlight it distinctly.
            let is_current = !app.log_search_results.is_empty() 
                 && app.log_search_results.get(app.log_search_current) == Some(&i);

            let style = if is_current {
                Style::default().fg(Color::Black).bg(Color::Yellow)
            } else if is_match {
                Style::default().fg(Color::Yellow)
            } else {
                theme.text_style()
            };

            ListItem::new(Line::from(line.as_str()))
                .style(style)
        })
        .collect();

    let mut state = ListState::default();
    
    // Auto-scroll logic
    if app.logs_stick_to_bottom && !app.logs.is_empty() {
        state.select(Some(app.logs.len() - 1));
    } else if app.logs_scroll < app.logs.len() {
        state.select(Some(app.logs_scroll));
    } else if !app.logs.is_empty() {
         state.select(Some(app.logs.len() - 1));
    }

    f.render_stateful_widget(
        List::new(items)
            .block(block)
            .highlight_style(if app.log_search_active && !app.log_search_results.is_empty() {
                 Style::default().add_modifier(Modifier::REVERSED)
            } else {
                 Style::default().add_modifier(Modifier::REVERSED)
            }), 
        list_area,
        &mut state
    );

    if let Some(s_area) = search_area {
        let title = if !app.log_search_results.is_empty() {
             format!(" Search ({} matches) ", app.log_search_results.len())
        } else {
             " Search ".to_string()
        };
        
        // Active border if typing
        let border_style = if app.input_mode == InputMode::LogSearch {
             theme.accent_style()
        } else {
             theme.border_style()
        };

        let search_block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(border_style);
            
        let query_text = format!("{}{}", app.log_search_query, if app.input_mode == InputMode::LogSearch { "█" } else { "" });
        
        f.render_widget(
            Paragraph::new(query_text).block(search_block).style(theme.text_style()),
            s_area
        );
    }
}
