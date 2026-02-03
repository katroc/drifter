use crate::app::state::{App, InputMode};
use crate::ui::theme::Theme;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};

pub fn render_logs(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let (list_area, search_area) =
        if app.input_mode == InputMode::LogSearch || app.log_search_active {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(0), Constraint::Length(3)])
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

    let title = format!(
        " System Logs [Level: {}] (Scroll: arrows | 1-5: Level) ",
        app.current_log_level.to_uppercase()
    );

    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_type(theme.border_type)
        .border_style(block_style)
        .style(theme.panel_style());

    let _inner_area = block.inner(list_area);

    let items: Vec<ListItem> = app
        .logs
        .iter()
        .enumerate()
        .map(|(i, line)| {
            // VecDeque access by index is O(1)
            let is_match = app.log_search_results.contains(&i);

            // Basic log level highlighting
            let mut style = theme.text_style();
            if line.contains("ERROR") {
                style = style.fg(Color::Red);
            } else if line.contains("WARN") {
                style = style.fg(Color::Yellow);
            } else if line.contains("INFO") {
                style = style.fg(Color::Cyan);
            } else if line.contains("DEBUG") {
                style = style.fg(Color::Blue);
            } else if line.contains("TRACE") {
                style = style.fg(Color::Magenta);
            }

            let is_current = !app.log_search_results.is_empty()
                && app.log_search_results.get(app.log_search_current) == Some(&i);

            if is_current {
                style = style.bg(Color::White).fg(Color::Black); // Explicit highlight
            } else if is_match {
                style = style.bg(Color::DarkGray);
            }

            // Apply horizontal scroll safely
            let display_text = if app.logs_scroll_x == 0 {
                line.as_str()
            } else {
                let start_byte = line
                    .char_indices()
                    .map(|(i, _)| i)
                    .nth(app.logs_scroll_x)
                    .unwrap_or(line.len());
                &line[start_byte..]
            };

            ListItem::new(Line::from(display_text)).style(style)
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
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED)),
        list_area,
        &mut state,
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
            .borders(Borders::LEFT | Borders::RIGHT | Borders::BOTTOM)
            .title(title)
            .border_type(theme.border_type)
            .border_style(border_style);

        let query_text = format!(
            "{}{}",
            app.log_search_query,
            if app.input_mode == InputMode::LogSearch {
                "█"
            } else {
                ""
            }
        );

        f.render_widget(
            Paragraph::new(query_text)
                .block(search_block)
                .style(theme.text_style()),
            s_area,
        );
    }
}
