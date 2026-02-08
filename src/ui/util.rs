use crate::ui::theme::StatusKind;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use std::time::SystemTime;

pub fn centered_rect(r: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

pub fn centered_fixed_rect(r: Rect, width: u16, height: u16) -> Rect {
    let empty_x = r.width.saturating_sub(width) / 2;
    let empty_y = r.height.saturating_sub(height) / 2;

    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(empty_y),
            Constraint::Length(height),
            Constraint::Length(empty_y),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(empty_x),
            Constraint::Length(width),
            Constraint::Length(empty_x),
        ])
        .split(popup_layout[1])[1]
}

pub fn calculate_list_offset(selected: usize, total: usize, display_height: usize) -> usize {
    if total <= display_height {
        return 0;
    }
    let mut offset = 0;
    if selected >= display_height / 2 {
        offset = (selected + 1).saturating_sub(display_height / 2);
    }
    if offset + display_height > total {
        offset = total.saturating_sub(display_height);
    }
    offset
}

pub fn centered_window_bounds(selected: usize, total: usize, max_visible: usize) -> (usize, usize) {
    if total == 0 {
        return (0, 0);
    }
    if max_visible == 0 {
        return (0, total);
    }

    let mut start = 0usize;
    if total > max_visible {
        start = selected.saturating_sub(max_visible / 2);
        if start + max_visible > total {
            start = total.saturating_sub(max_visible);
        }
    }
    let end = (start + max_visible).min(total);
    (start, end)
}

pub fn format_bytes_rate(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

pub fn format_bytes(value: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    const TB: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;
    let v = value as f64;
    if v >= TB {
        format!("{:.2} TB", v / TB)
    } else if v >= GB {
        format!("{:.2} GB", v / GB)
    } else if v >= MB {
        format!("{:.2} MB", v / MB)
    } else if v >= KB {
        format!("{:.2} KB", v / KB)
    } else {
        format!("{} B", value)
    }
}

pub fn format_size(size: u64) -> String {
    if size == 0 {
        return "-".to_string();
    }
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut s = size as f64;
    let mut unit = 0;
    while s >= 1024.0 && unit < UNITS.len() - 1 {
        s /= 1024.0;
        unit += 1;
    }
    format!("{:.1} {}", s, UNITS[unit])
}

pub fn format_relative_time(timestamp: &str) -> String {
    use chrono::{DateTime, Local};

    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp) {
        let now = Local::now();
        let duration = now.signed_duration_since(dt);

        let secs = duration.num_seconds();
        if secs < 60 {
            format!("{}s ago", secs)
        } else if secs < 3600 {
            format!("{}m ago", secs / 60)
        } else if secs < 86400 {
            format!("{}h ago", secs / 3600)
        } else {
            format!("{}d ago", secs / 86400)
        }
    } else {
        // Try parsing as simple datetime
        timestamp.split('T').next().unwrap_or(timestamp).to_string()
    }
}

pub fn format_modified(time: Option<SystemTime>) -> String {
    match time {
        Some(t) => {
            let datetime: chrono::DateTime<chrono::Local> = t.into();
            datetime.format("%Y-%m-%d %H:%M").to_string()
        }
        None => "-".to_string(),
    }
}

pub fn fuzzy_match(pattern: &str, text: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    let pattern = pattern.to_lowercase();
    let text = text.to_lowercase();
    text.contains(&pattern)
}

pub fn truncate_with_ellipsis(input: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    if input.chars().count() <= max_chars {
        return input.to_string();
    }

    let keep = max_chars.saturating_sub(1);
    let mut out: String = input.chars().take(keep).collect();
    out.push('…');
    out
}

pub fn status_kind(status: &str) -> StatusKind {
    match status {
        "complete" => StatusKind::Success,
        "quarantined" | "failed" => StatusKind::Error,
        "quarantined_removed" => StatusKind::Info,
        "uploading" | "transferring" | "scanning" => StatusKind::Info,
        _ => StatusKind::Warning,
    }
}

pub fn status_kind_for_message(message: &str) -> StatusKind {
    let lower = message.to_lowercase();

    if lower.is_empty() || lower == "ready" {
        return StatusKind::Info;
    }

    if [
        "fail",
        "error",
        "cannot",
        "invalid",
        "denied",
        "timed out",
        "panic",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return StatusKind::Error;
    }

    if [
        "warning",
        "retry",
        "pending",
        "disconnected",
        "not found",
        "skipped",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return StatusKind::Warning;
    }

    if [
        "saved",
        "complete",
        "completed",
        "success",
        "connected",
        "loaded",
        "created",
        "deleted",
        "downloaded",
        "queued",
        "watching",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return StatusKind::Success;
    }

    StatusKind::Info
}

pub fn extract_threat_name(scan_status: &str) -> String {
    if scan_status.contains("FOUND") {
        let parts: Vec<&str> = scan_status.split_whitespace().collect();
        if let Some(pos) = parts.iter().position(|&s| s == "FOUND") {
            // Check next
            if pos + 1 < parts.len() {
                return parts[pos + 1].to_string();
            }
            // Check prev
            if pos > 0 {
                let candidate = parts[pos - 1];
                if !candidate.contains('/') && !candidate.contains('\\') {
                    return candidate.to_string();
                }
            }
        }
        // Fallback
        truncate_with_ellipsis(scan_status, 21)
    } else {
        "-".to_string()
    }
}

pub fn format_duration_ms(ms: i64) -> String {
    if ms < 1000 {
        format!("{}ms", ms)
    } else if ms < 60_000 {
        format!("{}.{}s", ms / 1000, (ms % 1000) / 100)
    } else if ms < 3_600_000 {
        format!("{}m {}s", ms / 60_000, (ms % 60_000) / 1000)
    } else {
        format!("{}h {}m", ms / 3_600_000, (ms % 3_600_000) / 60_000)
    }
}
