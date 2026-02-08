use crate::app::settings::SettingsCategory;
use crate::app::state::{App, AppFocus, InputMode};

#[derive(Clone, Copy)]
pub struct KeyHint {
    pub key: Option<&'static str>,
    pub action: &'static str,
}

fn key_action(key: &'static str, action: &'static str) -> KeyHint {
    KeyHint {
        key: Some(key),
        action,
    }
}

fn text(action: &'static str) -> KeyHint {
    KeyHint { key: None, action }
}

fn settings_category_hints(category: SettingsCategory) -> Vec<KeyHint> {
    match category {
        SettingsCategory::S3 => vec![
            key_action("Tab/→", "Fields"),
            key_action("←", "Rail"),
            key_action("↑/↓", "Category"),
            key_action("[]", "Cycle S3"),
            key_action("n/x", "New/Del S3"),
            key_action("s", "Save"),
            key_action("t", "Test"),
        ],
        SettingsCategory::Scanner => vec![
            key_action("Tab/→", "Fields"),
            key_action("←", "Rail"),
            key_action("↑/↓", "Category"),
            key_action("s", "Save"),
            key_action("t", "Test"),
        ],
        SettingsCategory::Performance | SettingsCategory::General => vec![
            key_action("Tab/→", "Fields"),
            key_action("←", "Rail"),
            key_action("↑/↓", "Category"),
            key_action("s", "Save"),
        ],
    }
}

fn settings_field_hints(category: SettingsCategory) -> Vec<KeyHint> {
    match category {
        SettingsCategory::S3 => vec![
            key_action("←", "Category"),
            key_action("Enter", "Edit"),
            key_action("[]", "Cycle S3"),
            key_action("n/x", "New/Del S3"),
            key_action("s", "Save"),
            key_action("t", "Test"),
        ],
        SettingsCategory::Scanner => vec![
            key_action("←", "Category"),
            key_action("Enter", "Edit"),
            key_action("s", "Save"),
            key_action("t", "Test"),
        ],
        SettingsCategory::Performance => vec![
            key_action("←", "Category"),
            key_action("Enter", "Edit"),
            key_action("s", "Save"),
        ],
        SettingsCategory::General => vec![
            key_action("←", "Category"),
            key_action("Enter", "Edit/Open"),
            key_action("s", "Save"),
        ],
    }
}

pub fn footer_hints(app: &App) -> Vec<KeyHint> {
    match app.input_mode {
        InputMode::LogSearch => vec![key_action("Enter", "Search"), key_action("Esc", "Cancel")],
        InputMode::QueueSearch | InputMode::HistorySearch => {
            vec![text("Type to filter"), key_action("Enter/Esc", "Done")]
        }
        InputMode::Filter => vec![
            text("Type to filter"),
            key_action("↑/↓", "Select"),
            key_action("Enter", "Open"),
            key_action("Esc", "Back"),
        ],
        InputMode::Browsing => {
            let mut hints = vec![
                key_action("←/→", "Navigate"),
                key_action("↑/↓", "Select"),
                key_action("/", "Filter"),
                key_action("t", "Tree"),
                key_action("Space", "Select"),
                key_action("Esc/q", "Exit"),
            ];
            if matches!(
                app.transfer_direction,
                crate::core::transfer::TransferDirection::LocalToS3
            ) {
                hints.push(key_action("s", "Stage"));
            } else {
                hints.push(key_action("s", "Upload Off"));
            }
            hints.push(key_action("v", "Mode"));
            hints
        }
        InputMode::RemoteBrowsing => {
            let mut hints = vec![
                key_action("←/→", "Navigate"),
                key_action("↑/↓", "Select"),
                key_action("e", "Select Ep"),
                key_action("r", "Refresh"),
                key_action("d", "Download"),
                key_action("x", "Delete"),
                key_action("n", "New Folder"),
                key_action("Esc/q", "Exit"),
            ];
            if matches!(
                app.transfer_direction,
                crate::core::transfer::TransferDirection::S3ToLocal
                    | crate::core::transfer::TransferDirection::S3ToS3
            ) {
                hints.push(key_action("s", "Queue"));
            }
            if app.focus == AppFocus::Remote
                || (app.focus == AppFocus::Browser
                    && matches!(
                        app.transfer_direction,
                        crate::core::transfer::TransferDirection::S3ToS3
                    ))
            {
                hints.push(key_action("Space", "Select"));
                hints.push(key_action("c", "Clear Sel"));
            }
            hints.push(key_action("v", "Mode"));
            hints
        }
        InputMode::RemoteFolderCreate => {
            vec![key_action("Enter", "Create"), key_action("Esc", "Cancel")]
        }
        InputMode::EndpointSelect => vec![
            key_action("↑/↓", "Select"),
            key_action("Enter", "Apply"),
            key_action("Esc", "Cancel"),
        ],
        InputMode::Confirmation => vec![
            key_action("Enter/y", "Confirm"),
            key_action("Esc/n", "Cancel"),
        ],
        InputMode::LayoutAdjust => vec![
            key_action("1-3", "Select Panel"),
            key_action("+/-", "Adjust"),
            key_action("r/R", "Reset"),
            key_action("s", "Save"),
            key_action("Esc/q", "Cancel"),
        ],
        InputMode::Normal => {
            let mut hints = match app.focus {
                AppFocus::Logs => vec![key_action("q", "Back")],
                AppFocus::Rail => vec![
                    key_action("Tab/→", "Content"),
                    key_action("↑/↓", "Switch Tab"),
                ],
                AppFocus::Browser => {
                    let mut hints = if matches!(
                        app.transfer_direction,
                        crate::core::transfer::TransferDirection::S3ToS3
                    ) {
                        vec![
                            key_action("e", "Select Ep"),
                            key_action("a", "Browse"),
                            key_action("r", "Refresh"),
                            key_action("x", "Delete"),
                            key_action("Space", "Select"),
                            key_action("c", "Clear Sel"),
                        ]
                    } else {
                        vec![key_action("↑/↓", "Select"), key_action("a", "Browse")]
                    };
                    hints.push(key_action("v", "Mode"));
                    hints
                }
                AppFocus::Queue => vec![
                    key_action("↑/↓", "Select"),
                    key_action("p", "Pause/Resume"),
                    key_action("+/-", "Priority"),
                    key_action("Enter", "Details"),
                    key_action("c", "Clear Done"),
                    key_action("r", "Retry"),
                    key_action("←/→", "Navigate"),
                    key_action("d", "Cancel"),
                ],
                AppFocus::History => vec![key_action("←", "Queue")],
                AppFocus::Remote => {
                    let mut hints = vec![
                        key_action("e", "Select Ep"),
                        key_action("a", "Browse"),
                        key_action("r", "Refresh"),
                        key_action("d", "Download"),
                        key_action("x", "Delete"),
                    ];
                    if matches!(
                        app.transfer_direction,
                        crate::core::transfer::TransferDirection::S3ToLocal
                            | crate::core::transfer::TransferDirection::S3ToS3
                    ) {
                        hints.push(key_action("s", "Queue"));
                    }
                    if matches!(
                        app.transfer_direction,
                        crate::core::transfer::TransferDirection::LocalToS3
                            | crate::core::transfer::TransferDirection::S3ToLocal
                            | crate::core::transfer::TransferDirection::S3ToS3
                    ) {
                        hints.push(key_action("Space", "Select"));
                        hints.push(key_action("c", "Clear Sel"));
                    }
                    hints.push(key_action("v", "Mode"));
                    hints
                }
                AppFocus::Quarantine => vec![
                    key_action("←", "Rail"),
                    key_action("d", "Clear"),
                    key_action("R", "Refresh"),
                ],
                AppFocus::SettingsCategory => {
                    let category = app.settings.active_category;
                    settings_category_hints(category)
                }
                AppFocus::SettingsFields => {
                    let category = app.settings.active_category;
                    settings_field_hints(category)
                }
            };

            hints.push(key_action("Tab", "Navigate"));
            if app.focus != AppFocus::Logs {
                hints.push(key_action("q", "Quit"));
            }
            hints
        }
    }
}
