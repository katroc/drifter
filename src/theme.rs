use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::BorderType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusKind {
    Success,
    Warning,
    Error,
    Info,
}

#[derive(Debug, Clone)]
pub struct Theme {
    pub name: &'static str,
    pub bg: Color,
    pub fg: Color,
    pub muted_fg: Color,
    pub panel_bg: Color,
    pub panel_border: Color,
    pub border_type: BorderType,
    pub accent: Color,
    pub accent_alt: Color,
    pub success: Color,
    pub warning: Color,
    pub error: Color,
    pub info: Color,
    pub selection_bg: Color,
    pub selection_fg: Color,
    pub progress_bg: Color,
    pub progress_fg: Color,
    pub table_header_fg: Color,
    pub table_header_bg: Color,
    pub table_row_alt_bg: Color,
    pub button_fg: Color,
    pub button_bg: Color,
    pub button_active_fg: Color,
    pub button_active_bg: Color,
    pub input_fg: Color,
    pub input_bg: Color,
    pub input_border: Color,
    pub dim: Color,
    pub highlight: Color,
}

#[derive(Clone, Copy)]
struct ThemePreset {
    name: &'static str,
    make: fn() -> Theme,
}

const THEME_PRESETS: &[ThemePreset] = &[
    ThemePreset {
        name: "Transparent",
        make: Theme::transparent,
    },
    ThemePreset {
        name: "Antigravity",
        make: Theme::antigravity,
    },
    ThemePreset {
        name: "Nord",
        make: Theme::nord,
    },
    ThemePreset {
        name: "Gruvbox Dark",
        make: Theme::gruvbox_dark,
    },
    ThemePreset {
        name: "Catppuccin Mocha",
        make: Theme::catppuccin_mocha,
    },
    ThemePreset {
        name: "Tokyo Night",
        make: Theme::tokyo_night,
    },
    ThemePreset {
        name: "Dracula",
        make: Theme::dracula,
    },
    ThemePreset {
        name: "Solarized Dark",
        make: Theme::solarized_dark,
    },
    ThemePreset {
        name: "Solarized Light",
        make: Theme::solarized_light,
    },
    ThemePreset {
        name: "Monochrome",
        make: Theme::monochrome,
    },
    ThemePreset {
        name: "High Contrast",
        make: Theme::high_contrast,
    },
    ThemePreset {
        name: "One Dark",
        make: Theme::one_dark,
    },
    ThemePreset {
        name: "One Light",
        make: Theme::one_light,
    },
    ThemePreset {
        name: "Ayu Dark",
        make: Theme::ayu_dark,
    },
    ThemePreset {
        name: "Everforest Dark",
        make: Theme::everforest_dark,
    },
    ThemePreset {
        name: "Rose Pine",
        make: Theme::rose_pine,
    },
    ThemePreset {
        name: "Rose Pine Dawn",
        make: Theme::rose_pine_dawn,
    },
    ThemePreset {
        name: "Night Owl",
        make: Theme::night_owl,
    },
];

const LEGACY_ALIASES: &[(&str, &str)] = &[
    ("drifter", "Nord"),
    ("nebula", "Tokyo Night"),
    ("emerald", "Gruvbox Dark"),
    ("obsidian", "Monochrome"),
    ("synthwave", "Dracula"),
    ("oceanic", "Tokyo Night"),
    ("crimson", "Dracula"),
    ("amber", "Gruvbox Dark"),
    ("peppermint", "Solarized Light"),
    ("royal", "Catppuccin Mocha"),
];

fn normalize_name(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

impl Theme {
    pub fn default_name() -> &'static str {
        "Antigravity"
    }

    pub fn default_theme() -> Self {
        Self::antigravity()
    }

    pub fn resolve_name(name: &str) -> Option<&'static str> {
        let needle = normalize_name(name);
        for preset in THEME_PRESETS {
            if normalize_name(preset.name) == needle {
                return Some(preset.name);
            }
        }
        for (legacy, target) in LEGACY_ALIASES {
            if *legacy == needle {
                return Some(*target);
            }
        }
        None
    }

    pub fn from_name(name: &str) -> Self {
        if let Some(resolved) = Self::resolve_name(name) {
            for preset in THEME_PRESETS {
                if preset.name == resolved {
                    return (preset.make)();
                }
            }
        }
        Self::default_theme()
    }

    pub fn list_names() -> Vec<&'static str> {
        THEME_PRESETS.iter().map(|preset| preset.name).collect()
    }

    pub fn base_style(&self) -> Style {
        Style::default().fg(self.fg).bg(self.bg)
    }

    pub fn panel_style(&self) -> Style {
        Style::default().fg(self.fg).bg(self.panel_bg)
    }

    pub fn border_style(&self) -> Style {
        Style::default().fg(self.panel_border)
    }

    pub fn border_active_style(&self) -> Style {
        Style::default().fg(self.accent).add_modifier(Modifier::BOLD)
    }

    pub fn text_style(&self) -> Style {
        Style::default().fg(self.fg)
    }

    pub fn muted_style(&self) -> Style {
        Style::default().fg(self.muted_fg)
    }

    pub fn dim_style(&self) -> Style {
        Style::default().fg(self.dim).add_modifier(Modifier::DIM)
    }

    pub fn highlight_style(&self) -> Style {
        Style::default().fg(self.highlight).add_modifier(Modifier::BOLD)
    }

    pub fn accent_style(&self) -> Style {
        Style::default().fg(self.accent)
    }

    pub fn accent_alt_style(&self) -> Style {
        Style::default().fg(self.accent_alt)
    }

    pub fn selection_style(&self) -> Style {
        Style::default()
            .fg(self.selection_fg)
            .bg(self.selection_bg)
            .add_modifier(Modifier::BOLD)
    }

    pub fn selection_soft_style(&self) -> Style {
        Style::default().fg(self.selection_fg).bg(self.selection_bg)
    }

    pub fn header_style(&self) -> Style {
        Style::default()
            .fg(self.table_header_fg)
            .bg(self.table_header_bg)
            .add_modifier(Modifier::BOLD)
    }

    pub fn row_style(&self, alt: bool) -> Style {
        Style::default()
            .fg(self.fg)
            .bg(if alt { self.table_row_alt_bg } else { self.panel_bg })
    }

    pub fn button_style(&self, active: bool) -> Style {
        if active {
            Style::default()
                .fg(self.button_active_fg)
                .bg(self.button_active_bg)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(self.button_fg).bg(self.button_bg)
        }
    }

    pub fn input_style(&self, active: bool) -> Style {
        let mut style = Style::default().fg(self.input_fg).bg(self.input_bg);
        if active {
            style = style.add_modifier(Modifier::BOLD);
        }
        style
    }

    pub fn input_border_style(&self, active: bool) -> Style {
        if active {
            Style::default()
                .fg(self.input_border)
                .add_modifier(Modifier::BOLD)
        } else {
            self.border_style()
        }
    }

    pub fn progress_style(&self) -> Style {
        Style::default().fg(self.progress_fg).bg(self.progress_bg)
    }

    pub fn status_style(&self, kind: StatusKind) -> Style {
        match kind {
            StatusKind::Success => Style::default().fg(self.success),
            StatusKind::Warning => Style::default()
                .fg(self.warning)
                .add_modifier(Modifier::BOLD),
            StatusKind::Error => Style::default()
                .fg(self.error)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
            StatusKind::Info => Style::default()
                .fg(self.info)
                .add_modifier(Modifier::ITALIC),
        }
    }

    pub fn status_badge_style(&self, kind: StatusKind) -> Style {
        let (bg, modifier) = match kind {
            StatusKind::Success => (self.success, Modifier::BOLD),
            StatusKind::Warning => (self.warning, Modifier::BOLD),
            StatusKind::Error => (self.error, Modifier::BOLD),
            StatusKind::Info => (self.info, Modifier::BOLD),
        };
        Style::default()
            .fg(self.button_active_fg)
            .bg(bg)
            .add_modifier(modifier)
    }

    #[allow(dead_code)]
    pub fn info_style(&self) -> Style {
        Style::default().fg(self.info)
    }

    pub fn transparent() -> Self {
        Self {
            name: "Transparent",
            bg: Color::Reset,
            fg: Color::Reset,
            muted_fg: Color::Reset,
            panel_bg: Color::Reset,
            panel_border: Color::Reset,
            border_type: BorderType::Rounded,
            accent: Color::Reset,
            accent_alt: Color::Reset,
            success: Color::Green,
            warning: Color::Yellow,
            error: Color::Red,
            info: Color::Blue,
            selection_bg: Color::DarkGray,
            selection_fg: Color::White,
            progress_bg: Color::DarkGray,
            progress_fg: Color::Green,
            table_header_fg: Color::Reset,
            table_header_bg: Color::Reset,
            table_row_alt_bg: Color::Reset,
            button_fg: Color::Reset,
            button_bg: Color::Reset,
            button_active_fg: Color::Black,
            button_active_bg: Color::White,
            input_fg: Color::Reset,
            input_bg: Color::Reset,
            input_border: Color::White,
            dim: Color::DarkGray,
            highlight: Color::Cyan,
        }
    }

    pub fn nord() -> Self {
        Self {
            name: "Nord",
            bg: Color::Rgb(46, 52, 64),
            fg: Color::Rgb(216, 222, 233),
            muted_fg: Color::Rgb(166, 179, 196),
            panel_bg: Color::Rgb(59, 66, 82),
            panel_border: Color::Rgb(76, 86, 106),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(136, 192, 208),
            accent_alt: Color::Rgb(180, 142, 173),
            success: Color::Rgb(163, 190, 140),
            warning: Color::Rgb(235, 203, 139),
            error: Color::Rgb(191, 97, 106),
            info: Color::Rgb(129, 161, 193),
            selection_bg: Color::Rgb(67, 76, 94),
            selection_fg: Color::Rgb(236, 239, 244),
            progress_bg: Color::Rgb(76, 86, 106),
            progress_fg: Color::Rgb(136, 192, 208),
            table_header_fg: Color::Rgb(236, 239, 244),
            table_header_bg: Color::Rgb(59, 66, 82),
            table_row_alt_bg: Color::Rgb(53, 59, 73),
            button_fg: Color::Rgb(236, 239, 244),
            button_bg: Color::Rgb(76, 86, 106),
            button_active_fg: Color::Rgb(46, 52, 64),
            button_active_bg: Color::Rgb(136, 192, 208),
            input_fg: Color::Rgb(236, 239, 244),
            input_bg: Color::Rgb(67, 76, 94),
            input_border: Color::Rgb(136, 192, 208),
            dim: Color::Rgb(118, 132, 150),
            highlight: Color::Rgb(143, 188, 187),
        }
    }

    pub fn gruvbox_dark() -> Self {
        Self {
            name: "Gruvbox Dark",
            bg: Color::Rgb(40, 40, 40),
            fg: Color::Rgb(235, 219, 178),
            muted_fg: Color::Rgb(168, 153, 132),
            panel_bg: Color::Rgb(50, 48, 47),
            panel_border: Color::Rgb(102, 92, 84),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(215, 153, 33),
            accent_alt: Color::Rgb(131, 165, 152),
            success: Color::Rgb(152, 151, 26),
            warning: Color::Rgb(250, 189, 47),
            error: Color::Rgb(251, 73, 52),
            info: Color::Rgb(69, 133, 136),
            selection_bg: Color::Rgb(80, 73, 69),
            selection_fg: Color::Rgb(251, 241, 199),
            progress_bg: Color::Rgb(60, 56, 54),
            progress_fg: Color::Rgb(184, 187, 38),
            table_header_fg: Color::Rgb(251, 241, 199),
            table_header_bg: Color::Rgb(60, 56, 54),
            table_row_alt_bg: Color::Rgb(46, 42, 39),
            button_fg: Color::Rgb(251, 241, 199),
            button_bg: Color::Rgb(102, 92, 84),
            button_active_fg: Color::Rgb(40, 40, 40),
            button_active_bg: Color::Rgb(215, 153, 33),
            input_fg: Color::Rgb(251, 241, 199),
            input_bg: Color::Rgb(60, 56, 54),
            input_border: Color::Rgb(215, 153, 33),
            dim: Color::Rgb(146, 131, 116),
            highlight: Color::Rgb(250, 189, 47),
        }
    }

    pub fn catppuccin_mocha() -> Self {
        Self {
            name: "Catppuccin Mocha",
            bg: Color::Rgb(30, 30, 46),
            fg: Color::Rgb(205, 214, 244),
            muted_fg: Color::Rgb(166, 173, 200),
            panel_bg: Color::Rgb(49, 50, 68),
            panel_border: Color::Rgb(69, 71, 90),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(137, 180, 250),
            accent_alt: Color::Rgb(245, 194, 231),
            success: Color::Rgb(166, 227, 161),
            warning: Color::Rgb(249, 226, 175),
            error: Color::Rgb(243, 139, 168),
            info: Color::Rgb(116, 199, 236),
            selection_bg: Color::Rgb(69, 71, 90),
            selection_fg: Color::Rgb(245, 224, 220),
            progress_bg: Color::Rgb(88, 91, 112),
            progress_fg: Color::Rgb(137, 180, 250),
            table_header_fg: Color::Rgb(245, 224, 220),
            table_header_bg: Color::Rgb(49, 50, 68),
            table_row_alt_bg: Color::Rgb(36, 39, 58),
            button_fg: Color::Rgb(245, 224, 220),
            button_bg: Color::Rgb(69, 71, 90),
            button_active_fg: Color::Rgb(30, 30, 46),
            button_active_bg: Color::Rgb(137, 180, 250),
            input_fg: Color::Rgb(205, 214, 244),
            input_bg: Color::Rgb(69, 71, 90),
            input_border: Color::Rgb(137, 180, 250),
            dim: Color::Rgb(108, 112, 134),
            highlight: Color::Rgb(245, 194, 231),
        }
    }

    pub fn tokyo_night() -> Self {
        Self {
            name: "Tokyo Night",
            bg: Color::Rgb(26, 27, 38),
            fg: Color::Rgb(192, 202, 245),
            muted_fg: Color::Rgb(154, 165, 206),
            panel_bg: Color::Rgb(31, 35, 53),
            panel_border: Color::Rgb(59, 66, 97),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(122, 162, 247),
            accent_alt: Color::Rgb(187, 154, 247),
            success: Color::Rgb(158, 206, 106),
            warning: Color::Rgb(224, 175, 104),
            error: Color::Rgb(247, 118, 142),
            info: Color::Rgb(125, 207, 255),
            selection_bg: Color::Rgb(54, 74, 130),
            selection_fg: Color::Rgb(224, 224, 224),
            progress_bg: Color::Rgb(42, 46, 68),
            progress_fg: Color::Rgb(122, 162, 247),
            table_header_fg: Color::Rgb(192, 202, 245),
            table_header_bg: Color::Rgb(31, 35, 53),
            table_row_alt_bg: Color::Rgb(36, 40, 59),
            button_fg: Color::Rgb(192, 202, 245),
            button_bg: Color::Rgb(59, 66, 97),
            button_active_fg: Color::Rgb(26, 27, 38),
            button_active_bg: Color::Rgb(122, 162, 247),
            input_fg: Color::Rgb(192, 202, 245),
            input_bg: Color::Rgb(42, 46, 68),
            input_border: Color::Rgb(122, 162, 247),
            dim: Color::Rgb(86, 95, 137),
            highlight: Color::Rgb(187, 154, 247),
        }
    }

    pub fn dracula() -> Self {
        Self {
            name: "Dracula",
            bg: Color::Rgb(40, 42, 54),
            fg: Color::Rgb(248, 248, 242),
            muted_fg: Color::Rgb(98, 114, 164),
            panel_bg: Color::Rgb(52, 55, 70),
            panel_border: Color::Rgb(68, 71, 90),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(189, 147, 249),
            accent_alt: Color::Rgb(255, 121, 198),
            success: Color::Rgb(80, 250, 123),
            warning: Color::Rgb(241, 250, 140),
            error: Color::Rgb(255, 85, 85),
            info: Color::Rgb(139, 233, 253),
            selection_bg: Color::Rgb(68, 71, 90),
            selection_fg: Color::Rgb(248, 248, 242),
            progress_bg: Color::Rgb(62, 64, 83),
            progress_fg: Color::Rgb(80, 250, 123),
            table_header_fg: Color::Rgb(248, 248, 242),
            table_header_bg: Color::Rgb(52, 55, 70),
            table_row_alt_bg: Color::Rgb(47, 49, 66),
            button_fg: Color::Rgb(248, 248, 242),
            button_bg: Color::Rgb(68, 71, 90),
            button_active_fg: Color::Rgb(40, 42, 54),
            button_active_bg: Color::Rgb(189, 147, 249),
            input_fg: Color::Rgb(248, 248, 242),
            input_bg: Color::Rgb(62, 64, 83),
            input_border: Color::Rgb(189, 147, 249),
            dim: Color::Rgb(98, 114, 164),
            highlight: Color::Rgb(255, 121, 198),
        }
    }

    pub fn solarized_dark() -> Self {
        Self {
            name: "Solarized Dark",
            bg: Color::Rgb(0, 43, 54),
            fg: Color::Rgb(131, 148, 150),
            muted_fg: Color::Rgb(88, 110, 117),
            panel_bg: Color::Rgb(7, 54, 66),
            panel_border: Color::Rgb(88, 110, 117),
            border_type: BorderType::Thick,
            accent: Color::Rgb(38, 139, 210),
            accent_alt: Color::Rgb(42, 161, 152),
            success: Color::Rgb(133, 153, 0),
            warning: Color::Rgb(181, 137, 0),
            error: Color::Rgb(220, 50, 47),
            info: Color::Rgb(42, 161, 152),
            selection_bg: Color::Rgb(11, 58, 74),
            selection_fg: Color::Rgb(238, 232, 213),
            progress_bg: Color::Rgb(0, 53, 65),
            progress_fg: Color::Rgb(38, 139, 210),
            table_header_fg: Color::Rgb(238, 232, 213),
            table_header_bg: Color::Rgb(7, 54, 66),
            table_row_alt_bg: Color::Rgb(0, 55, 68),
            button_fg: Color::Rgb(238, 232, 213),
            button_bg: Color::Rgb(88, 110, 117),
            button_active_fg: Color::Rgb(0, 43, 54),
            button_active_bg: Color::Rgb(38, 139, 210),
            input_fg: Color::Rgb(238, 232, 213),
            input_bg: Color::Rgb(11, 58, 74),
            input_border: Color::Rgb(38, 139, 210),
            dim: Color::Rgb(101, 123, 131),
            highlight: Color::Rgb(42, 161, 152),
        }
    }

    pub fn solarized_light() -> Self {
        Self {
            name: "Solarized Light",
            bg: Color::Rgb(253, 246, 227),
            fg: Color::Rgb(88, 110, 117),
            muted_fg: Color::Rgb(147, 161, 161),
            panel_bg: Color::Rgb(238, 232, 213),
            panel_border: Color::Rgb(147, 161, 161),
            border_type: BorderType::Thick,
            accent: Color::Rgb(38, 139, 210),
            accent_alt: Color::Rgb(42, 161, 152),
            success: Color::Rgb(133, 153, 0),
            warning: Color::Rgb(181, 137, 0),
            error: Color::Rgb(220, 50, 47),
            info: Color::Rgb(42, 161, 152),
            selection_bg: Color::Rgb(224, 215, 194),
            selection_fg: Color::Rgb(7, 54, 66),
            progress_bg: Color::Rgb(224, 215, 194),
            progress_fg: Color::Rgb(38, 139, 210),
            table_header_fg: Color::Rgb(7, 54, 66),
            table_header_bg: Color::Rgb(238, 232, 213),
            table_row_alt_bg: Color::Rgb(245, 239, 218),
            button_fg: Color::Rgb(7, 54, 66),
            button_bg: Color::Rgb(147, 161, 161),
            button_active_fg: Color::Rgb(253, 246, 227),
            button_active_bg: Color::Rgb(38, 139, 210),
            input_fg: Color::Rgb(7, 54, 66),
            input_bg: Color::Rgb(224, 215, 194),
            input_border: Color::Rgb(38, 139, 210),
            dim: Color::Rgb(167, 181, 181),
            highlight: Color::Rgb(42, 161, 152),
        }
    }

    pub fn monochrome() -> Self {
        Self {
            name: "Monochrome",
            bg: Color::Rgb(17, 17, 17),
            fg: Color::Rgb(229, 229, 229),
            muted_fg: Color::Rgb(176, 176, 176),
            panel_bg: Color::Rgb(27, 27, 27),
            panel_border: Color::Rgb(58, 58, 58),
            border_type: BorderType::Plain,
            accent: Color::Rgb(255, 255, 255),
            accent_alt: Color::Rgb(204, 204, 204),
            success: Color::Rgb(229, 229, 229),
            warning: Color::Rgb(192, 192, 192),
            error: Color::Rgb(255, 255, 255),
            info: Color::Rgb(208, 208, 208),
            selection_bg: Color::Rgb(51, 51, 51),
            selection_fg: Color::Rgb(255, 255, 255),
            progress_bg: Color::Rgb(42, 42, 42),
            progress_fg: Color::Rgb(255, 255, 255),
            table_header_fg: Color::Rgb(255, 255, 255),
            table_header_bg: Color::Rgb(31, 31, 31),
            table_row_alt_bg: Color::Rgb(22, 22, 22),
            button_fg: Color::Rgb(255, 255, 255),
            button_bg: Color::Rgb(58, 58, 58),
            button_active_fg: Color::Rgb(17, 17, 17),
            button_active_bg: Color::Rgb(255, 255, 255),
            input_fg: Color::Rgb(255, 255, 255),
            input_bg: Color::Rgb(38, 38, 38),
            input_border: Color::Rgb(255, 255, 255),
            dim: Color::Rgb(138, 138, 138),
            highlight: Color::Rgb(255, 255, 255),
        }
    }

    pub fn high_contrast() -> Self {
        Self {
            name: "High Contrast",
            bg: Color::Rgb(0, 0, 0),
            fg: Color::Rgb(255, 255, 255),
            muted_fg: Color::Rgb(191, 191, 191),
            panel_bg: Color::Rgb(10, 10, 10),
            panel_border: Color::Rgb(255, 255, 255),
            border_type: BorderType::Double,
            accent: Color::Rgb(0, 255, 255),
            accent_alt: Color::Rgb(255, 0, 255),
            success: Color::Rgb(0, 255, 0),
            warning: Color::Rgb(255, 255, 0),
            error: Color::Rgb(255, 51, 51),
            info: Color::Rgb(0, 191, 255),
            selection_bg: Color::Rgb(255, 255, 255),
            selection_fg: Color::Rgb(0, 0, 0),
            progress_bg: Color::Rgb(43, 43, 43),
            progress_fg: Color::Rgb(0, 255, 255),
            table_header_fg: Color::Rgb(0, 0, 0),
            table_header_bg: Color::Rgb(255, 255, 255),
            table_row_alt_bg: Color::Rgb(17, 17, 17),
            button_fg: Color::Rgb(0, 0, 0),
            button_bg: Color::Rgb(255, 255, 255),
            button_active_fg: Color::Rgb(0, 0, 0),
            button_active_bg: Color::Rgb(0, 255, 255),
            input_fg: Color::Rgb(255, 255, 255),
            input_bg: Color::Rgb(0, 0, 0),
            input_border: Color::Rgb(0, 255, 255),
            dim: Color::Rgb(128, 128, 128),
            highlight: Color::Rgb(255, 0, 255),
        }
    }

    pub fn one_dark() -> Self {
        Self {
            name: "One Dark",
            bg: Color::Rgb(40, 44, 52),
            fg: Color::Rgb(171, 178, 191),
            muted_fg: Color::Rgb(89, 98, 111),
            panel_bg: Color::Rgb(47, 51, 61),
            panel_border: Color::Rgb(58, 63, 75),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(41, 121, 255),
            accent_alt: Color::Rgb(198, 121, 221),
            success: Color::Rgb(152, 195, 121),
            warning: Color::Rgb(229, 193, 124),
            error: Color::Rgb(224, 108, 117),
            info: Color::Rgb(87, 182, 194),
            selection_bg: Color::Rgb(77, 81, 93),
            selection_fg: Color::Rgb(255, 255, 255),
            progress_bg: Color::Rgb(56, 61, 72),
            progress_fg: Color::Rgb(152, 195, 121),
            table_header_fg: Color::Rgb(171, 178, 191),
            table_header_bg: Color::Rgb(47, 51, 61),
            table_row_alt_bg: Color::Rgb(56, 62, 73),
            button_fg: Color::Rgb(171, 178, 191),
            button_bg: Color::Rgb(58, 63, 75),
            button_active_fg: Color::Rgb(40, 44, 52),
            button_active_bg: Color::Rgb(41, 121, 255),
            input_fg: Color::Rgb(171, 178, 191),
            input_bg: Color::Rgb(56, 61, 72),
            input_border: Color::Rgb(41, 121, 255),
            dim: Color::Rgb(107, 114, 125),
            highlight: Color::Rgb(97, 174, 239),
        }
    }

    pub fn one_light() -> Self {
        Self {
            name: "One Light",
            bg: Color::Rgb(244, 244, 244),
            fg: Color::Rgb(35, 35, 36),
            muted_fg: Color::Rgb(160, 161, 167),
            panel_bg: Color::Rgb(234, 234, 235),
            panel_border: Color::Rgb(219, 219, 220),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(41, 121, 255),
            accent_alt: Color::Rgb(166, 38, 164),
            success: Color::Rgb(80, 161, 78),
            warning: Color::Rgb(193, 132, 1),
            error: Color::Rgb(228, 86, 74),
            info: Color::Rgb(1, 132, 188),
            selection_bg: Color::Rgb(255, 255, 255),
            selection_fg: Color::Rgb(35, 35, 36),
            progress_bg: Color::Rgb(219, 219, 220),
            progress_fg: Color::Rgb(80, 161, 78),
            table_header_fg: Color::Rgb(35, 35, 36),
            table_header_bg: Color::Rgb(234, 234, 235),
            table_row_alt_bg: Color::Rgb(255, 255, 255),
            button_fg: Color::Rgb(35, 35, 36),
            button_bg: Color::Rgb(219, 219, 220),
            button_active_fg: Color::Rgb(255, 255, 255),
            button_active_bg: Color::Rgb(41, 121, 255),
            input_fg: Color::Rgb(35, 35, 36),
            input_bg: Color::Rgb(255, 255, 255),
            input_border: Color::Rgb(41, 121, 255),
            dim: Color::Rgb(184, 184, 185),
            highlight: Color::Rgb(64, 120, 242),
        }
    }

    pub fn ayu_dark() -> Self {
        Self {
            name: "Ayu Dark",
            bg: Color::Rgb(16, 20, 28),
            fg: Color::Rgb(191, 189, 182),
            muted_fg: Color::Rgb(98, 106, 115),
            panel_bg: Color::Rgb(19, 23, 33),
            panel_border: Color::Rgb(98, 106, 115),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(57, 186, 230),
            accent_alt: Color::Rgb(210, 166, 255),
            success: Color::Rgb(170, 217, 76),
            warning: Color::Rgb(255, 180, 84),
            error: Color::Rgb(240, 113, 120),
            info: Color::Rgb(149, 230, 203),
            selection_bg: Color::Rgb(39, 55, 71),
            selection_fg: Color::Rgb(191, 189, 182),
            progress_bg: Color::Rgb(39, 55, 71),
            progress_fg: Color::Rgb(170, 217, 76),
            table_header_fg: Color::Rgb(191, 189, 182),
            table_header_bg: Color::Rgb(19, 23, 33),
            table_row_alt_bg: Color::Rgb(39, 55, 71),
            button_fg: Color::Rgb(191, 189, 182),
            button_bg: Color::Rgb(39, 55, 71),
            button_active_fg: Color::Rgb(16, 20, 28),
            button_active_bg: Color::Rgb(57, 186, 230),
            input_fg: Color::Rgb(191, 189, 182),
            input_bg: Color::Rgb(19, 23, 33),
            input_border: Color::Rgb(57, 186, 230),
            dim: Color::Rgb(98, 106, 115),
            highlight: Color::Rgb(255, 143, 64),
        }
    }

    pub fn everforest_dark() -> Self {
        Self {
            name: "Everforest Dark",
            bg: Color::Rgb(45, 53, 59),
            fg: Color::Rgb(211, 198, 170),
            muted_fg: Color::Rgb(157, 169, 160),
            panel_bg: Color::Rgb(52, 63, 68),
            panel_border: Color::Rgb(79, 88, 94),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(127, 187, 179),
            accent_alt: Color::Rgb(214, 153, 182),
            success: Color::Rgb(167, 192, 128),
            warning: Color::Rgb(219, 188, 127),
            error: Color::Rgb(230, 126, 128),
            info: Color::Rgb(131, 192, 146),
            selection_bg: Color::Rgb(71, 82, 88),
            selection_fg: Color::Rgb(211, 198, 170),
            progress_bg: Color::Rgb(61, 72, 77),
            progress_fg: Color::Rgb(167, 192, 128),
            table_header_fg: Color::Rgb(211, 198, 170),
            table_header_bg: Color::Rgb(52, 63, 68),
            table_row_alt_bg: Color::Rgb(61, 72, 77),
            button_fg: Color::Rgb(211, 198, 170),
            button_bg: Color::Rgb(79, 88, 94),
            button_active_fg: Color::Rgb(45, 53, 59),
            button_active_bg: Color::Rgb(127, 187, 179),
            input_fg: Color::Rgb(211, 198, 170),
            input_bg: Color::Rgb(52, 63, 68),
            input_border: Color::Rgb(127, 187, 179),
            dim: Color::Rgb(122, 132, 120),
            highlight: Color::Rgb(230, 152, 117),
        }
    }

    pub fn rose_pine() -> Self {
        Self {
            name: "Rose Pine",
            bg: Color::Rgb(25, 23, 36),
            fg: Color::Rgb(224, 222, 244),
            muted_fg: Color::Rgb(110, 106, 134),
            panel_bg: Color::Rgb(31, 29, 46),
            panel_border: Color::Rgb(82, 79, 103),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(196, 167, 231),
            accent_alt: Color::Rgb(235, 188, 186),
            success: Color::Rgb(156, 207, 216),
            warning: Color::Rgb(246, 193, 119),
            error: Color::Rgb(235, 111, 146),
            info: Color::Rgb(49, 116, 143),
            selection_bg: Color::Rgb(64, 61, 82),
            selection_fg: Color::Rgb(224, 222, 244),
            progress_bg: Color::Rgb(38, 35, 58),
            progress_fg: Color::Rgb(156, 207, 216),
            table_header_fg: Color::Rgb(224, 222, 244),
            table_header_bg: Color::Rgb(31, 29, 46),
            table_row_alt_bg: Color::Rgb(38, 35, 58),
            button_fg: Color::Rgb(224, 222, 244),
            button_bg: Color::Rgb(64, 61, 82),
            button_active_fg: Color::Rgb(25, 23, 36),
            button_active_bg: Color::Rgb(196, 167, 231),
            input_fg: Color::Rgb(224, 222, 244),
            input_bg: Color::Rgb(38, 35, 58),
            input_border: Color::Rgb(196, 167, 231),
            dim: Color::Rgb(144, 140, 170),
            highlight: Color::Rgb(246, 193, 119),
        }
    }

    pub fn rose_pine_dawn() -> Self {
        Self {
            name: "Rose Pine Dawn",
            bg: Color::Rgb(250, 244, 237),
            fg: Color::Rgb(87, 82, 121),
            muted_fg: Color::Rgb(152, 147, 165),
            panel_bg: Color::Rgb(255, 250, 243),
            panel_border: Color::Rgb(206, 202, 205),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(144, 122, 169),
            accent_alt: Color::Rgb(215, 130, 126),
            success: Color::Rgb(86, 148, 159),
            warning: Color::Rgb(234, 157, 52),
            error: Color::Rgb(180, 99, 122),
            info: Color::Rgb(40, 105, 131),
            selection_bg: Color::Rgb(223, 218, 217),
            selection_fg: Color::Rgb(87, 82, 121),
            progress_bg: Color::Rgb(244, 237, 232),
            progress_fg: Color::Rgb(86, 148, 159),
            table_header_fg: Color::Rgb(87, 82, 121),
            table_header_bg: Color::Rgb(255, 250, 243),
            table_row_alt_bg: Color::Rgb(242, 233, 225),
            button_fg: Color::Rgb(87, 82, 121),
            button_bg: Color::Rgb(223, 218, 217),
            button_active_fg: Color::Rgb(250, 244, 237),
            button_active_bg: Color::Rgb(144, 122, 169),
            input_fg: Color::Rgb(87, 82, 121),
            input_bg: Color::Rgb(255, 250, 243),
            input_border: Color::Rgb(144, 122, 169),
            dim: Color::Rgb(121, 117, 147),
            highlight: Color::Rgb(234, 157, 52),
        }
    }

    pub fn night_owl() -> Self {
        Self {
            name: "Night Owl",
            bg: Color::Rgb(1, 22, 39),
            fg: Color::Rgb(190, 197, 212),
            muted_fg: Color::Rgb(68, 89, 107),
            panel_bg: Color::Rgb(0, 17, 34),
            panel_border: Color::Rgb(16, 42, 68),
            border_type: BorderType::Plain,
            accent: Color::Rgb(130, 170, 255),
            accent_alt: Color::Rgb(199, 146, 234),
            success: Color::Rgb(173, 219, 103),
            warning: Color::Rgb(255, 203, 139),
            error: Color::Rgb(239, 83, 80),
            info: Color::Rgb(127, 219, 202),
            selection_bg: Color::Rgb(68, 89, 107),
            selection_fg: Color::Rgb(255, 255, 255),
            progress_bg: Color::Rgb(68, 89, 107),
            progress_fg: Color::Rgb(34, 218, 110),
            table_header_fg: Color::Rgb(255, 255, 255),
            table_header_bg: Color::Rgb(0, 17, 34),
            table_row_alt_bg: Color::Rgb(1, 22, 39),
            button_fg: Color::Rgb(190, 197, 212),
            button_bg: Color::Rgb(16, 42, 68),
            button_active_fg: Color::Rgb(1, 22, 39),
            button_active_bg: Color::Rgb(130, 170, 255),
            input_fg: Color::Rgb(190, 197, 212),
            input_bg: Color::Rgb(0, 17, 34),
            input_border: Color::Rgb(130, 170, 255),
            dim: Color::Rgb(68, 89, 107),
            highlight: Color::Rgb(255, 203, 139),
        }
    }

    pub fn antigravity() -> Self {
        Self {
            name: "Antigravity",
            bg: Color::Rgb(26, 27, 38),
            fg: Color::Rgb(192, 202, 245),
            muted_fg: Color::Rgb(154, 165, 206),
            panel_bg: Color::Rgb(31, 35, 53),
            panel_border: Color::Rgb(59, 66, 97),
            border_type: BorderType::Rounded,
            accent: Color::Rgb(255, 158, 100),
            accent_alt: Color::Rgb(187, 154, 247),
            success: Color::Rgb(158, 206, 106),
            warning: Color::Rgb(224, 175, 104),
            error: Color::Rgb(247, 118, 142),
            info: Color::Rgb(122, 162, 247),
            selection_bg: Color::Rgb(47, 53, 85),
            selection_fg: Color::Rgb(192, 202, 245),
            progress_bg: Color::Rgb(31, 35, 53),
            progress_fg: Color::Rgb(255, 158, 100),
            table_header_fg: Color::Rgb(187, 154, 247),
            table_header_bg: Color::Rgb(26, 27, 38),
            table_row_alt_bg: Color::Rgb(31, 35, 53),
            button_fg: Color::Rgb(192, 202, 245),
            button_bg: Color::Rgb(59, 66, 97),
            button_active_fg: Color::Rgb(26, 27, 38),
            button_active_bg: Color::Rgb(255, 158, 100),
            input_fg: Color::Rgb(192, 202, 245),
            input_bg: Color::Rgb(31, 35, 53),
            input_border: Color::Rgb(255, 158, 100),
            dim: Color::Rgb(86, 95, 137),
            highlight: Color::Rgb(122, 162, 247),
        }
    }
}
