# Drifter

Terminal UI for staging, scanning, and transferring data with live progress and quarantine handling.

## Themes

Built-in themes (case-insensitive):

- Nord
- Gruvbox Dark
- Catppuccin Mocha
- Tokyo Night
- Dracula
- Solarized Dark
- Solarized Light
- Monochrome
- High Contrast
- One Dark
- One Light
- Ayu Dark
- Everforest Dark
- Rose Pine
- Rose Pine Dawn
- Night Owl

### Select a theme

- In-app: press `t` to open the theme picker, or go to Settings → Theme and press Enter.

Theme choice is stored in the DB-backed config (`theme` setting) when you save settings.

### Screenshot tips (optional)

- Use a truecolor-capable terminal.
- Use `t` to preview themes before saving.

## Developer notes

### Add a new theme

1. Add a new preset in `src/theme.rs` by defining a `Theme::your_theme_name()` constructor.
2. Register it in `THEME_PRESETS` with a user-facing name.
3. (Optional) Add a legacy alias in `LEGACY_ALIASES` if you are replacing an old theme name.

The theme tokens are semantic (bg/fg, selection, status, table, input, button, etc.), so components automatically pick up new colors.
