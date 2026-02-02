use anyhow::Result;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickerView {
    List,
    Tree,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub path: PathBuf,
    pub is_dir: bool,
    pub is_parent: bool,
    pub depth: usize,
    pub size: u64,
    pub modified: Option<std::time::SystemTime>,
}

#[derive(Clone)]
pub struct FilePicker {
    pub cwd: PathBuf,
    pub entries: Vec<FileEntry>,
    pub selected: usize,
    pub view: PickerView,
    pub expanded: HashSet<PathBuf>,
    pub selected_paths: HashSet<PathBuf>,
    pub last_error: Option<String>,
    pub search_recursive: bool,
    pub is_searching: bool,
}

impl Default for FilePicker {
    fn default() -> Self {
        Self::new()
    }
}

impl FilePicker {
    pub fn new() -> Self {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let mut picker = Self {
            cwd,
            entries: Vec::new(),
            selected: 0,
            view: PickerView::List,
            expanded: HashSet::new(),
            selected_paths: HashSet::new(),
            last_error: None,
            search_recursive: false,
            is_searching: false,
        };
        picker.refresh();
        picker
    }

    pub fn refresh(&mut self) {
        if self.is_searching && self.search_recursive {
            match self.load_dir_recursive(&self.cwd) {
                Ok(entries) => {
                    self.entries = entries;
                    self.last_error = None;
                }
                Err(err) => {
                    self.entries.clear();
                    self.last_error = Some(err.to_string());
                }
            }
        } else {
            let res = match self.view {
                PickerView::List => self.load_dir_list(&self.cwd),
                PickerView::Tree => self.load_dir_tree(&self.cwd),
            };

            match res {
                Ok(entries) => {
                    self.entries = entries;
                    self.last_error = None;
                }
                Err(err) => {
                    self.last_error = Some(err.to_string());
                    self.entries.clear();
                    // Add ".." entry so user isn't stuck
                    if let Some(parent) = self.cwd.parent() {
                        self.entries.push(FileEntry {
                            name: "..".to_string(),
                            path: parent.to_path_buf(),
                            is_dir: true,
                            is_parent: true,
                            depth: 0,
                            size: 0,
                            modified: None,
                        });
                    }
                }
            }
        }

        if self.selected >= self.entries.len() {
            self.selected = self.entries.len().saturating_sub(1);
        }

        // Auto-select first item if we just loaded and have no selection (or if 0 is "..")
        if self.selected == 0 && self.entries.len() > 1 && self.entries[0].name == ".." {
            // Skip '..' parent on initial load
            self.selected = 1;
        }
    }

    pub fn try_set_cwd(&mut self, path: PathBuf) {
        match self.load_dir_list(&path) {
            Ok(entries) => {
                self.cwd = path;
                self.entries = entries;
                // Start on first real item (skip '..') if possible
                self.selected = if self.entries.len() > 1 { 1 } else { 0 };
                self.last_error = None;
                self.expanded.insert(self.cwd.clone());
                if self.view == PickerView::Tree {
                    self.entries = match self.load_dir_tree(&self.cwd) {
                        Ok(tree) => tree,
                        Err(err) => {
                            self.last_error = Some(err.to_string());
                            Vec::new()
                        }
                    };
                }
            }
            Err(err) => {
                self.last_error = Some(err.to_string());
            }
        }
    }

    fn load_dir_list(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let mut entries = Vec::new();
        if let Some(parent) = path.parent() {
            entries.push(FileEntry {
                name: "..".to_string(),
                path: parent.to_path_buf(),
                is_dir: true,
                is_parent: true,
                depth: 0,
                size: 0,
                modified: None,
            });
        }
        for entry in fs::read_dir(path)? {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let metadata = fs::metadata(&path).ok();
            let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
            let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            let modified = metadata.and_then(|m| m.modified().ok());

            entries.push(FileEntry {
                name,
                path,
                is_dir,
                is_parent: false,
                depth: 0,
                size,
                modified,
            });
        }
        entries.sort_by(
            |a, b| match (a.is_parent, b.is_parent, a.is_dir, b.is_dir) {
                (true, false, _, _) => std::cmp::Ordering::Less,
                (false, true, _, _) => std::cmp::Ordering::Greater,
                (_, _, true, false) => std::cmp::Ordering::Less,
                (_, _, false, true) => std::cmp::Ordering::Greater,
                _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
            },
        );
        Ok(entries)
    }

    fn load_dir_tree(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let mut entries = Vec::new();
        if let Some(parent) = path.parent() {
            entries.push(FileEntry {
                name: "..".to_string(),
                path: parent.to_path_buf(),
                is_dir: true,
                is_parent: true,
                depth: 0,
                size: 0,
                modified: None,
            });
        }
        self.build_tree(path, 0, 4, &mut entries)?;
        Ok(entries)
    }

    fn load_dir_recursive(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let mut entries = Vec::new();
        let mut stack = vec![(path.to_path_buf(), 0)];
        let max_files = 20000;
        let max_depth = 20;

        let skip_dirs = ["node_modules", ".git"];

        while let Some((curr_path, depth)) = stack.pop() {
            if entries.len() >= max_files || depth > max_depth {
                continue;
            }

            if let Ok(dir_entries) = fs::read_dir(&curr_path) {
                for entry in dir_entries.flatten() {
                    let p = entry.path();
                    let name = entry.file_name().to_string_lossy().to_string();

                    let metadata = fs::metadata(&p).ok();
                    let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
                    let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
                    let modified = metadata.and_then(|m| m.modified().ok());

                    entries.push(FileEntry {
                        name: name.clone(),
                        path: p.clone(),
                        is_dir,
                        is_parent: false,
                        depth,
                        size,
                        modified,
                    });

                    if is_dir && !skip_dirs.contains(&name.as_str()) {
                        stack.push((p, depth + 1));
                    }
                }
            }
        }

        entries.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        Ok(entries)
    }

    fn build_tree(
        &self,
        path: &Path,
        depth: usize,
        max_depth: usize,
        entries: &mut Vec<FileEntry>,
    ) -> Result<()> {
        let dir_entries = match fs::read_dir(path) {
            Ok(e) => e,
            Err(_) => return Ok(()),
        };

        let mut children = Vec::new();
        for entry in dir_entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let metadata = fs::metadata(&path).ok();
            let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
            let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            let modified = metadata.and_then(|m| m.modified().ok());

            children.push(FileEntry {
                name,
                path,
                is_dir,
                is_parent: false,
                depth,
                size,
                modified,
            });
        }
        children.sort_by(|a, b| match (a.is_dir, b.is_dir) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
        });

        for child in children {
            let is_expanded = child.is_dir && self.expanded.contains(&child.path);
            let child_path = child.path.clone();
            entries.push(child);
            if is_expanded && depth + 1 < max_depth {
                let _ = self.build_tree(&child_path, depth + 1, max_depth, entries);
            }
        }
        Ok(())
    }

    pub fn selected_entry(&self) -> Option<&FileEntry> {
        self.entries.get(self.selected)
    }

    pub fn move_up(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
        }
    }

    pub fn move_down(&mut self) {
        if self.selected + 1 < self.entries.len() {
            self.selected += 1;
        }
    }

    pub fn page_up(&mut self) {
        let step = 10;
        if self.selected >= step {
            self.selected -= step;
        } else {
            self.selected = 0;
        }
    }

    pub fn page_down(&mut self) {
        let step = 10;
        self.selected = (self.selected + step).min(self.entries.len().saturating_sub(1));
    }

    pub fn go_parent(&mut self) {
        if let Some(parent) = self.cwd.parent() {
            self.try_set_cwd(parent.to_path_buf());
        }
    }

    pub fn toggle_view(&mut self) {
        self.view = match self.view {
            PickerView::List => PickerView::Tree,
            PickerView::Tree => PickerView::List,
        };
        self.refresh();
    }

    pub fn toggle_expand(&mut self) {
        let entry = self.selected_entry().cloned();
        if let Some(entry) = entry
            && entry.is_dir
            && !entry.is_parent
        {
            if self.expanded.contains(&entry.path) {
                self.expanded.remove(&entry.path);
            } else {
                self.expanded.insert(entry.path);
            }
            if self.view == PickerView::Tree {
                self.refresh();
            }
        }
    }

    pub fn toggle_select(&mut self) {
        let entry = self.selected_entry().cloned();
        if let Some(entry) = entry {
            if entry.is_parent {
                return;
            }
            if self.selected_paths.contains(&entry.path) {
                self.selected_paths.remove(&entry.path);
            } else {
                self.selected_paths.insert(entry.path);
            }
        }
    }

    pub fn clear_selected(&mut self) {
        self.selected_paths.clear();
    }
}
