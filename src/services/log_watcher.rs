use crate::app::state::AppEvent;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

pub struct LogWatcher {
    path: PathBuf,
    tx: Sender<AppEvent>,
}

impl LogWatcher {
    pub fn new(path: PathBuf, tx: Sender<AppEvent>) -> Self {
        Self { path, tx }
    }

    pub fn start(self) {
        thread::spawn(move || {
            let mut last_size = if let Ok(metadata) = std::fs::metadata(&self.path) {
                // Start with the last 10KB of history
                metadata.len().saturating_sub(10000)
            } else {
                0
            };

            loop {
                if let Ok(file) = File::open(&self.path)
                    && let Ok(metadata) = file.metadata()
                {
                    let current_size = metadata.len();

                    if current_size < last_size {
                        // File truncated or rotated
                        last_size = 0;
                    }

                    if current_size > last_size {
                        let mut reader = BufReader::new(file);
                        if reader.seek(SeekFrom::Start(last_size)).is_ok() {
                            let mut line = String::new();
                            // Read everything new since last_size
                            while let Ok(n) = reader.read_line(&mut line) {
                                if n == 0 {
                                    break;
                                }
                                let _ =
                                    self.tx.send(AppEvent::LogLine(line.trim_end().to_string()));
                                line.clear();
                            }
                            last_size = current_size;
                        }
                    }
                }

                thread::sleep(Duration::from_millis(250)); // Slightly faster refresh
            }
        });
    }
}
