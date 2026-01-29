use crate::config::Config;
use crate::ingest::ingest_path;
use notify::{Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher};
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[allow(dead_code)]
pub struct Watcher {
    conn: Arc<Mutex<Connection>>,
    config: Config,
    watcher: Option<RecommendedWatcher>,
}

#[allow(dead_code)]
impl Watcher {
    pub fn new(conn: Arc<Mutex<Connection>>, config: Config) -> Self {
        Self {
            conn,
            config,
            watcher: None,
        }
    }

    pub fn start(&mut self, path: PathBuf) {
        let conn = self.conn.clone();
        let staging_dir = self.config.staging_dir.clone();

        // Create a watcher that runs on a separate thread (managed by notify)
        let watcher_result = RecommendedWatcher::new(
            move |res: notify::Result<Event>| {
                match res {
                    Ok(event) => {
                        match event.kind {
                            EventKind::Create(_) | EventKind::Modify(_) => {
                                for path in event.paths {
                                    if path.is_file() {
                                        // Locking here inside the callback is safe as long as we don't deadlock.
                                        // ingest_path might take time, blocking notifier thread, but that's ok for now.
                                        if let Ok(conn) = conn.lock() {
                                            // Ignore errors for now
                                            let _ = ingest_path(&conn, &staging_dir, &path.to_string_lossy());
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => println!("Watch error: {:?}", e),
                }
            },
            NotifyConfig::default(),
        );

        match watcher_result {
            Ok(mut watcher) => {
                 if let Err(e) = watcher.watch(&path, RecursiveMode::Recursive) {
                    println!("Failed to start watcher: {:?}", e);
                }
                self.watcher = Some(watcher);
            }
            Err(e) => {
                println!("Failed to create watcher: {:?}", e);
            }
        }
    }

    pub fn stop(&mut self) {
        self.watcher = None;
    }
}
