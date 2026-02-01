use crate::config::Config;
use crate::services::ingest::ingest_path;
use notify::{Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher};
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{info, error, debug, warn};

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
        info!("Starting directory watcher on: {:?}", path);
        let conn = self.conn.clone();
        let staging_dir = self.config.staging_dir.clone();
        let staging_mode = self.config.staging_mode.clone();

        // Create a watcher that runs on a separate thread (managed by notify)
        let watcher_result = RecommendedWatcher::new(
            move |res: notify::Result<Event>| {
                match res {
                    Ok(event) => {
                        debug!("Watcher event: {:?}", event);
                        match event.kind {
                            EventKind::Create(_) | EventKind::Modify(_) => {
                                for path in event.paths {
                                    if path.is_file() {
                                        info!("New file detected: {:?}", path);
                                        // Locking here inside the callback is safe as long as we don't deadlock.
                                        // ingest_path might take time, blocking notifier thread, but that's ok for now.
                                        if let Ok(conn) = conn.lock() {
                                            if let Err(e) = ingest_path(&conn, &staging_dir, &staging_mode, &path.to_string_lossy()) {
                                                error!("Failed to ingest file {:?}: {}", path, e);
                                            }
                                        } else {
                                            warn!("Failed to lock database for ingestion");
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => error!("Watch error: {:?}", e),
                }
            },
            NotifyConfig::default(),
        );

        match watcher_result {
            Ok(mut watcher) => {
                 if let Err(e) = watcher.watch(&path, RecursiveMode::Recursive) {
                    error!("Failed to start watcher: {:?}", e);
                } else {
                    info!("Watcher started successfully");
                }
                self.watcher = Some(watcher);
            }
            Err(e) => {
                error!("Failed to create watcher: {:?}", e);
            }
        }
    }

    pub fn stop(&mut self) {
        self.watcher = None;
    }
}
