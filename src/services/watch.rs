use crate::core::config::Config;
use crate::services::ingest::ingest_path;
use notify::{
    Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode,
    Watcher as NotifyWatcher,
};
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info};
use uuid::Uuid;

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
                                        let conn_clone = conn.clone();
                                        let staging_dir_clone = staging_dir.clone();
                                        let staging_mode_clone = staging_mode.clone();
                                        let path_str = path.to_string_lossy().to_string();

                                        tokio::spawn(async move {
                                            let session_id = Uuid::new_v4().to_string();
                                            if let Err(e) = ingest_path(
                                                conn_clone,
                                                &staging_dir_clone,
                                                &staging_mode_clone,
                                                &path_str,
                                                &session_id,
                                            )
                                            .await
                                            {
                                                error!("Failed to ingest file {}: {}", path_str, e);
                                            }
                                        });
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
