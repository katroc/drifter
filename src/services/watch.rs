use crate::services::ingest::ingest_path;
use notify::{
    Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode,
    Watcher as NotifyWatcher,
};
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const WATCH_EVENT_BUFFER: usize = 2048;
const WATCH_FLUSH_INTERVAL_MS: u64 = 250;
const WATCH_DEBOUNCE_MS: u64 = 1200;
const WATCH_MAX_IN_FLIGHT: usize = 4;

#[allow(dead_code)]
pub struct Watcher {
    conn: Arc<Mutex<Connection>>,
    watcher: Option<RecommendedWatcher>,
    event_tx: Option<mpsc::Sender<PathBuf>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

#[allow(dead_code)]
impl Watcher {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            watcher: None,
            event_tx: None,
            shutdown_tx: None,
            worker_handle: None,
        }
    }

    pub fn start(&mut self, path: PathBuf) {
        self.stop();

        info!("Starting directory watcher on: {:?}", path);
        let (event_tx, event_rx) = mpsc::channel(WATCH_EVENT_BUFFER);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker_conn = self.conn.clone();
        let worker_handle = tokio::spawn(async move {
            run_watch_worker(worker_conn, event_rx, shutdown_rx).await;
        });

        let callback_tx = event_tx.clone();

        let watcher_result = RecommendedWatcher::new(
            move |res: notify::Result<Event>| match res {
                Ok(event) => {
                    debug!("Watcher event: {:?}", event);
                    match event.kind {
                        EventKind::Create(_) | EventKind::Modify(_) => {
                            for event_path in event.paths {
                                if !event_path.is_file() {
                                    continue;
                                }

                                if let Err(send_err) = callback_tx.try_send(event_path.clone()) {
                                    match send_err {
                                        mpsc::error::TrySendError::Full(dropped_path) => warn!(
                                            "Watcher event buffer full, dropping path: {:?}",
                                            dropped_path
                                        ),
                                        mpsc::error::TrySendError::Closed(dropped_path) => warn!(
                                            "Watcher queue closed, dropping path: {:?}",
                                            dropped_path
                                        ),
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Err(e) => error!("Watch error: {:?}", e),
            },
            NotifyConfig::default(),
        );

        match watcher_result {
            Ok(mut watcher) => {
                if let Err(e) = watcher.watch(&path, RecursiveMode::Recursive) {
                    error!("Failed to start watcher: {:?}", e);
                    let _ = shutdown_tx.send(true);
                    worker_handle.abort();
                } else {
                    info!("Watcher started successfully");
                    self.event_tx = Some(event_tx);
                    self.shutdown_tx = Some(shutdown_tx);
                    self.worker_handle = Some(worker_handle);
                    self.watcher = Some(watcher);
                }
            }
            Err(e) => {
                error!("Failed to create watcher: {:?}", e);
                let _ = shutdown_tx.send(true);
                worker_handle.abort();
            }
        }
    }

    pub fn stop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(worker_handle) = self.worker_handle.take() {
            worker_handle.abort();
        }
        self.event_tx = None;
        self.watcher = None;
    }
}

async fn run_watch_worker(
    conn: Arc<Mutex<Connection>>,
    mut event_rx: mpsc::Receiver<PathBuf>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let debounce_window = Duration::from_millis(WATCH_DEBOUNCE_MS);
    let mut flush_tick = tokio::time::interval(Duration::from_millis(WATCH_FLUSH_INTERVAL_MS));
    flush_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut pending: HashMap<PathBuf, Instant> = HashMap::new();
    let mut in_flight: HashSet<PathBuf> = HashSet::new();
    let mut join_set: JoinSet<(PathBuf, anyhow::Result<()>)> = JoinSet::new();

    info!("Watch worker started");
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            Some(path) = event_rx.recv() => {
                pending.insert(path, Instant::now());
            }
            Some(joined) = join_set.join_next(), if !in_flight.is_empty() => {
                match joined {
                    Ok((path, result)) => {
                        in_flight.remove(&path);
                        if let Err(e) = result {
                            error!("Failed to ingest watched file {:?}: {}", path, e);
                        }
                    }
                    Err(e) => error!("Watch ingest worker task failed: {}", e),
                }
            }
            _ = flush_tick.tick() => {
                let now = Instant::now();
                let mut ready_paths = Vec::new();

                pending.retain(|path, last_seen| {
                    if now.duration_since(*last_seen) >= debounce_window {
                        ready_paths.push(path.clone());
                        false
                    } else {
                        true
                    }
                });

                for path in ready_paths {
                    if in_flight.contains(&path) || in_flight.len() >= WATCH_MAX_IN_FLIGHT {
                        pending.insert(path, now);
                        continue;
                    }

                    in_flight.insert(path.clone());
                    let conn_clone = conn.clone();
                    join_set.spawn(async move {
                        let is_file = tokio::fs::metadata(&path)
                            .await
                            .map(|m| m.is_file())
                            .unwrap_or(false);
                        if !is_file {
                            return (path, Ok(()));
                        }

                        let session_id = Uuid::new_v4().to_string();
                        let path_str = path.to_string_lossy().to_string();
                        let result =
                            ingest_path(conn_clone, &path_str, &session_id, None).await.map(|_| ());
                        (path, result)
                    });
                }
            }
            else => {
                if event_rx.is_closed() && pending.is_empty() && in_flight.is_empty() {
                    break;
                }
            }
        }
    }

    join_set.abort_all();
    while let Some(joined) = join_set.join_next().await {
        if let Ok((path, _)) = joined {
            in_flight.remove(&path);
        }
    }
    info!("Watch worker stopped");
}
