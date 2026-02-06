use crate::app::state::{App, AppEvent, LayoutTarget};
use crate::core::config::Config;
use crate::services::uploader::{S3Object, Uploader};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;

const REMOTE_CACHE_TTL_SECS: u64 = 30;

pub(crate) async fn adjust_layout_dimension(
    config: &Arc<AsyncMutex<Config>>,
    target: LayoutTarget,
    delta: i16,
) {
    let mut cfg = config.lock().await;
    match target {
        LayoutTarget::Local => {
            cfg.local_width_percent =
                (cfg.local_width_percent as i16 + delta * 5).clamp(20, 80) as u16;
        }
        LayoutTarget::Queue => {
            cfg.local_width_percent =
                (cfg.local_width_percent as i16 - delta * 5).clamp(20, 80) as u16;
        }
        LayoutTarget::History => {
            cfg.history_width = (cfg.history_width as i16 + delta).clamp(40, 100) as u16;
        }
    }
}

pub(crate) async fn reset_layout_dimension(config: &Arc<AsyncMutex<Config>>, target: LayoutTarget) {
    let mut cfg = config.lock().await;
    let defaults = Config::default();
    match target {
        LayoutTarget::Local | LayoutTarget::Queue => {
            cfg.local_width_percent = defaults.local_width_percent
        }
        LayoutTarget::History => cfg.history_width = defaults.history_width,
    }
}

pub(crate) async fn reset_all_layout_dimensions(config: &Arc<AsyncMutex<Config>>) {
    let mut cfg = config.lock().await;
    let defaults = Config::default();
    cfg.local_width_percent = defaults.local_width_percent;
    cfg.history_width = defaults.history_width;
}

pub(crate) async fn update_layout_message(app: &mut App, target: LayoutTarget) {
    let cfg = app.config.lock().await;
    app.layout_adjust_message = match target {
        LayoutTarget::Local => format!("Local Width: {}% (20-80)", cfg.local_width_percent),
        LayoutTarget::Queue => format!("Queue Width: {}% (20-80)", 100 - cfg.local_width_percent),
        LayoutTarget::History => format!("History Width: {} chars (40-100)", cfg.history_width),
    };
}

/// Request remote list with caching and deduplication.
/// Returns true if a request was initiated, false if served from cache or skipped.
pub(crate) async fn request_remote_list(app: &mut App, force_refresh: bool) -> bool {
    let current_path = app.remote_current_path.clone();

    if let Some(ref pending) = app.remote_request_pending
        && pending == &current_path
    {
        return false;
    }

    if !force_refresh
        && let Some((cached_files, fetched_at)) = app.remote_cache.get(&current_path)
        && fetched_at.elapsed().as_secs() < REMOTE_CACHE_TTL_SECS
    {
        let mut files = cached_files.clone();
        if !current_path.is_empty() {
            files.insert(
                0,
                S3Object {
                    key: "..".to_string(),
                    name: "..".to_string(),
                    size: 0,
                    last_modified: String::new(),
                    is_dir: true,
                    is_parent: true,
                },
            );
        }
        app.s3_objects = files;
        app.selected_remote = 0;
        app.set_status(format!("Loaded {} items from cache", app.s3_objects.len()));
        return false;
    }

    app.remote_loading = true;
    app.remote_request_pending = Some(current_path.clone());
    app.set_status("Loading remote...".to_string());

    let tx = app.async_tx.clone();
    let config_clone = app.config.lock().await.clone();
    let path_for_request = current_path.clone();

    tokio::spawn(async move {
        let path_arg = if path_for_request.is_empty() {
            None
        } else {
            Some(path_for_request.as_str())
        };
        match Uploader::list_bucket_contents(&config_clone, path_arg).await {
            Ok(files) => {
                if let Err(e) = tx.send(AppEvent::RemoteFileList(path_for_request, files)) {
                    warn!("Failed to send remote file list event: {}", e);
                }
            }
            Err(e) => {
                if let Err(send_err) = tx.send(AppEvent::Notification(format!("List Failed: {}", e)))
                {
                    warn!("Failed to send remote list failure notification: {}", send_err);
                }
            }
        }
    });

    true
}

pub(crate) fn s3_ready(config: &Config) -> bool {
    let bucket_ok = config
        .s3_bucket
        .as_ref()
        .map(|b| !b.trim().is_empty())
        .unwrap_or(false);
    if !bucket_ok {
        return false;
    }

    let access_ok = config
        .s3_access_key
        .as_ref()
        .map(|k| !k.trim().is_empty())
        .unwrap_or(false);
    let secret_ok = config
        .s3_secret_key
        .as_ref()
        .map(|k| !k.trim().is_empty())
        .unwrap_or(false);

    !(access_ok ^ secret_ok)
}
