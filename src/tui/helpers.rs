use crate::app::state::{App, AppEvent, LayoutTarget};
use crate::core::config::Config;
use crate::services::uploader::{S3Object, Uploader};
use std::path::PathBuf;
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
                if let Err(send_err) =
                    tx.send(AppEvent::Notification(format!("List Failed: {}", e)))
                {
                    warn!(
                        "Failed to send remote list failure notification: {}",
                        send_err
                    );
                }
            }
        }
    });

    true
}

pub(crate) fn s3_ready(config: &Config) -> bool {
    config.is_s3_ready()
}

pub(crate) fn selected_remote_object(app: &App) -> Option<S3Object> {
    app.s3_objects.get(app.selected_remote).cloned()
}

pub(crate) async fn start_remote_download(app: &mut App, key: String) {
    app.set_status(format!("Downloading {}...", key));
    let tx = app.async_tx.clone();
    let config_clone = app.config.lock().await.clone();

    let download_dir = dirs::download_dir().unwrap_or_else(|| PathBuf::from("."));
    let dest = download_dir.join(
        std::path::Path::new(&key)
            .file_name()
            .unwrap_or(std::ffi::OsStr::new("downloaded_file")),
    );
    let dest_clone = dest.clone();

    tokio::spawn(async move {
        let res = Uploader::download_file(&config_clone, &key, &dest_clone).await;
        match res {
            Ok(_) => {
                if let Err(send_err) = tx.send(AppEvent::Notification(format!(
                    "Downloaded to {:?}",
                    dest_clone
                ))) {
                    warn!("Failed to send download success notification: {}", send_err);
                }
            }
            Err(e) => {
                if let Err(send_err) =
                    tx.send(AppEvent::Notification(format!("Download Failed: {}", e)))
                {
                    warn!("Failed to send download failure notification: {}", send_err);
                }
            }
        }
    });
}

pub(crate) async fn prepare_remote_delete(app: &mut App, key: String, name: String, is_dir: bool) {
    app.confirmation_return_mode = Some(app.input_mode);

    if is_dir {
        let dir_key = if key.ends_with('/') {
            key
        } else {
            format!("{}/", key)
        };
        let config_clone = app.config.lock().await.clone();
        match Uploader::is_folder_empty(&config_clone, &dir_key).await {
            Ok(true) => {
                app.input_mode = crate::app::state::InputMode::Confirmation;
                app.pending_action = crate::app::state::ModalAction::DeleteRemoteObject(
                    dir_key,
                    app.remote_current_path.clone(),
                    true,
                );
                app.confirmation_msg = format!("Delete empty folder '{}'?", name);
            }
            Ok(false) => {
                app.input_mode = crate::app::state::InputMode::Confirmation;
                app.pending_action = crate::app::state::ModalAction::DeleteRemoteObject(
                    dir_key,
                    app.remote_current_path.clone(),
                    true,
                );
                app.confirmation_msg = format!("Delete folder '{}' and all contents?", name);
            }
            Err(e) => {
                app.set_status(format!("Folder check failed: {}", e));
            }
        }
    } else {
        app.input_mode = crate::app::state::InputMode::Confirmation;
        app.pending_action = crate::app::state::ModalAction::DeleteRemoteObject(
            key,
            app.remote_current_path.clone(),
            false,
        );
        app.confirmation_msg = format!("Delete '{}'?", name);
    }
}
