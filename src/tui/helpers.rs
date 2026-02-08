use crate::app::state::{App, AppEvent, LayoutTarget, RemoteTarget};
use crate::core::config::Config;
use crate::core::transfer::EndpointKind;
use crate::db;
use crate::services::uploader::{S3Object, Uploader};
use crate::utils::lock_mutex;
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;
use uuid::Uuid;

const REMOTE_CACHE_TTL_SECS: u64 = 30;

fn endpoint_config_string(config: &serde_json::Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(std::string::ToString::to_string)
}

fn apply_endpoint_to_config(
    cfg: &mut Config,
    profile: &db::EndpointProfileRow,
    secret: Option<String>,
) {
    cfg.s3_bucket = endpoint_config_string(&profile.config, "bucket");
    cfg.s3_prefix = endpoint_config_string(&profile.config, "prefix");
    cfg.s3_region = endpoint_config_string(&profile.config, "region");
    cfg.s3_endpoint = endpoint_config_string(&profile.config, "endpoint");
    cfg.s3_access_key = endpoint_config_string(&profile.config, "access_key");
    cfg.s3_secret_key = secret;
}

pub(crate) async fn config_for_endpoint(app: &App, endpoint_id: Option<i64>) -> Result<Config> {
    let endpoint_id = endpoint_id.context("no endpoint selected")?;
    let mut cfg = app.config.lock().await.clone();
    let (profile, secret) = {
        let conn = lock_mutex(&app.conn)?;
        let profile = db::get_endpoint_profile(&conn, endpoint_id)?
            .with_context(|| format!("endpoint profile id={} not found", endpoint_id))?;
        if profile.kind != EndpointKind::S3 {
            anyhow::bail!("endpoint '{}' is not S3", profile.name);
        }
        let secret = if let Some(secret_ref) = profile.credential_ref.as_deref() {
            db::get_secret(&conn, secret_ref)?
        } else {
            None
        };
        (profile, secret)
    };
    apply_endpoint_to_config(&mut cfg, &profile, secret);
    Ok(cfg)
}

fn remote_cache_key(endpoint_id: Option<i64>, path: &str) -> String {
    match endpoint_id {
        Some(id) => format!("{id}::{path}"),
        None => format!("none::{path}"),
    }
}

fn resolve_local_endpoint_id(conn: &rusqlite::Connection) -> Result<Option<i64>> {
    if let Some(profile) = db::get_default_source_endpoint_profile(conn)?
        && profile.kind == EndpointKind::Local
    {
        return Ok(Some(profile.id));
    }
    if let Some(profile) = db::get_default_destination_endpoint_profile(conn)?
        && profile.kind == EndpointKind::Local
    {
        return Ok(Some(profile.id));
    }
    Ok(db::list_endpoint_profiles(conn)?
        .into_iter()
        .find(|profile| profile.kind == EndpointKind::Local)
        .map(|profile| profile.id))
}

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
    let endpoint_id = app.primary_remote_endpoint_id();
    if endpoint_id.is_none() {
        app.s3_objects.clear();
        app.selected_remote = 0;
        app.remote_loading = false;
        app.remote_request_pending = None;
        app.set_status("No S3 endpoint selected for this panel.".to_string());
        return false;
    }

    let current_path = app.remote_current_path.clone();
    let request_key = remote_cache_key(endpoint_id, &current_path);

    if let Some(ref pending) = app.remote_request_pending
        && pending == &request_key
    {
        return false;
    }

    if !force_refresh
        && let Some((cached_files, fetched_at)) = app.remote_cache.get(&request_key)
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
    app.remote_request_pending = Some(request_key);
    app.set_status("Loading remote...".to_string());

    let tx = app.async_tx.clone();
    let config_clone = match config_for_endpoint(app, endpoint_id).await {
        Ok(cfg) => cfg,
        Err(e) => {
            app.remote_loading = false;
            app.remote_request_pending = None;
            app.set_status(format!("Remote endpoint config error: {}", e));
            return false;
        }
    };
    let path_for_request = current_path.clone();

    tokio::spawn(async move {
        let path_arg = if path_for_request.is_empty() {
            None
        } else {
            Some(path_for_request.as_str())
        };
        match Uploader::list_bucket_contents(&config_clone, path_arg).await {
            Ok(files) => {
                if let Err(e) = tx.send(AppEvent::RemoteFileList(
                    endpoint_id,
                    path_for_request,
                    files,
                )) {
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

pub(crate) async fn request_secondary_remote_list(app: &mut App, force_refresh: bool) -> bool {
    let endpoint_id = app.secondary_remote_endpoint_id();
    if endpoint_id.is_none() {
        app.s3_objects_secondary.clear();
        app.selected_remote_secondary = 0;
        app.remote_secondary_loading = false;
        app.remote_secondary_request_pending = None;
        app.set_status("No S3 destination endpoint selected.".to_string());
        return false;
    }

    let current_path = app.remote_secondary_current_path.clone();
    let request_key = remote_cache_key(endpoint_id, &current_path);

    if let Some(ref pending) = app.remote_secondary_request_pending
        && pending == &request_key
    {
        return false;
    }

    if !force_refresh
        && let Some((cached_files, fetched_at)) = app.remote_secondary_cache.get(&request_key)
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
        app.s3_objects_secondary = files;
        app.selected_remote_secondary = 0;
        app.set_status(format!(
            "Loaded {} items from secondary cache",
            app.s3_objects_secondary.len()
        ));
        return false;
    }

    let config_clone = match config_for_endpoint(app, endpoint_id).await {
        Ok(cfg) => cfg,
        Err(e) => {
            app.s3_objects_secondary.clear();
            app.selected_remote_secondary = 0;
            app.set_status(format!("Secondary endpoint config error: {}", e));
            return false;
        }
    };

    app.remote_secondary_loading = true;
    app.remote_secondary_request_pending = Some(request_key);
    app.set_status("Loading secondary remote...".to_string());

    let tx = app.async_tx.clone();
    let path_for_request = current_path.clone();

    tokio::spawn(async move {
        let path_arg = if path_for_request.is_empty() {
            None
        } else {
            Some(path_for_request.as_str())
        };
        match Uploader::list_bucket_contents(&config_clone, path_arg).await {
            Ok(files) => {
                if let Err(e) = tx.send(AppEvent::RemoteFileListSecondary(
                    endpoint_id,
                    path_for_request,
                    files,
                )) {
                    warn!("Failed to send secondary remote file list event: {}", e);
                }
            }
            Err(e) => {
                if let Err(send_err) = tx.send(AppEvent::Notification(format!(
                    "Secondary list failed: {}",
                    e
                ))) {
                    warn!(
                        "Failed to send secondary remote list failure notification: {}",
                        send_err
                    );
                }
            }
        }
    });

    true
}

pub(crate) fn selected_remote_object(app: &App) -> Option<S3Object> {
    app.s3_objects.get(app.selected_remote).cloned()
}

pub(crate) fn selected_secondary_remote_object(app: &App) -> Option<S3Object> {
    app.s3_objects_secondary
        .get(app.selected_remote_secondary)
        .cloned()
}

pub(crate) async fn start_remote_download(app: &mut App, key: String) {
    app.set_status(format!("Downloading {}...", key));
    let tx = app.async_tx.clone();
    let config_clone = match config_for_endpoint(app, app.primary_remote_endpoint_id()).await {
        Ok(cfg) => cfg,
        Err(e) => {
            app.set_status(format!("Download endpoint error: {}", e));
            return;
        }
    };

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

fn normalize_s3_prefix(prefix: Option<&str>) -> Option<String> {
    let raw = prefix?.trim();
    if raw.is_empty() {
        return None;
    }
    let trimmed = raw.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(format!("{trimmed}/"))
    }
}

fn relative_key_from_source_prefix(key: &str, source_prefix: Option<&str>) -> String {
    let normalized_key = key.trim_start_matches('/');
    if let Some(prefix) = normalize_s3_prefix(source_prefix)
        && let Some(stripped) = normalized_key.strip_prefix(&prefix)
    {
        return stripped.to_string();
    }
    normalized_key.to_string()
}

pub(crate) async fn queue_remote_download(app: &mut App, obj: S3Object, destination_root: PathBuf) {
    queue_remote_download_batch(app, vec![obj], destination_root).await;
}

pub(crate) async fn queue_remote_download_batch(
    app: &mut App,
    objects: Vec<S3Object>,
    destination_root: PathBuf,
) {
    if objects.is_empty() {
        app.set_status("No remote objects selected to queue.".to_string());
        return;
    }

    if objects.len() == 1 {
        app.set_status(format!("Planning download for {}...", objects[0].name));
    } else {
        app.set_status(format!(
            "Planning download for {} selected items...",
            objects.len()
        ));
    }

    let mut files_to_queue: Vec<(String, i64, PathBuf)> = Vec::new();
    let mut seen_source_keys = HashSet::new();
    let source_endpoint_id = app.primary_remote_endpoint_id();
    let config_clone = match config_for_endpoint(app, source_endpoint_id).await {
        Ok(cfg) => cfg,
        Err(e) => {
            app.set_status(format!("Source endpoint error: {}", e));
            return;
        }
    };

    for obj in objects {
        if obj.is_parent {
            continue;
        }

        if obj.is_dir {
            let mut folder_key = obj.key.clone();
            if !folder_key.ends_with('/') {
                folder_key.push('/');
            }

            let files = match Uploader::list_files_recursive(&config_clone, &folder_key).await {
                Ok(files) => files,
                Err(e) => {
                    app.set_status(format!("Queue failed: {}", e));
                    return;
                }
            };

            let root_name = std::path::Path::new(folder_key.trim_end_matches('/'))
                .file_name()
                .and_then(|n| n.to_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("downloaded_folder")
                .to_string();

            for file in files {
                if !seen_source_keys.insert(file.key.clone()) {
                    continue;
                }
                let relative = if let Some(rel) = file.key.strip_prefix(&folder_key) {
                    rel.to_string()
                } else {
                    std::path::Path::new(&file.key)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("downloaded_file")
                        .to_string()
                };
                let dest = destination_root.join(&root_name).join(relative);
                files_to_queue.push((file.key, file.size, dest));
            }
        } else {
            if !seen_source_keys.insert(obj.key.clone()) {
                continue;
            }
            let destination = destination_root.join(
                std::path::Path::new(&obj.key)
                    .file_name()
                    .unwrap_or(std::ffi::OsStr::new("downloaded_file")),
            );
            files_to_queue.push((obj.key, obj.size, destination));
        }
    }

    if files_to_queue.is_empty() {
        app.set_status("No files found in selected objects.".to_string());
        return;
    }

    let session_id = Uuid::new_v4().to_string();
    let result = (|| -> anyhow::Result<(usize, usize, Option<i64>)> {
        let conn = lock_mutex(&app.conn)?;
        let source_ep = source_endpoint_id;
        let destination_ep = resolve_local_endpoint_id(&conn)?;

        let mut queued = 0usize;
        let mut failed = 0usize;
        let mut first_job_id = None;
        for (key, size, destination) in files_to_queue {
            match db::create_job(&conn, &session_id, &key, size, Some(&key)) {
                Ok(job_id) => {
                    if let Err(e) = db::update_job_transfer_metadata(
                        &conn,
                        job_id,
                        &db::JobTransferMetadata {
                            source_endpoint_id: source_ep,
                            destination_endpoint_id: destination_ep,
                            transfer_direction: Some("s3_to_local".to_string()),
                            conflict_policy: Some("overwrite".to_string()),
                            scan_policy: Some("never".to_string()),
                        },
                    ) {
                        warn!("Failed to set transfer metadata for job {}: {}", job_id, e);
                        let _ = db::update_job_error_with_status(
                            &conn,
                            job_id,
                            db::JobStatus::Failed,
                            &format!("failed to set transfer metadata: {}", e),
                        );
                        failed += 1;
                        continue;
                    }
                    if let Err(e) = db::update_job_staged_with_status(
                        &conn,
                        job_id,
                        &destination.to_string_lossy(),
                        db::JobStatus::Queued,
                    ) {
                        warn!("Failed to stage transfer job {}: {}", job_id, e);
                        let _ = db::update_job_error_with_status(
                            &conn,
                            job_id,
                            db::JobStatus::Failed,
                            &format!("failed to set destination path: {}", e),
                        );
                        failed += 1;
                        continue;
                    }
                    if first_job_id.is_none() {
                        first_job_id = Some(job_id);
                    }
                    if let Err(e) = db::insert_event(&conn, job_id, "ingest", "queued for transfer")
                    {
                        warn!("Failed to add ingest event for job {}: {}", job_id, e);
                    }
                    queued += 1;
                }
                Err(e) => {
                    warn!("Failed to create transfer job for {}: {}", key, e);
                    failed += 1;
                }
            }
        }

        Ok((queued, failed, first_job_id))
    })();

    match result {
        Ok((queued, failed, first_job_id)) => {
            if queued == 0 {
                app.set_status("No transfer jobs were queued.".to_string());
            } else if failed == 0 {
                if let Some(job_id) = first_job_id {
                    app.set_status(format!(
                        "Queued {} transfer job(s) (session {}, first job #{})",
                        queued, session_id, job_id
                    ));
                } else {
                    app.set_status(format!(
                        "Queued {} transfer job(s) (session {})",
                        queued, session_id
                    ));
                }
            } else {
                app.set_status(format!(
                    "Queued {} transfer job(s), {} failed",
                    queued, failed
                ));
            }
        }
        Err(e) => {
            app.set_status(format!("Queue download failed: {}", e));
        }
    }
}

pub(crate) async fn queue_remote_s3_copy(app: &mut App, obj: S3Object) {
    queue_remote_s3_copy_batch(app, vec![obj]).await;
}

pub(crate) async fn queue_remote_s3_copy_batch(app: &mut App, objects: Vec<S3Object>) {
    if objects.is_empty() {
        app.set_status("No remote objects selected to queue.".to_string());
        return;
    }

    let source_endpoint_id = app.transfer_source_endpoint_id;
    let destination_endpoint_id = app.transfer_destination_endpoint_id;
    let config_clone = match config_for_endpoint(app, source_endpoint_id).await {
        Ok(cfg) => cfg,
        Err(e) => {
            app.set_status(format!("Source endpoint error: {}", e));
            return;
        }
    };
    let source_prefix = config_clone.s3_prefix.clone();
    let destination_base = if app.remote_secondary_current_path.is_empty() {
        String::new()
    } else if app.remote_secondary_current_path.ends_with('/') {
        app.remote_secondary_current_path.clone()
    } else {
        format!("{}/", app.remote_secondary_current_path)
    };

    if objects.len() == 1 {
        app.set_status(format!("Planning S3 copy for {}...", objects[0].name));
    } else {
        app.set_status(format!(
            "Planning S3 copy for {} selected items...",
            objects.len()
        ));
    }

    let mut files_to_queue: Vec<(String, i64, String)> = Vec::new();
    let mut seen_source_keys = HashSet::new();
    for obj in objects {
        if obj.is_parent {
            continue;
        }

        if obj.is_dir {
            let mut folder_key = obj.key.clone();
            if !folder_key.ends_with('/') {
                folder_key.push('/');
            }

            let files = match Uploader::list_files_recursive(&config_clone, &folder_key).await {
                Ok(files) => files,
                Err(e) => {
                    app.set_status(format!("Queue failed: {}", e));
                    return;
                }
            };

            for file in files {
                let relative_key =
                    relative_key_from_source_prefix(&file.key, source_prefix.as_deref());
                if !seen_source_keys.insert(relative_key.clone()) {
                    continue;
                }
                let destination_key = format!("{}{}", destination_base, relative_key);
                files_to_queue.push((relative_key, file.size, destination_key));
            }
        } else {
            let relative_key = relative_key_from_source_prefix(&obj.key, source_prefix.as_deref());
            if !seen_source_keys.insert(relative_key.clone()) {
                continue;
            }
            let destination_key = format!("{}{}", destination_base, relative_key);
            files_to_queue.push((relative_key, obj.size, destination_key));
        }
    }

    if files_to_queue.is_empty() {
        app.set_status("No files found in selected objects.".to_string());
        return;
    }

    let session_id = Uuid::new_v4().to_string();
    let result = (|| -> anyhow::Result<(usize, usize, Option<i64>)> {
        let conn = lock_mutex(&app.conn)?;
        let Some(source_ep) = source_endpoint_id else {
            anyhow::bail!("Source S3 endpoint is not selected.");
        };
        let Some(destination_ep) = destination_endpoint_id else {
            anyhow::bail!("Destination S3 endpoint is not selected.");
        };

        let mut queued = 0usize;
        let mut failed = 0usize;
        let mut first_job_id = None;
        for (source_key, size, destination_key) in files_to_queue {
            match db::create_job(&conn, &session_id, &source_key, size, Some(&source_key)) {
                Ok(job_id) => {
                    if let Err(e) = db::update_job_transfer_metadata(
                        &conn,
                        job_id,
                        &db::JobTransferMetadata {
                            source_endpoint_id: Some(source_ep),
                            destination_endpoint_id: Some(destination_ep),
                            transfer_direction: Some("s3_to_s3".to_string()),
                            conflict_policy: Some("overwrite".to_string()),
                            scan_policy: Some("never".to_string()),
                        },
                    ) {
                        warn!("Failed to set transfer metadata for job {}: {}", job_id, e);
                        let _ = db::update_job_error_with_status(
                            &conn,
                            job_id,
                            db::JobStatus::Failed,
                            &format!("failed to set transfer metadata: {}", e),
                        );
                        failed += 1;
                        continue;
                    }
                    if let Err(e) = db::update_job_staged_with_status(
                        &conn,
                        job_id,
                        &destination_key,
                        db::JobStatus::Queued,
                    ) {
                        warn!("Failed to stage transfer job {}: {}", job_id, e);
                        let _ = db::update_job_error_with_status(
                            &conn,
                            job_id,
                            db::JobStatus::Failed,
                            &format!("failed to set destination key: {}", e),
                        );
                        failed += 1;
                        continue;
                    }
                    if first_job_id.is_none() {
                        first_job_id = Some(job_id);
                    }
                    if let Err(e) =
                        db::insert_event(&conn, job_id, "ingest", "queued for s3_to_s3 transfer")
                    {
                        warn!("Failed to add ingest event for job {}: {}", job_id, e);
                    }
                    queued += 1;
                }
                Err(e) => {
                    warn!("Failed to create transfer job for {}: {}", source_key, e);
                    failed += 1;
                }
            }
        }

        Ok((queued, failed, first_job_id))
    })();

    match result {
        Ok((queued, failed, first_job_id)) => {
            if queued == 0 {
                app.set_status("No transfer jobs were queued.".to_string());
            } else if failed == 0 {
                if let Some(job_id) = first_job_id {
                    app.set_status(format!(
                        "Queued {} S3 copy job(s) (session {}, first job #{})",
                        queued, session_id, job_id
                    ));
                } else {
                    app.set_status(format!(
                        "Queued {} S3 copy job(s) (session {})",
                        queued, session_id
                    ));
                }
            } else {
                app.set_status(format!(
                    "Queued {} S3 copy job(s), {} failed",
                    queued, failed
                ));
            }
        }
        Err(e) => {
            app.set_status(format!("Queue S3 copy failed: {}", e));
        }
    }
}

pub(crate) async fn prepare_remote_delete(
    app: &mut App,
    key: String,
    name: String,
    is_dir: bool,
    target: RemoteTarget,
) {
    app.confirmation_return_mode = Some(app.input_mode);

    if key == ".." || name == ".." {
        app.set_status("Cannot delete parent entry.".to_string());
        return;
    }

    let current_path = match target {
        RemoteTarget::Primary => app.remote_current_path.clone(),
        RemoteTarget::Secondary => app.remote_secondary_current_path.clone(),
    };
    let endpoint_id = match target {
        RemoteTarget::Primary => app.primary_remote_endpoint_id(),
        RemoteTarget::Secondary => app.secondary_remote_endpoint_id(),
    };
    let config_clone = match config_for_endpoint(app, endpoint_id).await {
        Ok(cfg) => cfg,
        Err(e) => {
            app.set_status(format!("Remote endpoint error: {}", e));
            return;
        }
    };

    if is_dir {
        let dir_key = if key.ends_with('/') {
            key
        } else {
            format!("{}/", key)
        };
        match Uploader::is_folder_empty(&config_clone, &dir_key).await {
            Ok(true) => {
                app.input_mode = crate::app::state::InputMode::Confirmation;
                app.pending_action = crate::app::state::ModalAction::DeleteRemoteObject(
                    dir_key,
                    current_path,
                    true,
                    target,
                );
                app.confirmation_msg = format!("Delete empty folder '{}'?", name);
            }
            Ok(false) => {
                app.input_mode = crate::app::state::InputMode::Confirmation;
                app.pending_action = crate::app::state::ModalAction::DeleteRemoteObject(
                    dir_key,
                    current_path,
                    true,
                    target,
                );
                app.confirmation_msg = format!("Delete folder '{}' and all contents?", name);
            }
            Err(e) => {
                app.set_status(format!("Folder check failed: {}", e));
            }
        }
    } else {
        app.input_mode = crate::app::state::InputMode::Confirmation;
        app.pending_action =
            crate::app::state::ModalAction::DeleteRemoteObject(key, current_path, false, target);
        app.confirmation_msg = format!("Delete '{}'?", name);
    }
}
