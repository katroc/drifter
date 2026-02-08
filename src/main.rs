mod app;
mod components;
mod coordinator;
mod core;
mod db;
mod logging;
mod services;
mod tui;
mod ui;
mod utils;

use crate::app::state::AppEvent;
use crate::coordinator::Coordinator;
use crate::core::config;
use crate::db::WizardStatus;
use crate::db::init_db;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex as StdMutex, mpsc};
use tokio::sync::Mutex as AsyncMutex;

fn config_requires_wizard(cfg: &config::Config) -> bool {
    if cfg.validate().is_err() {
        return true;
    }

    cfg.quarantine_dir.trim().is_empty()
        || cfg.reports_dir.trim().is_empty()
        || cfg.state_dir.trim().is_empty()
        || cfg.clamd_host.trim().is_empty()
        || cfg.clamd_port == 0
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let (_guard, log_handle) = logging::init();

    tracing::info!("Starting Drifter application...");

    // Bootstrap using default configured state directory.
    let state_dir = config::Config::default().state_dir;
    std::fs::create_dir_all(&state_dir)?;

    let conn = init_db(&state_dir)?;

    // First run bootstrap: initialize defaults and explicitly mark setup incomplete.
    if !db::has_settings(&conn)? {
        let default_cfg = config::Config::default();
        config::save_config_to_db(&conn, &default_cfg)?;
        db::set_wizard_status(&conn, WizardStatus::NotComplete)?;
    }

    // Load config from database
    let cfg = config::load_config_from_db(&conn)?;

    // Determine wizard visibility using explicit status + config health fallback.
    let wizard_status = db::get_wizard_status(&conn)?;
    let needs_wizard = match wizard_status {
        WizardStatus::NotComplete => true,
        WizardStatus::Skipped => false,
        WizardStatus::Complete => config_requires_wizard(&cfg),
    };

    // If config health check failed while marked complete, downgrade state until setup is rerun.
    if matches!(wizard_status, WizardStatus::Complete) && needs_wizard {
        db::set_wizard_status(&conn, WizardStatus::NotComplete)?;
    }

    // Apply persisted log level
    if let Some(log_level) = &Some(&cfg.log_level) {
        use tracing_subscriber::EnvFilter;
        let new_filter = EnvFilter::new(log_level);
        if let Err(e) = log_handle.reload(new_filter) {
            tracing::error!("Failed to apply persisted log level '{}': {}", log_level, e);
        } else {
            tracing::info!("Applied persisted log level: {}", log_level);
        }
    }

    // Ensure directories exist
    std::fs::create_dir_all(&cfg.quarantine_dir)?;

    let conn = Arc::new(StdMutex::new(conn));
    let cfg = Arc::new(AsyncMutex::new(cfg));

    let progress = Arc::new(AsyncMutex::new(HashMap::new()));
    let cancellation_tokens = Arc::new(AsyncMutex::new(HashMap::<i64, Arc<AtomicBool>>::new()));

    // Create shared event channel for coordinator -> TUI communication
    let (app_tx, app_rx) = mpsc::channel::<AppEvent>();

    let coordinator_conn = conn.clone();
    let coordinator_cfg = cfg.clone();
    let coordinator_progress = progress.clone();
    let coordinator_tokens = cancellation_tokens.clone();
    let coordinator_tx = app_tx.clone();

    tokio::spawn(async move {
        let coordinator = match Coordinator::new(
            coordinator_conn,
            coordinator_cfg,
            coordinator_progress,
            coordinator_tokens,
            coordinator_tx,
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Failed to create coordinator: {}", e);
                return;
            }
        };
        coordinator.run().await;
    });

    // Run TUI, passing wizard flag
    tui::run_tui(tui::TuiArgs {
        conn_mutex: conn,
        cfg,
        progress,
        cancellation_tokens,
        needs_wizard,
        log_handle,
        app_tx,
        app_rx,
    })
    .await?;
    Ok(())
}
