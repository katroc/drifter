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

use crate::coordinator::Coordinator;
use crate::core::config;
use crate::db::init_db;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let (_guard, log_handle) = logging::init();

    tracing::info!("Starting Drifter application...");

    // Bootstrap: always use ./state for DB location
    let state_dir = "./state";
    std::fs::create_dir_all(state_dir)?;

    let conn = init_db(state_dir)?;

    // Check if first run (no settings in DB)
    let needs_wizard = !db::has_settings(&conn)?;

    // If no settings, initialize with defaults
    if needs_wizard {
        let default_cfg = config::Config::default();
        config::save_config_to_db(&conn, &default_cfg)?;
    }

    // Load config from database
    let cfg = config::load_config_from_db(&conn)?;

    // Apply persisted log level
    if let Some(log_level) = &Some(&cfg.log_level) {
        use tracing_subscriber::EnvFilter;
        let new_filter = EnvFilter::new(log_level);
        if let Err(e) = log_handle.reload(new_filter) {
            eprintln!("Failed to apply persisted log level '{}': {}", log_level, e);
        } else {
            tracing::info!("Applied persisted log level: {}", log_level);
        }
    }

    // Ensure directories exist
    std::fs::create_dir_all(&cfg.staging_dir)?;
    std::fs::create_dir_all(&cfg.quarantine_dir)?;

    let conn = Arc::new(StdMutex::new(conn));
    let cfg = Arc::new(AsyncMutex::new(cfg));

    let progress = Arc::new(AsyncMutex::new(HashMap::new()));
    let cancellation_tokens = Arc::new(AsyncMutex::new(HashMap::<i64, Arc<AtomicBool>>::new()));

    let coordinator_conn = conn.clone();
    let coordinator_cfg = cfg.clone();
    let coordinator_progress = progress.clone();
    let coordinator_tokens = cancellation_tokens.clone();

    tokio::spawn(async move {
        let coordinator = match Coordinator::new(
            coordinator_conn,
            coordinator_cfg,
            coordinator_progress,
            coordinator_tokens,
        ) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to create coordinator: {}", e);
                return;
            }
        };
        coordinator.run().await;
    });

    // Run TUI, passing wizard flag
    tui::run_tui(
        conn,
        cfg,
        progress,
        cancellation_tokens,
        needs_wizard,
        log_handle,
    )
    .await?;
    Ok(())
}
