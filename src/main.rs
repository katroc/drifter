mod app;
mod components;
mod core;
mod services;
mod ui;
mod db;
mod coordinator;
mod tui;
mod utils;

use crate::coordinator::Coordinator;
use crate::core::config;
use crate::db::init_db;
use anyhow::Result;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
use std::thread;
use std::collections::HashMap;

fn main() -> Result<()> {
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
    
    // Ensure directories exist
    std::fs::create_dir_all(&cfg.staging_dir)?;
    std::fs::create_dir_all(&cfg.quarantine_dir)?;

    let conn = Arc::new(Mutex::new(conn));
    let cfg = Arc::new(Mutex::new(cfg));

    let progress = Arc::new(Mutex::new(HashMap::new()));
    let cancellation_tokens = Arc::new(Mutex::new(HashMap::<i64, Arc<AtomicBool>>::new()));

    let coordinator_conn = conn.clone();
    let coordinator_cfg = cfg.clone();
    let coordinator_progress = progress.clone();
    let coordinator_tokens = cancellation_tokens.clone();
    
    thread::spawn(move || {
        let coordinator = match Coordinator::new(coordinator_conn, coordinator_cfg, coordinator_progress, coordinator_tokens) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to create coordinator: {}", e);
                return;
            }
        };
        coordinator.run();
    });

    // Run TUI, passing wizard flag
    tui::run_tui(conn, cfg, progress, cancellation_tokens, needs_wizard)?;
    Ok(())
}
