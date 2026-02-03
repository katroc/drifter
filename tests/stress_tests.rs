use anyhow::Result;
use drifter::coordinator::Coordinator;
use drifter::core::config::Config;
use drifter::db;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Mutex as AsyncMutex;

fn setup_stress_db() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        "
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL DEFAULT 'legacy',
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            source_path TEXT NOT NULL,
            staged_path TEXT,
            size_bytes INTEGER NOT NULL,
            scan_status TEXT,
            upload_status TEXT,
            s3_bucket TEXT,
            s3_key TEXT,
            s3_upload_id TEXT,
            checksum TEXT,
            remote_checksum TEXT,
            error TEXT,
            priority INTEGER DEFAULT 0,
            retry_count INTEGER DEFAULT 0,
            next_retry_at TEXT,
            scan_duration_ms INTEGER,
            upload_duration_ms INTEGER
        );
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );
        ",
    )?;
    Ok(conn)
}

#[tokio::test]
async fn stress_test_coordinator_cycle() -> Result<()> {
    let conn = Arc::new(Mutex::new(setup_stress_db()?));
    let config = Arc::new(AsyncMutex::new(Config::default()));
    let progress = Arc::new(AsyncMutex::new(HashMap::new()));
    let cancellation_tokens = Arc::new(AsyncMutex::new(HashMap::new()));

    let coordinator =
        Coordinator::new(conn.clone(), config.clone(), progress, cancellation_tokens)?;

    // Create 1000 jobs
    {
        let c = conn.lock().unwrap();
        for i in 0..1000 {
            let job_id = db::create_job(
                &c,
                "stress-session",
                &format!("/tmp/file_{}.txt", i),
                1024,
                None,
            )?;
            db::update_job_staged(&c, job_id, &format!("/tmp/staged_{}.txt", i), "queued")?;
        }
    }

    println!("Starting stress test: processing 1000 jobs through coordinator cycles...");
    let start = Instant::now();

    // Run cycles until all jobs are at least 'scanned' or 'uploading'
    // Note: Since we don't have a real uploader/scanner running, they will stay in 'scanning' or 'uploading'
    // depending on how process_cycle spawns tasks.

    for _ in 0..100 {
        // Run 100 cycles
        coordinator.process_cycle().await?;
    }

    let elapsed = start.elapsed();
    println!("Processed cycles in {:?}", elapsed);

    // Verify some jobs were picked up
    let (scanning_count, uploading_count) = {
        let c = conn.lock().unwrap();
        let scanning = db::count_jobs_with_status(&c, "scanning")?;
        let uploading = db::count_jobs_with_status(&c, "uploading")?;
        (scanning, uploading)
    };

    println!(
        "Jobs in scanning: {}, Jobs in uploading: {}",
        scanning_count, uploading_count
    );

    // We expect at least some jobs to be active
    assert!(scanning_count > 0 || uploading_count > 0);

    Ok(())
}
