use criterion::{criterion_group, criterion_main, Criterion};
use drifter::db;
use rusqlite::Connection;
use tokio::runtime::Runtime;

fn bench_db_operations(c: &mut Criterion) {
    let _rt = Runtime::new().unwrap();
    
    // Setup in-memory DB
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
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
        "
    ).unwrap();

    c.bench_function("db_create_job", |b| {
        b.iter(|| {
            db::create_job(
                &conn,
                "bench-session",
                "/tmp/bench-file.txt",
                1024,
                Some("bench-key.txt"),
            ).unwrap()
        })
    });

    let job_id = db::create_job(&conn, "bench-session", "/tmp/bench-file.txt", 1024, None).unwrap();

    c.bench_function("db_get_job", |b| {
        b.iter(|| db::get_job(&conn, job_id).unwrap())
    });

    c.bench_function("db_insert_event", |b| {
        b.iter(|| db::insert_event(&conn, job_id, "bench", "bench message").unwrap())
    });

    c.bench_function("db_list_active_jobs", |b| {
        b.iter(|| db::list_active_jobs(&conn, 100).unwrap())
    });
}

criterion_group!(benches, bench_db_operations);
criterion_main!(benches);
