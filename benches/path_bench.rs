use criterion::{Criterion, black_box, criterion_group, criterion_main};
use drifter::services::ingest;
use std::path::Path;

fn bench_path_calculations(c: &mut Criterion) {
    let file_path = Path::new("/home/user/documents/projects/drifter/src/services/ingest.rs");
    let base_path = Path::new("/home/user/documents");

    c.bench_function("calculate_base_path", |b| {
        b.iter(|| ingest::calculate_base_path(black_box(file_path)))
    });

    c.bench_function("calculate_relative_path", |b| {
        b.iter(|| ingest::calculate_relative_path(black_box(file_path), black_box(base_path)))
    });
}

criterion_group!(benches, bench_path_calculations);
criterion_main!(benches);
