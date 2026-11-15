//! Recovery cost: rebuilding an operator's keyed state from a checkpoint
//! (reading the snapshot back, or linking RocksDB files and opening the
//! database) against state size.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sluice_state::{CheckpointStorage, StateBackendKind, create_backend};
use tempfile::TempDir;

static IDS: AtomicU64 = AtomicU64::new(1);

fn recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    for kind in [StateBackendKind::Memory, StateBackendKind::RocksDb] {
        for entries in [10_000u64, 100_000, 1_000_000] {
            let dir = TempDir::new().unwrap();
            let storage = CheckpointStorage::new(dir.path()).unwrap();
            let work = storage.work_dir().join("source");
            let mut backend = create_backend(kind, "01-window", &["panes"], &work, Some(&storage), None).unwrap();
            for i in 0..entries {
                let mut key = [0u8; 24];
                key[16..].copy_from_slice(&i.to_be_bytes());
                backend.put("panes", &key, &(i as f64).to_le_bytes()).unwrap();
            }
            let handle = backend.snapshot(1).unwrap().upload(&storage).unwrap().handle;
            drop(backend);

            group.throughput(Throughput::Elements(entries));
            group.bench_with_input(BenchmarkId::new(format!("{kind:?} restore"), entries), &entries, |b, _| {
                b.iter(|| {
                    let work = storage.work_dir().join(format!("r{}", IDS.fetch_add(1, Ordering::Relaxed)));
                    let restored = create_backend(kind, "01-window", &["panes"], &work, Some(&storage), Some(&handle)).unwrap();
                    // Prove the state is usable: read the last key.
                    let mut last = [0u8; 24];
                    last[16..].copy_from_slice(&(entries - 1).to_be_bytes());
                    assert!(restored.get("panes", &last).unwrap().is_some());
                    drop(restored);
                    let _ = std::fs::remove_dir_all(work);
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, recovery);
criterion_main!(benches);
