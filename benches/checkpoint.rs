//! Checkpoint cost against state size: the synchronous phase (operator
//! paused at the barrier), the full snapshot, and incremental RocksDB
//! checkpoints after a small fraction of keys change.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sluice_state::{CheckpointStorage, StateBackend, StateBackendKind, create_backend};
use tempfile::TempDir;

static IDS: AtomicU64 = AtomicU64::new(1);

fn key(i: u64) -> [u8; 24] {
    // Like a window pane key: end ++ start ++ 8-byte user key.
    let mut k = [0u8; 24];
    k[..8].copy_from_slice(&(60_000 * (1 + i / 1_000_000)).to_be_bytes());
    k[16..].copy_from_slice(&i.to_be_bytes());
    k
}

fn filled(kind: StateBackendKind, storage: &CheckpointStorage, entries: u64) -> (Box<dyn StateBackend>, PathBuf) {
    let work = storage.work_dir().join(format!("w{}", IDS.fetch_add(1, Ordering::Relaxed)));
    let mut backend = create_backend(kind, "01-window", &["panes"], &work, Some(storage), None).unwrap();
    for i in 0..entries {
        backend.put("panes", &key(i), &(i as f64).to_le_bytes()).unwrap();
    }
    (backend, work)
}

/// Deletes what one measured checkpoint left on disk (not timed), so disk
/// use stays flat however many iterations criterion runs.
fn clean(storage: &CheckpointStorage, work: &Path, id: u64) {
    let _ = std::fs::remove_dir_all(storage.checkpoint_dir(id));
    let _ = std::fs::remove_dir_all(work.join("01-window").join(format!("snapshot-{id}")));
}

fn checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    for kind in [StateBackendKind::Memory, StateBackendKind::RocksDb] {
        for entries in [10_000u64, 100_000, 1_000_000] {
            let dir = TempDir::new().unwrap();
            let storage = CheckpointStorage::new(dir.path()).unwrap();
            let (mut backend, work) = filled(kind, &storage, entries);
            group.throughput(Throughput::Elements(entries));

            group.bench_with_input(BenchmarkId::new(format!("{kind:?} sync phase"), entries), &entries, |b, _| {
                b.iter_custom(|iters| {
                    let mut timed = Duration::ZERO;
                    for _ in 0..iters {
                        let id = IDS.fetch_add(1, Ordering::Relaxed);
                        let started = Instant::now();
                        let pending = backend.snapshot(id).unwrap();
                        timed += started.elapsed();
                        drop(pending);
                        clean(&storage, &work, id);
                    }
                    timed
                })
            });

            group.bench_with_input(BenchmarkId::new(format!("{kind:?} checkpoint, 1 key changed"), entries), &entries, |b, _| {
                b.iter_custom(|iters| {
                    let mut timed = Duration::ZERO;
                    for _ in 0..iters {
                        // A fresh change so RocksDB has something new to flush.
                        backend.put("panes", &key(0), &[1; 8]).unwrap();
                        let id = IDS.fetch_add(1, Ordering::Relaxed);
                        let started = Instant::now();
                        backend.snapshot(id).unwrap().upload(&storage).unwrap();
                        timed += started.elapsed();
                        clean(&storage, &work, id);
                    }
                    timed
                })
            });

            if kind == StateBackendKind::RocksDb {
                let changed = entries / 100;
                group.bench_with_input(BenchmarkId::new("RocksDb incremental, 1% keys changed", entries), &entries, |b, _| {
                    let mut round = 0u64;
                    b.iter_custom(|iters| {
                        let mut timed = Duration::ZERO;
                        for _ in 0..iters {
                            round += 1;
                            let started = Instant::now();
                            for i in 0..changed {
                                backend.put("panes", &key((i * 97 + round) % entries), &round.to_le_bytes()).unwrap();
                            }
                            let id = IDS.fetch_add(1, Ordering::Relaxed);
                            let result = backend.snapshot(id).unwrap().upload(&storage).unwrap();
                            timed += started.elapsed();
                            // Usually far below the state size; a checkpoint right
                            // after a compaction re-uploads the merged files.
                            std::hint::black_box(result.uploaded_bytes);
                            clean(&storage, &work, id);
                        }
                        timed
                    })
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, checkpoint);
criterion_main!(benches);
