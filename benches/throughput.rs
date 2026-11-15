//! End-to-end pipeline throughput: events/second from source to sink on
//! one node (every stage is a task; channels between them).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use sluice_core::Event;
use sluice_runtime::window::{Count, SlidingWindows, Sum, TumblingWindows};
use sluice_runtime::{
    CheckpointConfig, FnSink, JobConfig, JobStatus, StateBackendKind, StreamEnv, WatermarkStrategy, run_job,
};

const EVENTS: u64 = 200_000;
const KEYS: u64 = 100;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
}

fn counting_sink<T>(counter: &Arc<AtomicU64>) -> FnSink<impl FnMut(Event<T>) + Send + 'static> {
    let counter = Arc::clone(counter);
    FnSink::new(move |_| {
        counter.fetch_add(1, Ordering::Relaxed);
    })
}

/// Event `i` at `i` ms, key `i % 100`: 10-second windows hold 10k events.
fn events() -> impl Iterator<Item = Event<u64>> + Send {
    (0..EVENTS).map(|i| Event::new(i, i))
}

fn throughput(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(EVENTS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));

    group.bench_function("stateless map+filter", |b| {
        b.iter(|| {
            rt.block_on(async {
                let out = Arc::new(AtomicU64::new(0));
                let env = StreamEnv::new();
                env.from_iter("src", events(), WatermarkStrategy::monotonous())
                    .map(|x| x * 2)
                    .filter(|x| x % 3 != 0)
                    .sink("count", counting_sink(&out));
                assert_eq!(env.execute().wait().await, JobStatus::Finished);
            })
        })
    });

    group.bench_function("keyed tumbling window, memory state", |b| {
        b.iter(|| {
            rt.block_on(async {
                let out = Arc::new(AtomicU64::new(0));
                let env = StreamEnv::new();
                env.from_iter("src", events(), WatermarkStrategy::monotonous())
                    .key_by(|x: &u64| (x % KEYS).to_string())
                    .window(TumblingWindows::of_millis(10_000), Sum(|x: &u64| *x as f64))
                    .sink("count", counting_sink(&out));
                assert_eq!(env.execute().wait().await, JobStatus::Finished);
                assert_eq!(out.load(Ordering::Relaxed), EVENTS / 10_000 * KEYS);
            })
        })
    });

    group.bench_function("sliding window 10s/1s, memory state", |b| {
        b.iter(|| {
            rt.block_on(async {
                let out = Arc::new(AtomicU64::new(0));
                let env = StreamEnv::new();
                env.from_iter("src", events(), WatermarkStrategy::monotonous())
                    .key_by(|x: &u64| (x % KEYS).to_string())
                    .window(SlidingWindows::of_millis(10_000, 1_000), Count)
                    .sink("count", counting_sink(&out));
                assert_eq!(env.execute().wait().await, JobStatus::Finished);
            })
        })
    });

    group.bench_function("keyed tumbling window, RocksDB + checkpoint every 1s", |b| {
        b.iter(|| {
            rt.block_on(async {
                let dir = tempfile::TempDir::new().unwrap();
                let out = Arc::new(AtomicU64::new(0));
                let sink_counter = Arc::clone(&out);
                let config = JobConfig::new("bench").with_checkpointing(
                    CheckpointConfig::new(dir.path())
                        .with_interval(Duration::from_secs(1))
                        .with_backend(StateBackendKind::RocksDb),
                );
                let job = run_job(config, move |env| {
                    env.from_iter("src", events(), WatermarkStrategy::monotonous())
                        .key_by(|x: &u64| (x % KEYS).to_string())
                        .window(TumblingWindows::of_millis(10_000), Sum(|x: &u64| *x as f64))
                        .sink("count", counting_sink(&sink_counter));
                })
                .unwrap();
                assert_eq!(job.wait().await, JobStatus::Finished);
                assert_eq!(out.load(Ordering::Relaxed), EVENTS / 10_000 * KEYS);
            })
        })
    });

    let sql = sluice_sql::compile(
        "SELECT user_id, COUNT(*) AS n, SUM(amount) AS total FROM purchases
         WHERE amount > 0 WINDOW TUMBLING (SIZE 10 SECONDS) GROUP BY user_id HAVING COUNT(*) > 1",
    )
    .unwrap();
    let json: Vec<Event<Vec<u8>>> = (0..EVENTS)
        .map(|i| {
            let record = format!(r#"{{"user_id":"user-{:03}","amount":{}.25,"item":"book"}}"#, i % KEYS, i % 500);
            Event::new(record.into_bytes(), i)
        })
        .collect();
    group.bench_function("SQL: JSON decode + WHERE + window + HAVING", |b| {
        b.iter(|| {
            rt.block_on(async {
                let out = Arc::new(AtomicU64::new(0));
                let env = StreamEnv::new();
                let input = env.from_iter("src", json.clone(), WatermarkStrategy::monotonous());
                sql.physical.build(input).sink("count", counting_sink(&out));
                assert_eq!(env.execute().wait().await, JobStatus::Finished);
            })
        })
    });
    group.finish();
}

criterion_group!(benches, throughput);
criterion_main!(benches);
