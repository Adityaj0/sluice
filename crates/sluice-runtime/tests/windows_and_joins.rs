//! Phase 3: sliding and session windows, lateness and side outputs,
//! stream-stream joins, idleness, watermark alignment and backpressure.

use std::collections::BTreeMap;
use std::time::Duration;

use sluice_core::{Event, StreamElement};
use sluice_runtime::source::IterSource;
use sluice_runtime::window::{Count, SessionWindows, SlidingWindows, Sum, TumblingWindows};
use sluice_runtime::{
    CheckpointConfig, CollectSink, JobConfig, JobStatus, RestartStrategy, StateBackendKind, StreamEnv,
    WatermarkStrategy, run_job,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::timeout;

async fn finish(job: &sluice_runtime::JobHandle) -> JobStatus {
    timeout(Duration::from_secs(30), job.wait()).await.expect("job should finish")
}

/// Deterministic pseudo-random numbers.
fn lcg(seed: u64) -> impl FnMut() -> u64 {
    let mut state = seed;
    move || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        state >> 33
    }
}

#[tokio::test]
async fn sliding_windows_end_to_end() {
    let windows = SlidingWindows::of(Duration::from_secs(10), Duration::from_secs(2));
    let mut rand = lcg(7);
    // 5 users, 2000 events over ~100s, up to 3s out of order.
    let events: Vec<Event<u64>> = (0..2_000u64)
        .map(|i| Event::new(rand() % 5, (i * 50).saturating_sub(rand() % 3_000)))
        .collect();

    let mut expected: BTreeMap<(u64, String), u64> = BTreeMap::new();
    let mut scratch = Vec::new();
    for e in &events {
        scratch.clear();
        windows.assign(e.event_time, &mut scratch);
        for w in &scratch {
            *expected.entry((w.start, e.value.to_string())).or_default() += 1;
        }
    }

    let env = StreamEnv::new();
    let output = env
        .from_iter("events", events, WatermarkStrategy::bounded_out_of_orderness(3_000).with_emit_every(10))
        .key_by(|user: &u64| user.to_string())
        .window(windows, Count)
        .collect();
    let job = env.execute();
    assert_eq!(finish(&job).await, JobStatus::Finished);
    assert_eq!(job.metrics()[2].late_records, 0);

    let mut got = BTreeMap::new();
    for r in output.values() {
        assert_eq!(r.window.end - r.window.start, 10_000);
        assert!(got.insert((r.window.start, r.key_str().into_owned()), r.value).is_none());
    }
    assert_eq!(got, expected);
}

#[tokio::test]
async fn session_windows_end_to_end() {
    // Bursts per user separated by gaps longer than 5s, with one bridging
    // event that merges two sessions after the fact (out of order).
    let gap = 5_000;
    let raw = [
        ("ann", 1_000),
        ("ann", 3_000),
        ("bob", 2_000),
        ("ann", 20_000),
        ("ann", 22_000),
        ("bob", 9_000), // 2000 + 5000 < 9000: new session
        ("ann", 12_000), // 3000 + 5000 >= ... no: [12000,17000) touches neither
        ("ann", 16_000), // [16000,21000) bridges [12000,17000) and [20000,27000)
        ("bob", 30_000),
    ];
    let events: Vec<_> = raw.iter().map(|(u, t)| Event::new(u.to_string(), *t)).collect();

    let env = StreamEnv::new();
    let output = env
        .from_iter("clicks", events, WatermarkStrategy::bounded_out_of_orderness(10_000).with_emit_every(1))
        .key_by(|user: &String| user.clone())
        .window(SessionWindows::with_gap_millis(gap), Count)
        .collect();
    assert_eq!(finish(&env.execute()).await, JobStatus::Finished);

    let mut sessions: Vec<_> = output
        .values()
        .into_iter()
        .map(|r| (r.key_str().into_owned(), r.window.start, r.window.end, r.value))
        .collect();
    sessions.sort();
    assert_eq!(
        sessions,
        vec![
            ("ann".into(), 1_000, 8_000, 2),
            ("ann".into(), 12_000, 27_000, 4),
            ("bob".into(), 2_000, 7_000, 1),
            ("bob".into(), 9_000, 14_000, 1),
            ("bob".into(), 30_000, 35_000, 1),
        ]
    );
}

#[tokio::test]
async fn late_events_update_windows_or_go_to_the_side_output() {
    use StreamElement::{Record, Watermark};
    let event = |v: f64, t| Record(Event::new(v, t));

    let env = StreamEnv::new();
    let (results, late) = env
        .from_elements(
            "scripted",
            vec![
                event(1.0, 100),
                event(2.0, 900),
                Watermark(1_500), // fires [0, 1000)
                event(4.0, 300),  // late, within 1s lateness: updated result
                Watermark(2_100), // [0,1000) expires (999 + 1000 < 2100)
                event(8.0, 400),  // too late: side output
                event(16.0, 2_500),
            ],
        )
        .key_by(|_: &f64| "k")
        .windowed(TumblingWindows::of_millis(1_000))
        .allowed_lateness(Duration::from_secs(1))
        .aggregate_with_late_output(Sum(|v: &f64| *v));
    let results = results.collect();
    let late = late.collect();
    let job = env.execute();
    assert_eq!(finish(&job).await, JobStatus::Finished);

    let rows: Vec<_> = results.values().into_iter().map(|r| (r.window.start, r.value, r.late_firing)).collect();
    assert_eq!(rows, vec![(0, 3.0, false), (0, 7.0, true), (2_000, 16.0, false)]);
    let late: Vec<_> = late.snapshot().into_iter().map(|e| (e.value, e.event_time)).collect();
    assert_eq!(late, vec![(8.0, 400)]);
    let window = job.metrics().into_iter().find(|m| m.name == "window").unwrap();
    assert_eq!(window.late_records, 1);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Click {
    user: u64,
    page: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Purchase {
    user: u64,
    amount: u64,
}

fn clicks() -> impl Iterator<Item = Event<Click>> + Send {
    (0..3_000u64).map(|i| Event::new(Click { user: i % 7, page: i }, i * 10))
}

fn purchases() -> impl Iterator<Item = Event<Purchase>> + Send {
    (0..1_000u64).map(|i| Event::new(Purchase { user: i % 5, amount: i }, i * 30 + 5))
}

/// Naive window join: (window start, user) -> number of pairs.
fn expected_pairs() -> BTreeMap<(u64, u64), u64> {
    let window = |t: u64| t - t % 1_000;
    let mut clicks_by: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for e in clicks() {
        *clicks_by.entry((window(e.event_time), e.value.user)).or_default() += 1;
    }
    let mut pairs = BTreeMap::new();
    for e in purchases() {
        let key = (window(e.event_time), e.value.user);
        if let Some(n) = clicks_by.get(&key) {
            *pairs.entry(key).or_default() += n;
        }
    }
    pairs
}

fn pair_counts(output: &CollectSink<(u64, u64, u64)>) -> BTreeMap<(u64, u64), u64> {
    let mut got = BTreeMap::new();
    for e in output.snapshot() {
        let window_start = e.event_time + 1 - 1_000;
        *got.entry((window_start, e.value.0)).or_default() += 1;
    }
    got
}

#[tokio::test]
async fn window_join_matches_a_naive_join() {
    let env = StreamEnv::new();
    let left = env.from_iter("clicks", clicks(), WatermarkStrategy::monotonous().with_emit_every(10));
    let right = env.from_iter("purchases", purchases(), WatermarkStrategy::monotonous().with_emit_every(10));
    let output = left
        .join(right)
        .on(|c: &Click| c.user.to_string(), |p: &Purchase| p.user.to_string())
        .window(TumblingWindows::of_millis(1_000))
        .apply(|c: &Click, p: &Purchase| (c.user, c.page, p.amount))
        .collect();
    let job = env.execute();
    assert_eq!(finish(&job).await, JobStatus::Finished);
    assert_eq!(pair_counts(&output), expected_pairs());
    assert!(output.values().iter().all(|(user, page, amount)| page % 7 == *user && amount % 5 == *user));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_join_recovers_exactly_once() {
    for backend in [StateBackendKind::Memory, StateBackendKind::RocksDb] {
        let dir = tempfile::TempDir::new().unwrap();
        let output = CollectSink::new().exactly_once();
        let sink = output.clone();
        let config = JobConfig::new("join")
            .with_checkpointing(
                CheckpointConfig::new(dir.path())
                    .with_interval(Duration::from_millis(10))
                    .with_backend(backend),
            )
            .with_restart(RestartStrategy::fixed_delay(2, Duration::ZERO));
        let job = run_job(config, move |env| {
            let crash = env.attempt() == 0;
            let left = env.from_source(
                "clicks",
                IterSource::new(clicks()).with_rate(30_000.0),
                WatermarkStrategy::monotonous().with_emit_every(10),
            );
            let right = env.from_source(
                "purchases",
                IterSource::new(purchases()).with_rate(10_000.0),
                WatermarkStrategy::monotonous().with_emit_every(10),
            );
            left.join(right)
                .on(|c: &Click| c.user.to_string(), |p: &Purchase| p.user.to_string())
                .window(TumblingWindows::of_millis(1_000))
                .apply(move |c: &Click, p: &Purchase| {
                    assert!(!(crash && c.page == 1_500), "injected failure");
                    (c.user, c.page, p.amount)
                })
                .sink("output", sink.clone());
        })
        .unwrap();

        assert_eq!(finish(&job).await, JobStatus::Finished, "{backend:?}: {:?}", job.failures());
        assert_eq!(job.restarts(), 1);
        assert!(job.checkpoints().restored_from.is_some(), "{backend:?}");
        assert_eq!(pair_counts(&output), expected_pairs(), "{backend:?}");
    }
}

#[tokio::test]
async fn idle_source_does_not_stall_the_join() {
    // The purchases side sends one event and then goes quiet but stays open.
    let (purchases_tx, purchases_rx) = mpsc::channel(16);
    purchases_tx.send(Event::new(Purchase { user: 1, amount: 9 }, 50)).await.unwrap();

    let env = StreamEnv::new();
    let left = env.from_channel(
        "clicks",
        {
            let (tx, rx) = mpsc::channel(64);
            for i in 0..30u64 {
                tx.send(Event::new(Click { user: 1, page: i }, i * 100)).await.unwrap();
            }
            std::mem::forget(tx); // keep the clicks stream open too
            rx
        },
        WatermarkStrategy::monotonous()
            .with_emit_every(1)
            .with_idleness(Duration::from_millis(50)),
    );
    let right = env.from_channel(
        "purchases",
        purchases_rx,
        WatermarkStrategy::monotonous().with_idleness(Duration::from_millis(50)),
    );
    let output = left
        .join(right)
        .on(|c: &Click| c.user.to_string(), |p: &Purchase| p.user.to_string())
        .window(TumblingWindows::of_millis(1_000))
        .apply(|c: &Click, p: &Purchase| (c.page, p.amount))
        .collect();
    let job = env.execute();

    // Window [0, 1000) can only fire because the idle purchases source no
    // longer holds back the join's watermark (clicks reach 2900).
    timeout(Duration::from_secs(5), async {
        while output.len() < 10 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("join should fire despite the idle input");
    assert_eq!(output.values(), (0..10).map(|page| (page, 9)).collect::<Vec<_>>());
    assert!(job.metrics().iter().any(|m| m.name == "purchases" && m.idle));
    job.cancel();
    drop(purchases_tx);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watermark_alignment_bounds_drift_between_sources() {
    // "fast" can read at 100x the speed of "slow"; alignment keeps its
    // watermark within 2s of slow's.
    let env = StreamEnv::new();
    let strategy = |name: &str| {
        let _ = name;
        WatermarkStrategy::monotonous()
            .with_emit_every(1)
            .with_alignment("purchases-and-clicks", Duration::from_secs(2))
    };
    let fast = env.from_source(
        "fast",
        IterSource::new((0..5_000u64).map(|i| Event::new(i, i * 10))).with_rate(200_000.0),
        strategy("fast"),
    );
    let slow = env.from_source(
        "slow",
        IterSource::new((0..500u64).map(|i| Event::new(i, i * 100))).with_rate(2_000.0),
        strategy("slow"),
    );
    let _ = fast.union([slow]).collect();
    let job = env.execute();

    let mut max_drift = 0i64;
    while !job.status().is_terminal() {
        let metrics = job.metrics();
        if let (Some(f), Some(s)) = (
            metrics.iter().find(|m| m.name == "fast").and_then(|m| m.watermark),
            metrics.iter().find(|m| m.name == "slow").and_then(|m| m.watermark),
        ) {
            if f < 49_000 && s < 49_000 {
                max_drift = max_drift.max(f as i64 - s as i64);
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(job.wait().await, JobStatus::Finished);
    // One emitted watermark of slack per check (events are 100ms apart on
    // the slow side).
    assert!(max_drift <= 2_000 + 200, "fast ran {max_drift}ms ahead of slow");
    assert!(max_drift > 1_000, "alignment let fast lead up to the bound ({max_drift}ms)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_sink_shows_up_as_upstream_backpressure() {
    let env = StreamEnv::with_channel_capacity(64);
    env.from_iter("events", (0..400u64).map(|i| Event::new(i, i)), WatermarkStrategy::none())
        .map(|x| x)
        .sink(
            "slow-sink",
            sluice_runtime::FnSink::new(|_: Event<u64>| std::thread::sleep(Duration::from_millis(2))),
        );
    let job = env.execute();

    tokio::time::sleep(Duration::from_millis(600)).await;
    let metrics = job.metrics();
    let map = metrics.iter().find(|m| m.name == "map").unwrap();
    let sink = metrics.iter().find(|m| m.name == "slow-sink").unwrap();
    assert!(map.backpressure_ratio > 0.5, "map is mostly waiting on the sink: {map:?}");
    assert!(map.backpressured_ms > 300, "{map:?}");
    // The buffer is in use; how full it looks depends on scheduling, since
    // this sink blocks its worker thread (see README, known limits).
    assert!(sink.input_queue > 0, "the sink has a backlog: {sink:?}");
    assert_eq!(sink.input_capacity, 64);
    job.cancel();
}
