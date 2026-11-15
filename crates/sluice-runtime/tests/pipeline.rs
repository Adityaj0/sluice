use std::collections::BTreeMap;
use std::time::Duration;

use sluice_core::{Event, StreamElement};
use sluice_runtime::window::{Count, Sum, TimeWindow, TumblingWindows};
use sluice_runtime::{JobStatus, StreamEnv, WatermarkStrategy};
use tokio::sync::mpsc;
use tokio::time::timeout;

const FIVE_MINUTES: u64 = 5 * 60 * 1_000;

#[tokio::test]
async fn source_map_filter_sink() {
    let env = StreamEnv::new();
    let output = env
        .from_iter("numbers", (1..=100u64).map(|i| Event::new(i, i)), WatermarkStrategy::monotonous())
        .map(|x| x * 2)
        .filter(|x| *x > 50)
        .collect();

    let job = env.execute();
    assert_eq!(job.wait().await, JobStatus::Finished);

    let values: Vec<u64> = output.values();
    assert_eq!(values, (26..=100).map(|i| i * 2).collect::<Vec<_>>());

    let metrics = job.metrics();
    let names: Vec<_> = metrics.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["numbers", "map", "filter", "collect"]);
    assert_eq!(metrics[1].records_in, 100);
    assert_eq!(metrics[2].records_out, 75);
    assert_eq!(metrics[3].records_in, 75);
}

#[tokio::test]
async fn tumbling_window_counts_1000_events_into_10_windows() {
    let env = StreamEnv::new();
    let output = env
        .from_iter(
            "events",
            (0..1_000u64).map(|i| Event::new(i, i * 10)), // event times 0..9_990
            WatermarkStrategy::monotonous().with_emit_every(50),
        )
        .key_by(|_: &u64| "all")
        .window(TumblingWindows::of_millis(1_000), Count)
        .collect();

    assert_eq!(env.execute().wait().await, JobStatus::Finished);

    let results = output.values();
    assert_eq!(results.len(), 10);
    for (i, result) in results.iter().enumerate() {
        let start = i as u64 * 1_000;
        assert_eq!(result.window, TimeWindow { start, end: start + 1_000 });
        assert_eq!(result.value, 100);
    }
}

#[derive(Debug, Clone)]
struct Purchase {
    user_id: String,
    amount: f64,
}

/// Deterministic pseudo-random purchases: 20 users over two hours, with
/// event times up to 30s out of order.
fn purchases(n: u64) -> Vec<Event<Purchase>> {
    let mut seed = 42u64;
    let mut next = move || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        seed >> 33
    };
    (0..n)
        .map(|i| {
            let base = 1_700_000_000_000 + i * 1_000;
            let jitter = next() % 30_000;
            let purchase = Purchase {
                user_id: format!("user-{}", next() % 20),
                amount: (next() % 10_000) as f64 / 100.0,
            };
            Event::new(purchase, base.saturating_sub(jitter))
        })
        .collect()
}

#[tokio::test]
async fn keyed_tumbling_window_end_to_end() {
    let input = purchases(7_200);

    let mut expected: BTreeMap<(u64, String), (u64, f64)> = BTreeMap::new();
    for e in &input {
        let start = e.event_time - e.event_time % FIVE_MINUTES;
        let entry = expected.entry((start, e.value.user_id.clone())).or_default();
        entry.0 += 1;
        entry.1 += e.value.amount;
    }

    let env = StreamEnv::new();
    let counts = env
        .from_iter(
            "purchases",
            input.clone(),
            WatermarkStrategy::bounded_out_of_orderness(30_000).with_emit_every(1),
        )
        .key_by(|p: &Purchase| p.user_id.clone())
        .window(TumblingWindows::of(Duration::from_millis(FIVE_MINUTES)), Count)
        .collect();

    let env2 = StreamEnv::new();
    let sums = env2
        .from_iter("purchases", input, WatermarkStrategy::bounded_out_of_orderness(30_000))
        .key_by(|p: &Purchase| p.user_id.clone())
        .window(TumblingWindows::of_millis(FIVE_MINUTES), Sum(|p: &Purchase| p.amount))
        .collect();

    let job = env.execute();
    let job2 = env2.execute();
    assert_eq!(job.wait().await, JobStatus::Finished);
    assert_eq!(job2.wait().await, JobStatus::Finished);

    let window_stage = &job.metrics()[2];
    assert_eq!(window_stage.late_records, 0, "all disorder is within the bound");

    let got_counts: BTreeMap<_, _> = counts
        .values()
        .into_iter()
        .map(|r| ((r.window.start, r.key_str().into_owned()), r.value))
        .collect();
    let want_counts: BTreeMap<_, _> = expected.iter().map(|(k, (c, _))| (k.clone(), *c)).collect();
    assert_eq!(got_counts, want_counts);

    let got_sums = sums.values();
    assert_eq!(got_sums.len(), expected.len());
    for r in got_sums {
        let (_, want) = expected[&(r.window.start, r.key_str().into_owned())];
        assert!((r.value - want).abs() < 1e-6, "{r:?} != {want}");
    }

    // Results come out in window order: a window never fires before an
    // earlier one.
    let starts: Vec<_> = counts.values().iter().map(|r| r.window.start).collect();
    assert!(starts.is_sorted());
}

#[tokio::test]
async fn late_events_are_dropped_and_counted() {
    use StreamElement::{Record, Watermark};

    let env = StreamEnv::new();
    let output = env
        .from_elements(
            "scripted",
            vec![
                Record(Event::new(1u32, 1_000)),
                Record(Event::new(1, 2_000)),
                Watermark(4_999), // closes [0, 5000)
                Record(Event::new(1, 3_000)), // late
                Record(Event::new(1, 6_000)),
            ],
        )
        .key_by(|_: &u32| "k")
        .window(TumblingWindows::of_millis(5_000), Count)
        .collect();

    let job = env.execute();
    assert_eq!(job.wait().await, JobStatus::Finished);

    let results: Vec<_> = output.values().into_iter().map(|r| (r.window.start, r.value)).collect();
    assert_eq!(results, vec![(0, 2), (5_000, 1)]);
    assert_eq!(job.metrics()[2].late_records, 1);
}

#[tokio::test]
async fn watermarks_propagate_to_the_sink() {
    let env = StreamEnv::new();
    let _ = env
        .from_iter("src", (0..10u64).map(|i| Event::new(i, i * 100)), WatermarkStrategy::monotonous().with_emit_every(1))
        .map(|x| x + 1)
        .collect();
    let job = env.execute();
    job.wait().await;
    let metrics = job.metrics();
    assert_eq!(metrics[0].watermark, Some(899), "source: max seen 900 - 1");
    assert_eq!(metrics[1].watermark, Some(u64::MAX), "end of input flushes to MAX");
    assert_eq!(metrics[2].watermark, Some(u64::MAX));
}

#[tokio::test]
async fn channel_source_flushes_windows_when_senders_drop() {
    let (tx, rx) = mpsc::channel(16);
    let env = StreamEnv::new();
    let output = env
        .from_channel("ingest", rx, WatermarkStrategy::monotonous())
        .key_by(|v: &String| v.clone())
        .window(TumblingWindows::of_millis(1_000), Count)
        .collect();
    let job = env.execute();

    for (v, t) in [("a", 10), ("b", 20), ("a", 30), ("a", 1_500)] {
        tx.send(Event::new(v.to_string(), t)).await.unwrap();
    }
    drop(tx);

    assert_eq!(timeout(Duration::from_secs(5), job.wait()).await.unwrap(), JobStatus::Finished);
    let results: Vec<_> = output
        .values()
        .into_iter()
        .map(|r| (r.window.start, r.key_str().into_owned(), r.value))
        .collect();
    assert_eq!(
        results,
        vec![(0, "a".into(), 2), (0, "b".into(), 1), (1_000, "a".into(), 1)]
    );
}

#[tokio::test]
async fn channel_source_advances_watermark_on_timer() {
    let (tx, rx) = mpsc::channel(16);
    let env = StreamEnv::new();
    let output = env
        .from_channel(
            "ingest",
            rx,
            WatermarkStrategy::monotonous().with_interval(Duration::from_millis(20)),
        )
        .key_by(|_: &u8| "k")
        .window(TumblingWindows::of_millis(1_000), Count)
        .collect();
    let job = env.execute();

    tx.send(Event::new(0, 100)).await.unwrap();
    tx.send(Event::new(0, 2_000)).await.unwrap(); // well under emit_every

    // The window [0, 1000) fires without the stream ending.
    timeout(Duration::from_secs(5), async {
        while output.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("window should fire from the periodic watermark");
    assert_eq!(output.values()[0].value, 1);
    assert_eq!(job.status(), JobStatus::Running);
    drop(tx);
    job.wait().await;
}

#[tokio::test]
async fn panicking_operator_fails_the_job() {
    let env = StreamEnv::new();
    let _ = env
        .from_iter("src", (0..100u64).map(|i| Event::new(i, i)), WatermarkStrategy::monotonous())
        .map(|x| {
            assert!(x != 42, "boom at 42");
            x
        })
        .collect();

    match timeout(Duration::from_secs(5), env.execute().wait()).await.unwrap() {
        JobStatus::Failed { error } => {
            assert!(error.contains("map") && error.contains("boom at 42"), "{error}")
        }
        other => panic!("expected failure, got {other:?}"),
    }
}

#[tokio::test]
async fn cancel_stops_a_running_job() {
    let (tx, rx) = mpsc::channel::<Event<u8>>(16);
    let env = StreamEnv::new();
    let _ = env.from_channel("ingest", rx, WatermarkStrategy::none()).collect();
    let job = env.execute();

    tx.send(Event::new(1, 1)).await.unwrap();
    job.cancel();
    assert_eq!(timeout(Duration::from_secs(5), job.wait()).await.unwrap(), JobStatus::Cancelled);
    assert!(tx.is_closed(), "the source dropped its receiver");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backpressure_bounds_in_flight_events() {
    // Capacity-1 channels and a sink that never gets drained: the source
    // must stall instead of buffering everything.
    let (tx, rx) = mpsc::channel::<Event<u64>>(1);
    let env = StreamEnv::with_channel_capacity(1);
    let _ = env
        .from_channel("ingest", rx, WatermarkStrategy::none())
        .map(|x| {
            std::thread::sleep(Duration::from_millis(5));
            x
        })
        .collect();
    let job = env.execute();

    let mut sent = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    while tokio::time::Instant::now() < deadline {
        if tx.try_send(Event::new(sent, sent)).is_ok() {
            sent += 1;
        }
        tokio::task::yield_now().await;
    }
    // ~20 events processed in 100ms plus a handful held in channels.
    assert!(sent < 60, "producer was not throttled: {sent} accepted");
    job.cancel();
    job.wait().await;
}
