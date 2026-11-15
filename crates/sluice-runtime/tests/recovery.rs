//! Fault-tolerance tests: crash a pipeline mid-stream and check the output
//! is exactly what a failure-free run produces (no loss, no duplicates).

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use sluice_core::Event;
use sluice_runtime::source::IterSource;
use sluice_runtime::window::{Count, TumblingWindows, WindowResult};
use sluice_runtime::{
    CheckpointConfig, CheckpointStatus, CollectSink, JobConfig, JobHandle, JobStatus, RestartStrategy,
    StateBackendKind, StreamEnv, WatermarkStrategy, run_job,
};
use tempfile::TempDir;
use tokio::time::timeout;

const EVENTS: u64 = 6_000;
const KEYS: u64 = 10;
const WINDOW_MS: u64 = 1_000;

/// Event `i` happens at `i * 10` ms with key `i % 10`: 60 one-second windows,
/// each holding exactly 10 events per key.
fn events() -> impl Iterator<Item = Event<u64>> + Send {
    (0..EVENTS).map(|i| Event::new(i, i * 10))
}

fn expected_counts() -> BTreeMap<(u64, String), u64> {
    let mut expected = BTreeMap::new();
    for i in 0..EVENTS {
        let window = (i * 10) / WINDOW_MS * WINDOW_MS;
        *expected.entry((window, (i % KEYS).to_string())).or_default() += 1;
    }
    expected
}

/// Asserts every (window, key) result appears exactly once with the right
/// count.
fn assert_exactly_once(output: &CollectSink<WindowResult<u64>>) {
    let results = output.values();
    let mut got = BTreeMap::new();
    for r in &results {
        let previous = got.insert((r.window.start, r.key_str().into_owned()), r.value);
        assert!(previous.is_none(), "duplicate result for {:?}", r);
    }
    assert_eq!(got, expected_counts());
    assert_eq!(results.len(), expected_counts().len());
}

fn config(name: &str, dir: &Path, backend: StateBackendKind) -> JobConfig {
    JobConfig::new(name)
        .with_checkpointing(
            CheckpointConfig::new(dir)
                .with_interval(Duration::from_millis(10))
                .with_timeout(Duration::from_secs(5))
                .with_backend(backend)
                .with_retained(2),
        )
        .with_restart(RestartStrategy::fixed_delay(3, Duration::ZERO))
}

/// Source → map → key_by → window(count) → exactly-once sink, with an
/// optional crash in the map on the first attempt.
fn counting_pipeline(env: &StreamEnv, sink: &CollectSink<WindowResult<u64>>, rate: f64, crash_at: Option<u64>) {
    let crash_at = crash_at.filter(|_| env.attempt() == 0);
    env.from_source("events", IterSource::new(events()).with_rate(rate), WatermarkStrategy::monotonous().with_emit_every(50))
        .map(move |x: u64| {
            assert!(Some(x) != crash_at, "injected failure at record {x}");
            x
        })
        .key_by(|x: &u64| (x % KEYS).to_string())
        .window(TumblingWindows::of_millis(WINDOW_MS), Count)
        .sink("output", sink.clone());
}

async fn finish(job: &JobHandle) -> JobStatus {
    timeout(Duration::from_secs(30), job.wait()).await.expect("job should finish")
}

async fn wait_for_checkpoint(job: &JobHandle) {
    timeout(Duration::from_secs(10), async {
        while job.checkpoints().completed == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a checkpoint should complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_exactly_once_after_operator_crash() {
    for backend in [StateBackendKind::Memory, StateBackendKind::RocksDb] {
        let dir = TempDir::new().unwrap();
        let output = CollectSink::new().exactly_once();
        let sink = output.clone();
        let job = run_job(config("crash", dir.path(), backend), move |env| {
            counting_pipeline(env, &sink, 40_000.0, Some(4_000))
        })
        .unwrap();

        assert_eq!(finish(&job).await, JobStatus::Finished, "{backend:?}: {:?}", job.failures());
        assert_eq!(job.restarts(), 1);
        assert!(job.failures()[0].contains("injected failure at record 4000"), "{:?}", job.failures());
        let history = job.checkpoints();
        let restored = history.restored_from.expect("restored from a checkpoint, not from scratch");
        assert!(history.completed >= 2, "{history:?}");
        assert!(history.record(restored).is_some_and(|r| r.status == CheckpointStatus::Completed));
        assert_exactly_once(&output);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_after_a_stage_is_killed() {
    let dir = TempDir::new().unwrap();
    let output = CollectSink::new().exactly_once();
    let sink = output.clone();
    let job = run_job(config("kill", dir.path(), StateBackendKind::RocksDb), move |env| {
        counting_pipeline(env, &sink, 20_000.0, None)
    })
    .unwrap();

    wait_for_checkpoint(&job).await;
    let window = job.stages().iter().position(|s| s.uid.ends_with("window")).unwrap();
    assert!(job.kill_stage(window));

    assert_eq!(finish(&job).await, JobStatus::Finished);
    assert_eq!(job.restarts(), 1);
    assert_eq!(job.failures(), ["stage `window` was killed"]);
    assert!(job.checkpoints().restored_from.is_some());
    assert_exactly_once(&output);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn union_of_two_sources_recovers_exactly_once() {
    let dir = TempDir::new().unwrap();
    let output = CollectSink::new().exactly_once();
    let sink = output.clone();
    let job = run_job(config("union", dir.path(), StateBackendKind::RocksDb), move |env| {
        let crash = env.attempt() == 0;
        // Even and odd events from two sources running at different speeds:
        // barrier alignment has to hold back the faster one.
        let evens = env.from_source(
            "evens",
            IterSource::new(events().filter(|e| e.value % 2 == 0)).with_rate(30_000.0),
            WatermarkStrategy::monotonous().with_emit_every(20),
        );
        let odds = env.from_source(
            "odds",
            IterSource::new(events().filter(|e| e.value % 2 == 1)).with_rate(15_000.0),
            WatermarkStrategy::monotonous().with_emit_every(20),
        );
        evens
            .union([odds])
            .map(move |x: u64| {
                assert!(!(crash && x == 3_001), "injected failure");
                x
            })
            .key_by(|x: &u64| (x % KEYS).to_string())
            .window(TumblingWindows::of_millis(WINDOW_MS), Count)
            .sink("output", sink.clone());
    })
    .unwrap();

    assert_eq!(finish(&job).await, JobStatus::Finished, "{:?}", job.failures());
    assert_eq!(job.restarts(), 1);
    let history = job.checkpoints();
    assert!(history.restored_from.is_some(), "{history:?}");
    let union_stats = history
        .records
        .iter()
        .filter(|r| r.status == CheckpointStatus::Completed)
        .flat_map(|r| &r.stages)
        .find(|s| s.stage.ends_with("union"))
        .expect("union stage acknowledged checkpoints");
    assert_eq!(union_stats.state_size, 0, "union is stateless");
    assert_exactly_once(&output);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_run_resumes_from_the_previous_runs_checkpoint() {
    let dir = TempDir::new().unwrap();
    let output = CollectSink::new().exactly_once();

    let sink = output.clone();
    let first = run_job(config("resume", dir.path(), StateBackendKind::RocksDb), move |env| {
        counting_pipeline(env, &sink, 20_000.0, None)
    })
    .unwrap();
    wait_for_checkpoint(&first).await;
    first.cancel();
    assert_eq!(finish(&first).await, JobStatus::Cancelled);
    let last_checkpoint = first.checkpoints().latest_completed.unwrap();

    let sink = output.clone();
    let mut resumed_config = config("resume", dir.path(), StateBackendKind::RocksDb);
    if let Some(cp) = resumed_config.checkpointing.take() {
        resumed_config.checkpointing = Some(cp.resuming());
    }
    let second = run_job(resumed_config, move |env| counting_pipeline(env, &sink, 200_000.0, None)).unwrap();
    assert_eq!(finish(&second).await, JobStatus::Finished);

    let history = second.checkpoints();
    assert!(history.restored_from.is_some_and(|id| id >= last_checkpoint), "{history:?}");
    assert!(
        history.records.iter().all(|r| r.id > last_checkpoint),
        "checkpoint ids continue across runs"
    );
    assert_exactly_once(&output);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rocksdb_checkpoints_are_incremental_and_old_ones_are_removed() {
    let dir = TempDir::new().unwrap();
    let output = CollectSink::new().exactly_once();
    let sink = output.clone();
    let job = run_job(config("incremental", dir.path(), StateBackendKind::RocksDb), move |env| {
        counting_pipeline(env, &sink, 20_000.0, None)
    })
    .unwrap();
    assert_eq!(finish(&job).await, JobStatus::Finished);
    assert_exactly_once(&output);

    let history = job.checkpoints();
    let completed: Vec<_> = history
        .records
        .iter()
        .filter(|r| r.status == CheckpointStatus::Completed)
        .collect();
    assert!(completed.len() >= 5, "{history:?}");
    assert!(
        completed.iter().any(|r| r.uploaded_bytes < r.state_size),
        "some checkpoint reused SST files uploaded earlier: {completed:#?}"
    );

    let mut on_disk: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("chk-"))
        .collect();
    on_disk.sort();
    assert!(on_disk.len() <= 2, "only the retained checkpoints remain: {on_disk:?}");
    assert!(!dir.path().join("_work").join("attempt-0").exists(), "work dir cleaned up");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_stage_times_out_a_checkpoint_without_failing_the_job() {
    let dir = TempDir::new().unwrap();
    let mut job_config = config("timeout", dir.path(), StateBackendKind::Memory);
    if let Some(cp) = job_config.checkpointing.take() {
        job_config.checkpointing = Some(cp.with_timeout(Duration::from_millis(50)));
    }
    let output = CollectSink::new().exactly_once();
    let sink = output.clone();
    let job = run_job(job_config, move |env| {
        env.from_source("events", IterSource::new(events()).with_rate(40_000.0), WatermarkStrategy::monotonous())
            .map(|x: u64| {
                if x == 2_000 {
                    std::thread::sleep(Duration::from_millis(300)); // stalls barriers behind it
                }
                x
            })
            .key_by(|x: &u64| (x % KEYS).to_string())
            .window(TumblingWindows::of_millis(WINDOW_MS), Count)
            .sink("output", sink.clone());
    })
    .unwrap();

    assert_eq!(finish(&job).await, JobStatus::Finished);
    assert_eq!(job.restarts(), 0);
    let history = job.checkpoints();
    assert!(
        history
            .records
            .iter()
            .any(|r| matches!(&r.status, CheckpointStatus::Failed { reason } if reason.contains("timed out"))),
        "{history:?}"
    );
    assert!(history.completed > 0, "checkpointing carries on after the timeout");
    assert_exactly_once(&output);
}

#[tokio::test]
async fn failure_without_restarts_left_fails_the_job() {
    let dir = TempDir::new().unwrap();
    let job_config = config("doomed", dir.path(), StateBackendKind::Memory)
        .with_restart(RestartStrategy::fixed_delay(1, Duration::ZERO));
    let job = run_job(job_config, |env| {
        let _ = env
            .from_iter("events", events(), WatermarkStrategy::monotonous())
            .map(|x: u64| {
                assert!(x != 10, "always fails");
                x
            })
            .collect();
    })
    .unwrap();
    match finish(&job).await {
        JobStatus::Failed { error } => assert!(error.contains("always fails"), "{error}"),
        other => panic!("expected failure, got {other:?}"),
    }
    assert_eq!(job.restarts(), 1);
    assert_eq!(job.failures().len(), 2);
}
