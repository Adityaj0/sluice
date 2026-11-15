//! SQL text → running pipeline → output, checked against a naive
//! evaluation of the same query.

use std::collections::BTreeMap;
use std::time::Duration;

use sluice_core::Event;
use sluice_runtime::source::IterSource;
use sluice_runtime::{
    CheckpointConfig, CollectSink, JobConfig, JobStatus, RestartStrategy, StateBackendKind, StreamEnv,
    WatermarkStrategy, run_job,
};
use sluice_sql::{compile, compile_with};
use serde_json::{Value, json};

const FIVE_MINUTES: u64 = 300_000;

struct Tx {
    user: String,
    amount: i64,
    item: &'static str,
    time: u64,
}

/// 6000 transactions over ~2 hours from 8 users, amounts -20..180,
/// timestamps up to 10s out of order.
fn transactions() -> Vec<Tx> {
    let mut state = 11u64;
    let mut next = move || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        state >> 33
    };
    let items = ["book", "coffee", "game"];
    (0..6_000u64)
        .map(|i| Tx {
            user: format!("u{}", next() % 8),
            amount: (next() % 200) as i64 - 20,
            item: items[(next() % 3) as usize],
            time: 1_000_000 + i * 1_200 - next() % 10_000,
        })
        .collect()
}

fn encode(tx: &Tx) -> Event<Vec<u8>> {
    let record = json!({ "user_id": tx.user, "amount": tx.amount, "item": tx.item, "noise": "x".repeat(50) });
    Event::new(record.to_string().into_bytes(), tx.time)
}

/// Runs a query over the transactions as a one-off pipeline.
async fn run(sql: &str, optimize: bool) -> Vec<Value> {
    let query = compile_with(sql, optimize).unwrap();
    let env = StreamEnv::new();
    let input = env.from_iter(
        "transactions",
        transactions().iter().map(encode).collect::<Vec<_>>(),
        WatermarkStrategy::bounded_out_of_orderness(10_000).with_emit_every(20),
    );
    let output = query.physical.build(input).collect();
    let job = env.execute();
    assert_eq!(tokio::time::timeout(Duration::from_secs(30), job.wait()).await.unwrap(), JobStatus::Finished);
    assert!(job.metrics().iter().all(|m| m.late_records == 0));
    output.values()
}

fn sorted(mut rows: Vec<Value>) -> Vec<Value> {
    rows.sort_by_key(|r| r.to_string());
    rows
}

#[tokio::test]
async fn the_plan_example_query() {
    let sql = "SELECT user_id, COUNT(*) as tx_count, SUM(amount) as total
               FROM transactions
               WHERE amount > 0
               WINDOW TUMBLING (SIZE 5 MINUTES)
               GROUP BY user_id
               HAVING COUNT(*) > 10";

    let mut expected: BTreeMap<(u64, String), (i64, i64)> = BTreeMap::new();
    for tx in transactions().iter().filter(|t| t.amount > 0) {
        let entry = expected.entry((tx.time / FIVE_MINUTES, tx.user.clone())).or_default();
        entry.0 += 1;
        entry.1 += tx.amount;
    }
    let expected: Vec<Value> = expected
        .into_iter()
        .filter(|(_, (count, _))| *count > 10)
        .map(|((_, user), (count, total))| json!({ "user_id": user, "tx_count": count, "total": total }))
        .collect();
    assert!(expected.len() > 100, "the HAVING clause keeps most groups");

    let got = run(sql, true).await;
    assert_eq!(sorted(got.clone()), sorted(expected));
    // Column order follows the SELECT list.
    let keys: Vec<_> = got[0].as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys, ["user_id", "tx_count", "total"]);
}

#[tokio::test]
async fn optimizations_do_not_change_results() {
    let sql = "SELECT user_id, item, WINDOW_START AS ws, COUNT(*) AS n, AVG(amount) AS avg, MIN(amount) AS lo, MAX(amount) AS hi
               FROM transactions
               WHERE amount > 10 - 10 AND TRUE
               WINDOW TUMBLING (SIZE 10 MINUTES)
               GROUP BY user_id, item
               HAVING user_id != 'u3' AND COUNT(*) >= 2 * 2";
    let optimized = compile(sql).unwrap();
    assert!(optimized.rules.len() >= 4, "{:#?}", optimized.rules);
    let names = optimized.physical.stage_names();
    assert_eq!(names[0], "scan(transactions where amount > 0 AND user_id != 'u3' fields [amount, item, user_id])");

    let with = sorted(run(sql, true).await);
    let without = sorted(run(sql, false).await);
    assert!(!with.is_empty());
    assert_eq!(with, without);
    assert!(with.iter().all(|r| r["user_id"] != "u3" && r["n"].as_u64() >= Some(4)));
}

#[tokio::test]
async fn stateless_queries_transform_every_record() {
    // AND binds tighter than OR: every book, plus games with a refund.
    let rows = run(
        "SELECT UPPER(user_id) AS user, amount * 2 + 1 AS score, amount / 4 AS quarter, coupon IS NULL AS no_coupon
         FROM transactions WHERE item = 'book' OR item = 'game' AND amount < 0",
        true,
    )
    .await;
    let expected: Vec<Value> = transactions()
        .into_iter()
        .filter(|t| t.item == "book" || (t.item == "game" && t.amount < 0))
        .map(|t| {
            json!({
                "user": t.user.to_uppercase(),
                "score": t.amount * 2 + 1,
                "quarter": t.amount as f64 / 4.0,
                "no_coupon": true,
            })
        })
        .collect();
    assert_eq!(sorted(rows), sorted(expected));
}

#[tokio::test]
async fn group_by_several_keys() {
    let rows = run(
        "SELECT item, user_id, COUNT(*) AS n FROM transactions
         WINDOW TUMBLING (SIZE 1 HOUR) GROUP BY item, user_id HAVING COUNT(*) > 30",
        true,
    )
    .await;
    let mut expected: BTreeMap<(u64, &str, String), u64> = BTreeMap::new();
    for tx in transactions() {
        *expected.entry((tx.time / 3_600_000, tx.item, tx.user.clone())).or_default() += 1;
    }
    let expected: Vec<Value> = expected
        .into_iter()
        .filter(|(_, n)| *n > 30)
        .map(|((_, item, user), n)| json!({ "item": item, "user_id": user, "n": n }))
        .collect();
    assert!(!expected.is_empty());
    assert_eq!(sorted(rows), sorted(expected));
}

#[tokio::test]
async fn sliding_and_session_windows_in_sql() {
    let rows = run(
        "SELECT user_id, WINDOW_START, WINDOW_END, COUNT(*) AS n FROM transactions
         WINDOW SLIDING (SIZE 10 MINUTES, SLIDE 5 MINUTES) GROUP BY user_id",
        true,
    )
    .await;
    let mut expected: BTreeMap<(u64, String), u64> = BTreeMap::new();
    for tx in transactions() {
        let last = tx.time / FIVE_MINUTES * FIVE_MINUTES;
        for start in [last.saturating_sub(FIVE_MINUTES), last] {
            if start + 2 * FIVE_MINUTES > tx.time && start <= tx.time {
                *expected.entry((start, tx.user.clone())).or_default() += 1;
            }
        }
    }
    let got: BTreeMap<(u64, String), u64> = rows
        .iter()
        .map(|r| {
            assert_eq!(r["window_end"].as_u64().unwrap() - r["window_start"].as_u64().unwrap(), 2 * FIVE_MINUTES);
            ((r["window_start"].as_u64().unwrap(), r["user_id"].as_str().unwrap().to_owned()), r["n"].as_u64().unwrap())
        })
        .collect();
    assert_eq!(got, expected);

    // Every user buys every ~10s on average: with a 1-minute gap almost
    // everything merges into one session per user.
    let sessions = run(
        "SELECT user_id, COUNT(*) AS n FROM transactions WINDOW SESSION (GAP 1 MINUTE) GROUP BY user_id",
        true,
    )
    .await;
    let total: u64 = sessions.iter().map(|r| r["n"].as_u64().unwrap()).sum();
    assert_eq!(total, 6_000, "every event lands in exactly one session");
    assert!(sessions.len() >= 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_job_recovers_exactly_once() {
    let sql = "SELECT user_id, COUNT(*) AS n, SUM(amount) AS total FROM transactions
               WINDOW TUMBLING (SIZE 5 MINUTES) GROUP BY user_id";
    let expected = sorted(run(sql, true).await);

    let dir = tempfile::TempDir::new().unwrap();
    let output = CollectSink::new().exactly_once();
    let sink = output.clone();
    let config = JobConfig::new("sql")
        .with_checkpointing(
            CheckpointConfig::new(dir.path())
                .with_interval(Duration::from_millis(10))
                .with_backend(StateBackendKind::RocksDb),
        )
        .with_restart(RestartStrategy::fixed_delay(2, Duration::ZERO));
    let query = compile(sql).unwrap();
    let job = run_job(config, move |env| {
        let crash = env.attempt() == 0;
        let events: Vec<_> = transactions()
            .iter()
            .enumerate()
            .map(|(i, tx)| {
                let mut event = encode(tx);
                if crash && i == 4_000 {
                    event.value = b"__crash__".to_vec();
                }
                event
            })
            .collect();
        let input = env
            .from_source(
                "transactions",
                IterSource::new(events.into_iter()).with_rate(40_000.0),
                WatermarkStrategy::bounded_out_of_orderness(10_000).with_emit_every(20),
            )
            .map(|bytes: Vec<u8>| {
                assert!(bytes != b"__crash__", "injected failure");
                bytes
            });
        query.physical.build(input).sink("output", sink.clone());
    })
    .unwrap();

    assert_eq!(tokio::time::timeout(Duration::from_secs(30), job.wait()).await.unwrap(), JobStatus::Finished);
    assert_eq!(job.restarts(), 1);
    assert!(job.checkpoints().restored_from.is_some());
    assert_eq!(sorted(output.values()), expected);
}
