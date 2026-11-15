use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use sluice_api::{JobManager, app};
use sluice_core::Event;
use sluice_ingest::EventRouter;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

async fn call(app: &Router, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, body)
}

async fn get(app: &Router, uri: &str) -> Value {
    call(app, Method::GET, uri, None).await.1
}

/// Polls `uri` until `done` accepts the response.
async fn wait_for(app: &Router, uri: &str, done: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let body = get(app, uri).await;
            if done(&body) {
                return body;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting on {uri}"))
}

struct Setup {
    router: EventRouter,
    app: Router,
    data: TempDir,
}

fn setup() -> Setup {
    let data = TempDir::new().unwrap();
    let router = EventRouter::new();
    let manager = Arc::new(JobManager::new(router.clone(), data.path()));
    Setup {
        app: app(manager),
        router,
        data,
    }
}

fn spend_job() -> Value {
    json!({
        "name": "spend-per-user",
        "source": { "type": "grpc", "stream": "purchases" },
        "operators": [
            { "type": "filter", "field": "amount", "op": "gt", "value": 0 },
            { "type": "key_by", "field": "user_id" },
            { "type": "window", "window": { "kind": "tumbling", "size_ms": 1000 },
              "aggregate": { "fn": "sum", "field": "amount" } }
        ]
    })
}

async fn send(router: &EventRouter, user: &str, amount: f64, t: u64) {
    let payload = json!({ "user_id": user, "amount": amount }).to_string().into_bytes();
    router.route("purchases", Event::new(payload, t)).await.unwrap();
}

fn window_sums(output: &Value) -> Vec<(Value, Value, Value)> {
    output
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["value"]["window_start"].clone(), r["key"].clone(), r["value"]["sum"].clone()))
        .collect()
}

fn expected_sums() -> Vec<(Value, Value, Value)> {
    vec![
        (json!(0), json!("alice"), json!(17.0)),
        (json!(0), json!("bob"), json!(5.0)),
        (json!(1000), json!("bob"), json!(1.0)),
    ]
}

#[tokio::test]
async fn invalid_requests_are_rejected() {
    let Setup { app, .. } = setup();

    let mut job = spend_job();
    job["operators"].as_array_mut().unwrap().remove(1); // drop the key_by
    let (status, body) = call(&app, Method::POST, "/jobs", Some(job)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("key_by"), "{body}");

    let mut job = spend_job();
    job["checkpointing"] = json!({ "interval_ms": 0 });
    let (status, body) = call(&app, Method::POST, "/jobs", Some(job)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, body) = call(&app, Method::POST, "/jobs", Some(json!({ "name": "x" }))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(body["error"].as_str().unwrap().contains("source"), "{body}");

    let (status, _) = call(&app, Method::GET, "/jobs/99", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn submitted_job_processes_ingested_events() {
    let Setup { router, app, .. } = setup();

    let (status, job) = call(&app, Method::POST, "/jobs", Some(spend_job())).await;
    assert_eq!(status, StatusCode::CREATED, "{job}");
    assert_eq!(job["status"]["state"], "running");
    let id = job["id"].as_u64().unwrap();

    send(&router, "alice", 10.0, 100).await;
    send(&router, "bob", 5.0, 200).await;
    send(&router, "alice", 7.0, 900).await;
    send(&router, "alice", -3.0, 950).await; // filtered out
    send(&router, "bob", 1.0, 1_500).await;
    router.route("purchases", Event::new(b"not json".to_vec(), 1_600)).await.unwrap();
    send(&router, "carol", 1.0, 5_000).await; // pushes the watermark past 2000

    // The source's periodic watermark fires the first two windows.
    let output = wait_for(&app, &format!("/jobs/{id}/output"), |o| o.as_array().unwrap().len() >= 3).await;
    assert_eq!(window_sums(&output), expected_sums());

    let metrics = get(&app, &format!("/jobs/{id}/metrics")).await;
    let names: Vec<_> = metrics.as_array().unwrap().iter().map(|m| m["name"].as_str().unwrap().to_owned()).collect();
    assert_eq!(
        names,
        [
            "source(grpc:purchases)",
            "decode(json)",
            "filter(amount > 0)",
            "key_by(user_id)",
            "window(tumbling 1000ms, sum(amount))",
            "sink(buffer)"
        ]
    );
    assert_eq!(metrics[1]["records_in"], 7);
    assert_eq!(metrics[1]["records_out"], 6, "bad JSON dropped");
    assert_eq!(metrics[2]["records_out"], 5, "negative amount dropped");

    let (_, prom) = call(&app, Method::GET, "/metrics", None).await;
    let prom = prom.as_str().unwrap();
    assert!(prom.contains(r#"sluice_operator_records_in_total{job_id="1",job="spend-per-user",operator_id="1",operator="decode(json)"} 7"#), "{prom}");
    assert!(prom.contains(r#"sluice_job_restarts_total{job_id="1",job="spend-per-user"} 0"#), "{prom}");

    let streams = get(&app, "/streams").await;
    assert_eq!(streams[0]["name"], "purchases");
    assert_eq!(streams[0]["subscribers"], 1);
    assert_eq!(streams[0]["next_offset"], 7);

    let (status, _) = call(&app, Method::DELETE, &format!("/jobs/{id}"), None).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let cancelled = wait_for(&app, &format!("/jobs/{id}"), |j| j["status"]["state"] == "cancelled").await;
    assert_eq!(cancelled["definition"]["name"], "spend-per-user");

    // The cancelled job releases its subscription.
    wait_for(&app, "/streams", |s| s[0]["subscribers"] == 0).await;
    assert!(router.route("purchases", Event::new(vec![], 0)).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpointed_job_recovers_exactly_once_after_a_stage_is_killed() {
    let Setup { router, app, data } = setup();

    let mut job = spend_job();
    job["checkpointing"] = json!({ "interval_ms": 20, "backend": "rocksdb" });
    job["restart"] = json!({ "max_restarts": 2, "delay_ms": 0 });
    let (status, job) = call(&app, Method::POST, "/jobs", Some(job)).await;
    assert_eq!(status, StatusCode::CREATED, "{job}");
    let id = job["id"].as_u64().unwrap();

    // First half of the input, then wait for a checkpoint to cover it.
    send(&router, "alice", 10.0, 100).await;
    send(&router, "bob", 5.0, 200).await;
    send(&router, "alice", 7.0, 900).await;
    let covered = wait_for(&app, &format!("/jobs/{id}/checkpoints"), |c| c["completed"].as_u64() >= Some(2)).await;
    assert!(covered["records"][0]["stages"].as_array().unwrap().len() == 6, "{covered}");

    // Crash the stateful window stage.
    let details = get(&app, &format!("/jobs/{id}")).await;
    let window = details["stages"]
        .as_array()
        .unwrap()
        .iter()
        .position(|s| s["uid"].as_str().unwrap().contains("window"))
        .unwrap();
    let (status, _) = call(&app, Method::POST, &format!("/jobs/{id}/stages/{window}/kill"), None).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _) = call(&app, Method::POST, &format!("/jobs/{id}/stages/99/kill"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let recovered = wait_for(&app, &format!("/jobs/{id}"), |j| j["restarts"] == 1 && j["status"]["state"] == "running").await;
    assert_eq!(recovered["failures"][0], "stage `window(tumbling 1000ms, sum(amount))` was killed");

    // Second half, after recovery.
    send(&router, "alice", -3.0, 950).await;
    send(&router, "bob", 1.0, 1_500).await;
    send(&router, "carol", 1.0, 5_000).await;

    // Output appears once a checkpoint after the windows fired completes.
    wait_for(&app, &format!("/jobs/{id}/output"), |o| o.as_array().unwrap().len() >= 3).await;
    tokio::time::sleep(Duration::from_millis(100)).await; // later checkpoints must not duplicate anything
    let output = get(&app, &format!("/jobs/{id}/output")).await;
    assert_eq!(window_sums(&output), expected_sums(), "exactly once across the crash");

    let checkpoints = get(&app, &format!("/jobs/{id}/checkpoints")).await;
    assert!(checkpoints["restored_from"].as_u64().is_some(), "{checkpoints}");
    let dir = get(&app, &format!("/jobs/{id}")).await["checkpoint_dir"].as_str().unwrap().to_owned();
    assert!(dir.starts_with(data.path().to_str().unwrap()));
    assert!(std::fs::read_dir(&dir).unwrap().any(|e| e.unwrap().file_name().to_string_lossy().starts_with("chk-")));

    // Committed offsets let the log drop everything already checkpointed.
    let streams = wait_for(&app, "/streams", |s| s[0]["retained"] == 0).await;
    assert_eq!(streams[0]["next_offset"], 6);
}

#[tokio::test]
async fn sliding_window_job_with_late_events_emitted() {
    let Setup { router, app, .. } = setup();
    let job = json!({
        "name": "late-aware",
        "source": { "type": "grpc", "stream": "purchases", "idle_timeout_ms": 10000 },
        "operators": [
            { "type": "key_by", "field": "user_id" },
            { "type": "window", "window": { "kind": "sliding", "size_ms": 1000, "slide_ms": 500 },
              "aggregate": { "fn": "count" }, "allowed_lateness_ms": 2500, "late_events": "emit" }
        ]
    });
    let (status, job) = call(&app, Method::POST, "/jobs", Some(job)).await;
    assert_eq!(status, StatusCode::CREATED, "{job}");
    let id = job["id"].as_u64().unwrap();

    send(&router, "ann", 1.0, 600).await; // windows [0,1000) and [500,1500)
    send(&router, "ann", 1.0, 3_000).await; // watermark 2999: both fire
    wait_for(&app, &format!("/jobs/{id}/output"), |o| o.as_array().unwrap().len() >= 2).await;
    send(&router, "ann", 1.0, 700).await; // late, within lateness (1499 + 2500 > 2999): both windows re-fire
    send(&router, "ann", 1.0, 5_000).await; // watermark 4999: both windows expire
    wait_for(&app, &format!("/jobs/{id}/metrics"), |m| m[3]["watermark"].as_u64() >= Some(4_999)).await;
    send(&router, "ann", 1.0, 800).await; // too late: emitted as a late event

    let output = wait_for(&app, &format!("/jobs/{id}/output"), |o| {
        o.as_array().unwrap().iter().any(|r| r["value"].get("late_event").is_some())
    })
    .await;
    let rows: Vec<_> = output
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["value"].clone())
        .filter(|v| v.get("late_event").is_some() || v["window_start"].as_u64() < Some(1000))
        .collect();
    assert_eq!(
        rows,
        vec![
            json!({ "key": "ann", "window_start": 0, "window_end": 1000, "count": 1 }),
            json!({ "key": "ann", "window_start": 500, "window_end": 1500, "count": 1 }),
            json!({ "key": "ann", "window_start": 0, "window_end": 1000, "count": 2, "late_firing": true }),
            json!({ "key": "ann", "window_start": 500, "window_end": 1500, "count": 2, "late_firing": true }),
            json!({ "late_event": { "user_id": "ann", "amount": 1.0 } }),
        ]
    );

    let metrics = get(&app, &format!("/jobs/{id}/metrics")).await;
    let names: Vec<_> = metrics.as_array().unwrap().iter().map(|m| m["name"].as_str().unwrap().to_owned()).collect();
    assert_eq!(
        names,
        [
            "source(grpc:purchases)",
            "decode(json)",
            "key_by(user_id)",
            "window(sliding 1000ms every 500ms, count(*), lateness 2500ms)",
            "late-events",
            "late-events(json)",
            "union",
            "sink(buffer)"
        ]
    );
    let window = &metrics[3];
    assert_eq!(window["late_records"], 1);
    assert!(window["backpressure_ratio"].as_f64().is_some());
    assert_eq!(window["input_capacity"], 1024);

    let (_, prom) = call(&app, Method::GET, "/metrics", None).await;
    assert!(prom.as_str().unwrap().contains("sluice_operator_backpressure_ratio{"));
}

#[tokio::test]
async fn sql_jobs_and_explain() {
    let Setup { router, app, .. } = setup();

    let query = "SELECT user_id, COUNT(*) AS tx_count, SUM(amount) AS total
                 FROM purchases WHERE amount > 0
                 WINDOW TUMBLING (SIZE 1 SECOND) GROUP BY user_id HAVING COUNT(*) > 1";
    let (status, plan) = call(&app, Method::POST, "/sql/explain", Some(json!({ "query": query }))).await;
    assert_eq!(status, StatusCode::OK, "{plan}");
    assert_eq!(plan["stream"], "purchases");
    assert_eq!(plan["optimized"]["node"], "Project");
    let stages: Vec<_> = plan["stages"].as_array().unwrap().iter().map(|s| s["kind"].as_str().unwrap()).collect();
    assert_eq!(stages, ["scan", "key_by", "window", "filter", "project"]);
    assert!(plan["explain"].as_str().unwrap().contains("== Optimized plan =="));

    let (status, error) = call(&app, Method::POST, "/sql/explain", Some(json!({ "query": "SELECT FROM t" }))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["details"]["kind"], "syntax");
    assert_eq!(error["details"]["column"], 8);

    let mut job = json!({ "name": "sql", "source": { "type": "grpc", "stream": "other" }, "query": query });
    let (status, body) = call(&app, Method::POST, "/jobs", Some(job.clone())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("source stream is `other`"), "{body}");

    job["source"]["stream"] = json!("purchases");
    let (status, body) = call(&app, Method::POST, "/jobs", Some(job)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let id = body["id"].as_u64().unwrap();

    send(&router, "alice", 10.0, 100).await;
    send(&router, "alice", 7.0, 900).await;
    send(&router, "alice", -3.0, 950).await; // filtered by WHERE
    send(&router, "bob", 5.0, 200).await; // only one: filtered by HAVING
    send(&router, "carol", 1.0, 5_000).await; // advances the watermark
    let output = wait_for(&app, &format!("/jobs/{id}/output"), |o| !o.as_array().unwrap().is_empty()).await;
    assert_eq!(output[0]["value"], json!({ "user_id": "alice", "tx_count": 2, "total": 17.0 }));
    assert_eq!(output.as_array().unwrap().len(), 1);

    let metrics = get(&app, &format!("/jobs/{id}/metrics")).await;
    assert_eq!(metrics[1]["name"], "scan(purchases where amount > 0 fields [amount, user_id])");
    assert_eq!(metrics[1]["records_in"], 5);
    assert_eq!(metrics[1]["records_out"], 4, "the scan drops the refund itself");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_travel_state_inspection() {
    for (backend, sql) in [("memory", false), ("rocksdb", false), ("rocksdb", true)] {
        let Setup { router, app, .. } = setup();
        let mut job = spend_job();
        if sql {
            job["operators"] = json!([]);
            job["query"] = json!("SELECT user_id, SUM(amount) AS total FROM purchases WINDOW TUMBLING (SIZE 1 SECOND) GROUP BY user_id");
        }
        job["checkpointing"] = json!({ "interval_ms": 20, "backend": backend, "retained": 5 });
        let (status, body) = call(&app, Method::POST, "/jobs", Some(job)).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");

        // Window [0, 1000) stays open: its contents are in every checkpoint.
        send(&router, "alice", 10.0, 100).await;
        send(&router, "bob", 5.0, 200).await;
        send(&router, "alice", 7.0, 900).await;
        let checkpoints = wait_for(&app, "/jobs/1/checkpoints", |c| {
            c["completed"].as_u64() >= Some(3) && !c["retained"].as_array().unwrap().is_empty()
        })
        .await;
        let latest = checkpoints["retained"].as_array().unwrap().last().unwrap().as_u64().unwrap();

        let state = get(&app, &format!("/jobs/1/checkpoints/{latest}/state")).await;
        assert_eq!(state["checkpoint"], latest, "{state}");
        let stages = state["stages"].as_array().unwrap();
        let source = &stages[0];
        assert_eq!(source["operator_state"]["source"]["position"], 3, "{source}");
        let window = stages.iter().find(|s| s["name"].as_str().unwrap().starts_with("window")).unwrap();
        assert_eq!(window["keyed_state"]["backend"], backend);
        let entries = &window["keyed_state"]["states"][0]["entries"];
        let by_key: Vec<_> = entries
            .as_array()
            .unwrap()
            .iter()
            .map(|e| (e["key"].clone(), e["window"]["start"].clone(), if sql { e["value"]["aggregates"][0].clone() } else { e["value"].clone() }))
            .collect();
        assert_eq!(
            by_key,
            vec![(json!("alice"), json!(0), json!(17.0)), (json!("bob"), json!(0), json!(5.0))],
            "{backend} sql={sql}: {window}"
        );
        let watermark = window["operator_state"]["window"]["watermark"].as_u64();
        assert!(watermark < Some(999), "window [0, 1000) has not fired: {watermark:?}");

        let (status, body) = call(&app, Method::GET, "/jobs/1/checkpoints/999999/state", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["error"].as_str().unwrap().contains("retained checkpoints"), "{body}");
    }
}

#[tokio::test]
async fn live_snapshot_carries_topology_metrics_and_checkpoints() {
    let data = TempDir::new().unwrap();
    let router = EventRouter::new();
    let manager = Arc::new(JobManager::new(router.clone(), data.path()));
    let mut job = spend_job();
    job["operators"][2]["late_events"] = json!("emit");
    let summary = manager.submit(serde_json::from_value(job).unwrap()).unwrap();
    send(&router, "alice", 1.0, 100).await;
    send(&router, "alice", 1.0, 5_000).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let live = manager.live(summary.id).unwrap();
    assert_eq!(live["job"]["status"]["state"], "running");
    let inputs: Vec<_> = live["stages"].as_array().unwrap().iter().map(|s| s["inputs"].clone()).collect();
    // source → decode → filter → key_by → window → split ─┬─────────────→ union → sink
    //                                                     └→ late(json) ─┘
    assert_eq!(
        inputs,
        vec![json!([]), json!([0]), json!([1]), json!([2]), json!([3]), json!([4]), json!([5]), json!([5, 6]), json!([7])]
    );
    let sink = live["operators"].as_array().unwrap().last().unwrap().clone();
    assert!(sink["latency_ms"]["samples"].as_u64() >= Some(1), "{sink}");
    assert!(live["checkpoints"]["recent"].as_array().unwrap().is_empty());

    let app = app(manager);
    let (status, _) = call(&app, Method::GET, &format!("/jobs/{}/live", summary.id), None).await;
    assert!(status.is_client_error(), "a plain GET is not a WebSocket upgrade");
}

#[tokio::test]
async fn serves_the_ui_with_spa_fallback() {
    let ui = TempDir::new().unwrap();
    std::fs::write(ui.path().join("index.html"), "<html>sluice</html>").unwrap();
    std::fs::create_dir(ui.path().join("assets")).unwrap();
    std::fs::write(ui.path().join("assets/app.js"), "console.log(1)").unwrap();
    let data = TempDir::new().unwrap();
    let manager = Arc::new(JobManager::new(EventRouter::new(), data.path()));
    let app = sluice_api::app_with_ui(manager, Some(ui.path().to_path_buf()));

    assert_eq!(call(&app, Method::GET, "/assets/app.js", None).await.1, json!("console.log(1)"));
    assert_eq!(call(&app, Method::GET, "/debugger", None).await.1, json!("<html>sluice</html>"));
    assert_eq!(call(&app, Method::GET, "/jobs", None).await.1, json!([]), "the API still wins");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_from_the_ui_generators_and_manual_events() {
    let Setup { app, .. } = setup();

    // Nothing reads the stream yet: sending is refused, a generator waits.
    let (status, body) = call(&app, Method::POST, "/streams/purchases/events", Some(json!({ "user_id": "ann", "amount": 3 }))).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (status, generator) = call(&app, Method::POST, "/generators", Some(json!({ "stream": "purchases", "rate": 2000, "users": 5 }))).await;
    assert_eq!(status, StatusCode::CREATED, "{generator}");
    let (status, _) = call(&app, Method::POST, "/generators", Some(json!({ "stream": "purchases", "rate": 1e9 }))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, job) = call(&app, Method::POST, "/jobs", Some(spend_job())).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(job["kind"], "pipeline");
    assert_eq!(job["stream"], "purchases");

    let generators = wait_for(&app, "/generators", |g| g[0]["sent"].as_u64() > Some(100) && g[0]["waiting"] == false).await;
    assert_eq!(generators[0]["rate"], 2000.0);

    let (status, sent) = call(&app, Method::POST, "/streams/purchases/events", Some(json!([{ "user_id": "ann", "amount": 3 }, { "user_id": "bo", "amount": 4 }]))).await;
    assert_eq!(status, StatusCode::OK, "{sent}");
    assert_eq!(sent["count"], 2);

    let id = generator["id"].as_u64().unwrap();
    let (status, _) = call(&app, Method::DELETE, &format!("/generators/{id}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(get(&app, "/generators").await, json!([]));
    let (status, _) = call(&app, Method::DELETE, &format!("/generators/{id}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
