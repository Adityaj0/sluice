//! `sluice-loadgen`: synthetic purchase events at high rate over gRPC
//! streams, plus measurement of what the engine does with them.
//!
//! ```text
//! cargo run --release --bin sluice-loadgen -- \
//!     --submit examples/sql_job.json --events 2000000 --connections 4
//! ```
//!
//! Prints ingest throughput, end-to-end processing throughput (until the
//! job's source has consumed every event), per-stage counts and checkpoint
//! statistics as markdown. `--kill-stage window --kill-after 3` also crashes
//! a stage mid-run and measures how long recovery takes.

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sluice_ingest::proto::ingest_client::IngestClient;
use sluice_ingest::proto::{Event, EventBatch};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone)]
struct Args {
    grpc: String,
    api: String,
    stream: String,
    /// 0 = run until interrupted.
    events: u64,
    /// Events per second across all connections; 0 = as fast as possible.
    rate: f64,
    batch: usize,
    connections: usize,
    users: u64,
    disorder_ms: u64,
    /// Jobs to deploy first; measurements follow the first one.
    submit: Vec<PathBuf>,
    kill_stage: Option<String>,
    kill_after: f64,
    quiet: bool,
}

const USAGE: &str = "usage: sluice-loadgen [--grpc http://127.0.0.1:50051] [--api 127.0.0.1:8080] [--stream purchases]
       [--events 1000000 (0 = forever)] [--rate 0 (events/s, 0 = max)] [--batch 1000] [--connections 4]
       [--users 1000] [--disorder-ms 2000] [--submit JOB.json]... [--kill-stage NAME --kill-after SECONDS] [--quiet]";

fn parse_args() -> Result<Args> {
    let mut args = Args {
        grpc: "http://127.0.0.1:50051".into(),
        api: "127.0.0.1:8080".into(),
        stream: "purchases".into(),
        events: 1_000_000,
        rate: 0.0,
        batch: 1_000,
        connections: 4,
        users: 1_000,
        disorder_ms: 2_000,
        submit: Vec::new(),
        kill_stage: None,
        kill_after: 3.0,
        quiet: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "-h" || flag == "--help" {
            println!("{USAGE}");
            std::process::exit(0);
        }
        if flag == "--quiet" {
            args.quiet = true;
            continue;
        }
        let value = it.next().ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        match flag.as_str() {
            "--grpc" => args.grpc = value,
            "--api" => args.api = value.trim_start_matches("http://").to_owned(),
            "--stream" => args.stream = value,
            "--events" => args.events = value.parse()?,
            "--rate" => args.rate = value.parse()?,
            "--batch" => args.batch = value.parse::<usize>()?.max(1),
            "--connections" => args.connections = value.parse::<usize>()?.max(1),
            "--users" => args.users = value.parse::<u64>()?.max(1),
            "--disorder-ms" => args.disorder_ms = value.parse()?,
            "--submit" => args.submit.push(value.into()),
            "--kill-stage" => args.kill_stage = Some(value),
            "--kill-after" => args.kill_after = value.parse()?,
            other => return Err(format!("unknown flag {other}\n{USAGE}").into()),
        }
    }
    Ok(args)
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// Minimal HTTP/1.1 JSON client for the REST API (keeps the tool free of
/// an HTTP client dependency).
async fn http(api: &str, method: &str, path: &str, body: Option<&str>) -> Result<(u16, Value)> {
    let mut stream = tokio::net::TcpStream::connect(api).await?;
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {api}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw);
    let (head, payload) = text.split_once("\r\n\r\n").ok_or("malformed HTTP response")?;
    let status: u16 = head.split_whitespace().nth(1).ok_or("no status")?.parse()?;
    let payload = if head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
        let mut out = String::new();
        let mut rest = payload;
        while let Some((size, tail)) = rest.split_once("\r\n") {
            let size = usize::from_str_radix(size.trim(), 16)?;
            if size == 0 {
                break;
            }
            out.push_str(&tail[..size]);
            rest = &tail[size + 2..];
        }
        out
    } else {
        payload.to_owned()
    };
    let json = serde_json::from_str(&payload).unwrap_or(Value::String(payload));
    Ok((status, json))
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

const ITEMS: [&str; 6] = ["coffee", "book", "headphones", "groceries", "shoes", "game"];

/// One producer: generates batches (rate-limited) and streams them.
async fn produce(args: Args, worker: usize, events: u64, rate: f64, acked: Arc<AtomicU64>) -> Result<u64> {
    let mut client = IngestClient::connect(args.grpc.clone()).await?;
    let (tx, rx) = mpsc::channel::<EventBatch>(4);
    let stream = args.stream.clone();
    let generator = tokio::spawn(async move {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (worker as u64 + 1));
        let started = Instant::now();
        let mut sent = 0u64;
        while events == 0 || sent < events {
            let n = if events == 0 { args.batch as u64 } else { (events - sent).min(args.batch as u64) };
            let now = now_ms();
            let batch = EventBatch {
                stream: stream.clone(),
                events: (0..n)
                    .map(|_| {
                        let user = rng.next() % args.users;
                        let cents = rng.next() % 20_000 + 100;
                        let item = ITEMS[(rng.next() % ITEMS.len() as u64) as usize];
                        let jitter = if args.disorder_ms == 0 { 0 } else { rng.next() % args.disorder_ms };
                        Event {
                            key: format!("user-{user:04}").into_bytes(),
                            value: format!(
                                r#"{{"user_id":"user-{user:04}","amount":{}.{:02},"item":"{item}"}}"#,
                                cents / 100,
                                cents % 100
                            )
                            .into_bytes(),
                            event_time: now.saturating_sub(jitter),
                        }
                    })
                    .collect(),
            };
            if tx.send(batch).await.is_err() {
                break;
            }
            sent += n;
            if rate > 0.0 {
                tokio::time::sleep_until((started + Duration::from_secs_f64(sent as f64 / rate)).into()).await;
            }
        }
    });
    let mut acks = client.ingest_stream(ReceiverStream::new(rx)).await?.into_inner();
    let mut total = 0;
    while let Some(ack) = acks.message().await? {
        total += u64::from(ack.count);
        acked.fetch_add(u64::from(ack.count), Ordering::Relaxed);
    }
    generator.abort();
    Ok(total)
}

async fn job_metrics(api: &str, id: u64) -> Result<Vec<Value>> {
    let (_, metrics) = http(api, "GET", &format!("/jobs/{id}/metrics"), None).await?;
    Ok(metrics.as_array().cloned().unwrap_or_default())
}

struct Recovery {
    killed: String,
    detected_ms: f64,
    checkpoint_ms: Option<f64>,
}

/// Kills a stage, then times the restart and the first checkpoint after it.
async fn chaos(api: String, id: u64, name: String, after: f64) -> Result<Recovery> {
    tokio::time::sleep(Duration::from_secs_f64(after)).await;
    let (_, job) = http(&api, "GET", &format!("/jobs/{id}"), None).await?;
    let stages = job["stages"].as_array().cloned().unwrap_or_default();
    let index = stages
        .iter()
        .position(|s| s["uid"].as_str().is_some_and(|u| u.contains(&name)))
        .ok_or_else(|| format!("no stage matching `{name}`"))?;
    let killed = stages[index]["uid"].as_str().unwrap_or_default().to_owned();
    let restarts_before = job["restarts"].as_u64().unwrap_or(0);
    let start = Instant::now();
    http(&api, "POST", &format!("/jobs/{id}/stages/{index}/kill"), None).await?;

    let mut detected_ms = None;
    let deadline = start + Duration::from_secs(60);
    while Instant::now() < deadline {
        let (_, job) = http(&api, "GET", &format!("/jobs/{id}"), None).await?;
        if detected_ms.is_none() && job["restarts"].as_u64().unwrap_or(0) > restarts_before && job["status"]["state"] == "running" {
            detected_ms = Some(start.elapsed().as_secs_f64() * 1000.0);
        }
        if let Some(detected) = detected_ms {
            let (_, checkpoints) = http(&api, "GET", &format!("/jobs/{id}/checkpoints"), None).await?;
            let restored = checkpoints["restored_from"].as_u64();
            let latest = checkpoints["latest_completed"].as_u64();
            if latest.is_some() && latest > restored {
                return Ok(Recovery {
                    killed,
                    detected_ms: detected,
                    checkpoint_ms: Some(start.elapsed().as_secs_f64() * 1000.0),
                });
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(Recovery {
        killed,
        detected_ms: detected_ms.unwrap_or(f64::NAN),
        checkpoint_ms: None,
    })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;

    // The engine may still be starting (docker compose, demo script).
    let mut attempts = 0;
    while !matches!(http(&args.api, "GET", "/health", None).await, Ok((200, _))) {
        attempts += 1;
        if attempts > 60 {
            return Err(format!("the engine's REST API at {} is not reachable", args.api).into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let mut job_id = None;
    for path in &args.submit {
        let definition = std::fs::read_to_string(path)?;
        let (status, job) = http(&args.api, "POST", "/jobs", Some(&definition)).await?;
        if status != 201 {
            return Err(format!("submitting {}: {job}", path.display()).into());
        }
        let id = job["id"].as_u64().ok_or("no job id")?;
        eprintln!("submitted job {id} ({})", job["name"]);
        job_id.get_or_insert(id);
    }

    let chaos = match (&args.kill_stage, job_id) {
        (Some(name), Some(id)) => Some(tokio::spawn(chaos(args.api.clone(), id, name.clone(), args.kill_after))),
        (Some(_), None) => return Err("--kill-stage needs --submit".into()),
        _ => None,
    };

    let acked = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let per_worker = args.events / args.connections as u64;
    let mut producers = Vec::new();
    for worker in 0..args.connections {
        let events = if args.events == 0 {
            0
        } else if worker == 0 {
            per_worker + args.events % args.connections as u64
        } else {
            per_worker
        };
        producers.push(tokio::spawn(produce(args.clone(), worker, events, args.rate / args.connections as f64, Arc::clone(&acked))));
    }
    let quiet = args.quiet;
    let progress = {
        let acked = Arc::clone(&acked);
        tokio::spawn(async move {
            if quiet {
                return;
            }
            let mut last = 0;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = acked.load(Ordering::Relaxed);
                eprintln!("  {:>10} events sent ({:>9}/s)", now, now - last);
                last = now;
            }
        })
    };

    let mut sent = 0;
    for producer in producers {
        sent += producer.await??;
    }
    let ingest_secs = started.elapsed().as_secs_f64();
    progress.abort();

    println!("\n### Ingest\n");
    println!("| events | connections | batch | seconds | events/s |");
    println!("|---:|---:|---:|---:|---:|");
    println!(
        "| {sent} | {} | {} | {ingest_secs:.2} | {:.0} |",
        args.connections,
        args.batch,
        sent as f64 / ingest_secs
    );

    let Some(id) = job_id else { return Ok(()) };

    // Processing: until the source has consumed everything and the rest of
    // the pipeline has drained.
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut consumed_secs = None;
    let mut last_counts = Vec::new();
    let mut stable_since = Instant::now();
    loop {
        let metrics = job_metrics(&args.api, id).await?;
        let source_in = metrics.first().and_then(|m| m["records_in"].as_u64()).unwrap_or(0);
        // Metrics restart with each attempt; after a restart the source
        // resumes from its checkpointed offset, so judge by the stream log
        // instead: everything ingested and released by a checkpoint.
        let (_, job) = http(&args.api, "GET", &format!("/jobs/{id}"), None).await?;
        let consumed = if job["restarts"].as_u64().unwrap_or(0) == 0 {
            source_in >= sent
        } else {
            let (_, streams) = http(&args.api, "GET", "/streams", None).await?;
            streams.as_array().into_iter().flatten().any(|s| {
                s["name"] == args.stream.as_str() && s["next_offset"].as_u64() >= Some(sent) && s["retained"] == 0
            })
        };
        if consumed_secs.is_none() && consumed {
            consumed_secs = Some(started.elapsed().as_secs_f64());
        }
        let counts: Vec<u64> = metrics.iter().map(|m| m["records_in"].as_u64().unwrap_or(0)).collect();
        if counts != last_counts {
            stable_since = Instant::now();
            last_counts = counts;
        }
        if consumed_secs.is_some() && stable_since.elapsed() > Duration::from_millis(1500) || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let recovery = match chaos {
        Some(task) => Some(task.await??),
        None => None,
    };

    let metrics = job_metrics(&args.api, id).await?;
    let (_, checkpoints) = http(&args.api, "GET", &format!("/jobs/{id}/checkpoints"), None).await?;
    let (_, job) = http(&args.api, "GET", &format!("/jobs/{id}"), None).await?;

    println!("\n### Processing (job {id}: {})\n", job["name"]);
    match consumed_secs {
        Some(secs) => println!(
            "Source consumed all {sent} events {secs:.2} s after ingest started: **{:.0} events/s end to end**{}.\n",
            sent as f64 / secs,
            if job["restarts"].as_u64().unwrap_or(0) > 0 {
                " (with a restart; stage counts below are for the attempt after it)"
            } else {
                ""
            }
        ),
        None => println!("The source did not consume every event within 5 minutes.\n"),
    }
    println!("| stage | records in | records out | late | backpressured ms |");
    println!("|---|---:|---:|---:|---:|");
    for m in &metrics {
        println!(
            "| `{}` | {} | {} | {} | {} |",
            m["name"].as_str().unwrap_or("?"),
            m["records_in"],
            m["records_out"],
            m["late_records"],
            m["backpressured_ms"]
        );
    }
    if let Some(latency) = metrics.last().map(|m| &m["latency_ms"]).filter(|l| !l.is_null()) {
        println!("\nSink latency (ingestion → sink): p50 ≤ {} ms, p95 ≤ {} ms, p99 ≤ {} ms.", latency["p50"], latency["p95"], latency["p99"]);
    }

    let records = checkpoints["records"].as_array().cloned().unwrap_or_default();
    let completed: Vec<&Value> = records.iter().filter(|r| r["status"]["state"] == "completed").collect();
    if !records.is_empty() {
        let mut durations: Vec<f64> = completed.iter().filter_map(|r| r["duration_ms"].as_f64()).collect();
        durations.sort_by(f64::total_cmp);
        let sizes: Vec<f64> = completed.iter().filter_map(|r| r["state_size"].as_f64()).collect();
        let uploaded: f64 = completed.iter().filter_map(|r| r["uploaded_bytes"].as_f64()).sum();
        let total: f64 = sizes.iter().sum();
        println!("\n### Checkpoints\n");
        println!("| completed | failed | duration p50 | duration p99 | max state | uploaded / referenced |");
        println!("|---:|---:|---:|---:|---:|---:|");
        println!(
            "| {} | {} | {:.0} ms | {:.0} ms | {:.1} KB | {:.0}% |",
            checkpoints["completed"],
            checkpoints["failed"],
            percentile(&durations, 0.5),
            percentile(&durations, 0.99),
            sizes.iter().cloned().fold(0.0, f64::max) / 1024.0,
            if total > 0.0 { uploaded / total * 100.0 } else { 0.0 }
        );
    }
    if let Some(r) = recovery {
        println!("\n### Recovery\n");
        println!("| killed stage | back to running | first checkpoint after restart | restarts |");
        println!("|---|---:|---:|---:|");
        println!(
            "| `{}` | {:.0} ms | {} | {} |",
            r.killed,
            r.detected_ms,
            r.checkpoint_ms.map_or("–".into(), |ms| format!("{ms:.0} ms")),
            job["restarts"]
        );
    }
    Ok(())
}
