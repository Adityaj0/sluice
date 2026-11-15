//! Built-in data generators: synthetic purchase events streamed into the
//! engine, started and stopped from the UI or the API, so a demo needs no
//! external producer.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sluice_core::{Event, Timestamp, now_millis};
use sluice_ingest::{EventRouter, RawEvent, RouteError};
use serde::{Deserialize, Serialize};
use tokio::task::AbortHandle;

pub const MAX_RATE: f64 = 200_000.0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratorSpec {
    pub stream: String,
    /// Events per second.
    #[serde(default = "default_rate")]
    pub rate: f64,
    /// Distinct `user_id`s.
    #[serde(default = "default_users")]
    pub users: u64,
    /// Event times are up to this far in the past (out-of-order arrival).
    #[serde(default = "default_disorder")]
    pub max_disorder_ms: u64,
}

fn default_rate() -> f64 {
    1_000.0
}
fn default_users() -> u64 {
    50
}
fn default_disorder() -> u64 {
    1_000
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratorInfo {
    pub id: u64,
    #[serde(flatten)]
    pub spec: GeneratorSpec,
    pub sent: u64,
    pub started_at: Timestamp,
    /// No running job reads the stream, so events are not being accepted.
    pub waiting: bool,
}

struct Generator {
    spec: GeneratorSpec,
    sent: Arc<AtomicU64>,
    waiting: Arc<AtomicBool>,
    started_at: Timestamp,
    task: AbortHandle,
}

impl Generator {
    fn info(&self, id: u64) -> GeneratorInfo {
        GeneratorInfo {
            id,
            spec: self.spec.clone(),
            sent: self.sent.load(Ordering::Relaxed),
            started_at: self.started_at,
            waiting: self.waiting.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Generator {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct GeneratorManager {
    router: EventRouter,
    next_id: AtomicU64,
    generators: Mutex<BTreeMap<u64, Generator>>,
}

const ITEMS: [&str; 6] = ["coffee", "book", "headphones", "groceries", "shoes", "game"];

/// One synthetic purchase: `{"user_id", "amount", "item"}`.
pub fn purchase(seed: &mut u64, users: u64, now: Timestamp, max_disorder_ms: u64) -> RawEvent {
    let mut next = || {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    };
    let user = next() % users.max(1);
    let cents = next() % 20_000 + 100;
    let item = ITEMS[(next() % ITEMS.len() as u64) as usize];
    let jitter = if max_disorder_ms == 0 { 0 } else { next() % max_disorder_ms };
    let value = format!(r#"{{"user_id":"user-{user:03}","amount":{}.{:02},"item":"{item}"}}"#, cents / 100, cents % 100);
    let mut event = Event::new(value.into_bytes(), now.saturating_sub(jitter));
    event.key = Some(format!("user-{user:03}").into_bytes());
    event
}

impl GeneratorManager {
    pub fn new(router: EventRouter) -> Self {
        Self {
            router,
            next_id: AtomicU64::new(1),
            generators: Mutex::default(),
        }
    }

    pub fn start(&self, spec: GeneratorSpec) -> Result<GeneratorInfo, String> {
        if spec.stream.trim().is_empty() {
            return Err("`stream` must not be empty".into());
        }
        if !(spec.rate > 0.0 && spec.rate <= MAX_RATE) {
            return Err(format!("`rate` must be between 1 and {MAX_RATE} events per second"));
        }
        if spec.users == 0 || spec.users > 1_000_000 {
            return Err("`users` must be between 1 and 1,000,000".into());
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let sent = Arc::new(AtomicU64::new(0));
        let waiting = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(run(self.router.clone(), spec.clone(), id, Arc::clone(&sent), Arc::clone(&waiting)));
        let generator = Generator {
            spec,
            sent,
            waiting,
            started_at: now_millis(),
            task: task.abort_handle(),
        };
        let info = generator.info(id);
        tracing::info!(generator = id, stream = %info.spec.stream, rate = info.spec.rate, "generator started");
        self.generators.lock().unwrap_or_else(PoisonError::into_inner).insert(id, generator);
        Ok(info)
    }

    pub fn stop(&self, id: u64) -> Option<GeneratorInfo> {
        let generator = self.generators.lock().unwrap_or_else(PoisonError::into_inner).remove(&id)?;
        tracing::info!(generator = id, "generator stopped");
        Some(generator.info(id))
    }

    pub fn list(&self) -> Vec<GeneratorInfo> {
        let generators = self.generators.lock().unwrap_or_else(PoisonError::into_inner);
        generators.iter().map(|(id, g)| g.info(*id)).collect()
    }
}

async fn run(router: EventRouter, spec: GeneratorSpec, id: u64, sent: Arc<AtomicU64>, waiting: Arc<AtomicBool>) {
    let mut seed = 0x9E37_79B9_7F4A_7C15 ^ id;
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    let mut last = Instant::now();
    let mut owed = 0.0;
    loop {
        ticker.tick().await;
        owed += spec.rate * last.elapsed().as_secs_f64();
        last = Instant::now();
        let count = owed.floor() as u64;
        if count == 0 {
            continue;
        }
        owed -= count as f64;
        let now = now_millis();
        let events = (0..count).map(|_| purchase(&mut seed, spec.users, now, spec.max_disorder_ms)).collect();
        match router.route_batch(&spec.stream, events).await {
            Ok(_) => {
                sent.fetch_add(count, Ordering::Relaxed);
                waiting.store(false, Ordering::Relaxed);
            }
            Err(RouteError::NoSubscribers(_)) => {
                // Nobody reads the stream yet: drop this tick's events and
                // keep going, so the generator starts feeding as soon as a
                // job is deployed.
                waiting.store(true, Ordering::Relaxed);
                owed = 0.0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn purchases_are_valid_json_with_bounded_users_and_disorder() {
        let mut seed = 7;
        for _ in 0..1000 {
            let event = purchase(&mut seed, 5, 10_000, 500);
            let value: serde_json::Value = serde_json::from_slice(&event.value).unwrap();
            let user = value["user_id"].as_str().unwrap();
            assert!(["user-000", "user-001", "user-002", "user-003", "user-004"].contains(&user));
            assert!(value["amount"].as_f64().unwrap() >= 1.0);
            assert!(event.event_time > 9_500 && event.event_time <= 10_000);
            assert_eq!(event.key_bytes(), user.as_bytes());
        }
    }

    #[tokio::test]
    async fn generators_feed_subscribed_streams_and_wait_otherwise() {
        let router = EventRouter::new();
        let manager = GeneratorManager::new(router.clone());
        assert!(manager.start(GeneratorSpec { stream: "s".into(), rate: 0.0, users: 1, max_disorder_ms: 0 }).is_err());

        let info = manager
            .start(GeneratorSpec { stream: "s".into(), rate: 2_000.0, users: 10, max_disorder_ms: 0 })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(manager.list()[0].waiting, "no job reads `s` yet");

        let sub = router.subscribe("s");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let listed = manager.list().remove(0);
        assert!(!listed.waiting);
        assert!(listed.sent > 200, "{listed:?}");
        assert!(sub.log().read(0) != sluice_ingest::ReadResult::Pending);

        let stopped = manager.stop(info.id).unwrap();
        assert!(manager.list().is_empty());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let offset = router.streams()[0].next_offset;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(router.streams()[0].next_offset, offset, "stopped generators stop sending");
        assert!(stopped.sent > 0);
    }
}
