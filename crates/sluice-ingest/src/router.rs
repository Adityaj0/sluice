//! Per-stream replayable logs.
//!
//! Exactly-once processing needs sources that can rewind to a checkpointed
//! position. Each stream is therefore an in-memory log (like a Kafka
//! partition): ingested events are appended with increasing offsets, every
//! subscribed job reads at its own position, and events are only discarded
//! once every subscriber has *committed* past them, which a checkpointed
//! job does when a checkpoint completes. A full log makes producers wait,
//! so backpressure reaches gRPC callers.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use sluice_core::Event;
use tokio::sync::Notify;

/// An event as it arrives off the wire: the payload is still undecoded.
pub type RawEvent = Event<Vec<u8>>;

/// Position of an event within its stream.
pub type Offset = u64;

/// Default per-stream cap on events kept for replay.
pub const DEFAULT_RETENTION: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    #[error("no running job consumes stream `{0}`")]
    NoSubscribers(String),
}

/// Routes ingested events into per-stream logs.
#[derive(Clone)]
pub struct EventRouter {
    streams: Arc<RwLock<HashMap<String, Arc<StreamLog>>>>,
    retention: usize,
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    pub name: String,
    pub subscribers: usize,
    /// Offset the next ingested event will get.
    pub next_offset: Offset,
    /// Events held for replay (not yet committed by every subscriber).
    pub retained: usize,
}

impl EventRouter {
    pub fn new() -> Self {
        Self::with_retention(DEFAULT_RETENTION)
    }

    /// `retention` caps the uncommitted events kept per stream; producers
    /// wait when it is reached.
    pub fn with_retention(retention: usize) -> Self {
        assert!(retention > 0, "retention must be positive");
        Self {
            streams: Arc::default(),
            retention,
        }
    }

    fn log(&self, stream: &str) -> Option<Arc<StreamLog>> {
        self.streams.read().unwrap_or_else(PoisonError::into_inner).get(stream).cloned()
    }

    /// Subscribes to `stream`, starting at its current end. Dropping the
    /// subscription releases everything it was holding back.
    pub fn subscribe(&self, stream: &str) -> Subscription {
        let log = Arc::clone(
            self.streams
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(stream.to_owned())
                .or_insert_with(|| Arc::new(StreamLog::new(self.retention))),
        );
        let id = log.subscribe();
        Subscription { log, id }
    }

    /// Appends `event` to `stream` and returns its offset. Waits while the
    /// stream's log is full.
    pub async fn route(&self, stream: &str, event: RawEvent) -> Result<Offset, RouteError> {
        match self.log(stream) {
            Some(log) => log.append(event).await.ok_or_else(|| RouteError::NoSubscribers(stream.to_owned())),
            None => Err(RouteError::NoSubscribers(stream.to_owned())),
        }
    }

    /// Appends a batch, waiting for space as needed, and returns the offset
    /// of its first event (the stream's next offset for an empty batch).
    pub async fn route_batch(&self, stream: &str, events: Vec<RawEvent>) -> Result<Offset, RouteError> {
        match self.log(stream) {
            Some(log) => log.append_batch(events).await.ok_or_else(|| RouteError::NoSubscribers(stream.to_owned())),
            None => Err(RouteError::NoSubscribers(stream.to_owned())),
        }
    }

    pub fn streams(&self) -> Vec<StreamInfo> {
        let streams = self.streams.read().unwrap_or_else(PoisonError::into_inner);
        let mut info: Vec<_> = streams
            .iter()
            .map(|(name, log)| {
                let state = log.lock();
                StreamInfo {
                    name: name.clone(),
                    subscribers: state.committed.len(),
                    next_offset: state.end(),
                    retained: state.events.len(),
                }
            })
            .collect();
        info.sort_by(|a, b| a.name.cmp(&b.name));
        info
    }
}

struct LogState {
    events: VecDeque<RawEvent>,
    /// Offset of `events[0]`.
    base: Offset,
    /// Committed offset per subscriber.
    committed: HashMap<u64, Offset>,
    next_subscriber: u64,
}

impl LogState {
    fn end(&self) -> Offset {
        self.base + self.events.len() as Offset
    }

    /// Drops events every subscriber has committed past. Returns whether
    /// anything was dropped.
    fn truncate(&mut self) -> bool {
        let keep_from = self.committed.values().copied().min().unwrap_or_else(|| self.end());
        let mut dropped = false;
        while self.base < keep_from && self.events.pop_front().is_some() {
            self.base += 1;
            dropped = true;
        }
        dropped
    }
}

/// One stream's log.
pub struct StreamLog {
    state: Mutex<LogState>,
    retention: usize,
    appended: Notify,
    space: Notify,
}

/// Result of reading at an offset.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadResult {
    Event(RawEvent),
    /// Nothing at this offset yet.
    Pending,
    /// The offset was already discarded; the oldest available one is given.
    Truncated(Offset),
}

impl StreamLog {
    fn new(retention: usize) -> Self {
        Self {
            state: Mutex::new(LogState {
                events: VecDeque::new(),
                base: 0,
                committed: HashMap::new(),
                next_subscriber: 0,
            }),
            retention,
            appended: Notify::new(),
            space: Notify::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, LogState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn subscribe(&self) -> u64 {
        let mut state = self.lock();
        let id = state.next_subscriber;
        state.next_subscriber += 1;
        let end = state.end();
        state.committed.insert(id, end);
        id
    }

    fn unsubscribe(&self, id: u64) {
        let mut state = self.lock();
        state.committed.remove(&id);
        if state.committed.is_empty() {
            // Nobody will ever read the backlog.
            let end = state.end();
            state.events.clear();
            state.base = end;
        } else {
            state.truncate();
        }
        drop(state);
        self.space.notify_waiters();
    }

    /// Appends, waiting for space. `None` if the stream has no subscribers.
    async fn append(&self, event: RawEvent) -> Option<Offset> {
        loop {
            let space = self.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            {
                let mut state = self.lock();
                if state.committed.is_empty() {
                    return None;
                }
                if state.events.len() < self.retention {
                    let offset = state.end();
                    state.events.push_back(event);
                    drop(state);
                    self.appended.notify_waiters();
                    return Some(offset);
                }
            }
            space.await;
        }
    }

    /// Appends as many events as fit under one lock, waiting for space
    /// between rounds. `None` if the stream has no subscribers.
    async fn append_batch(&self, events: Vec<RawEvent>) -> Option<Offset> {
        let mut events = events.into_iter().peekable();
        let mut first = None;
        loop {
            let space = self.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            {
                let mut state = self.lock();
                if state.committed.is_empty() {
                    return None;
                }
                first.get_or_insert(state.end());
                let free = self.retention.saturating_sub(state.events.len());
                let before = state.events.len();
                state.events.extend(events.by_ref().take(free));
                let appended = state.events.len() > before;
                drop(state);
                if appended {
                    self.appended.notify_waiters();
                }
            }
            if events.peek().is_none() {
                return first;
            }
            space.await;
        }
    }

    pub fn read(&self, offset: Offset) -> ReadResult {
        let state = self.lock();
        if offset < state.base {
            return ReadResult::Truncated(state.base);
        }
        match state.events.get((offset - state.base) as usize) {
            Some(event) => ReadResult::Event(event.clone()),
            None => ReadResult::Pending,
        }
    }

    /// Waits until an event exists at `offset` (or it was truncated).
    pub async fn read_wait(&self, offset: Offset) -> ReadResult {
        loop {
            let appended = self.appended.notified();
            tokio::pin!(appended);
            appended.as_mut().enable();
            match self.read(offset) {
                ReadResult::Pending => appended.await,
                other => return other,
            }
        }
    }
}

/// A job's registration on a stream: its committed offset holds back
/// truncation of the log.
pub struct Subscription {
    log: Arc<StreamLog>,
    id: u64,
}

impl Subscription {
    pub fn log(&self) -> &Arc<StreamLog> {
        &self.log
    }

    /// The offset this subscriber has committed up to: where a job without
    /// a checkpoint starts reading.
    pub fn committed(&self) -> Offset {
        let state = self.log.lock();
        state.committed.get(&self.id).copied().unwrap_or(state.base)
    }

    /// Releases events before `offset` (never moves backwards).
    pub fn commit(&self, offset: Offset) {
        let mut state = self.log.lock();
        let end = state.end();
        if let Some(committed) = state.committed.get_mut(&self.id) {
            *committed = (*committed).max(offset.min(end));
        }
        let freed = state.truncate();
        drop(state);
        if freed {
            self.log.space.notify_waiters();
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.log.unsubscribe(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn event(v: &str) -> RawEvent {
        Event::new(v.as_bytes().to_vec(), 1)
    }

    #[tokio::test]
    async fn subscribers_read_independently_and_replay_until_committed() {
        let router = EventRouter::new();
        let a = router.subscribe("s");
        let b = router.subscribe("s");
        assert_eq!(router.route("s", event("x")).await, Ok(0));
        assert_eq!(router.route("s", event("y")).await, Ok(1));

        for sub in [&a, &b] {
            assert!(matches!(sub.log().read(0), ReadResult::Event(e) if e.value == b"x"));
            assert!(matches!(sub.log().read(1), ReadResult::Event(e) if e.value == b"y"));
            assert_eq!(sub.log().read(2), ReadResult::Pending);
        }

        a.commit(2);
        assert_eq!(router.streams()[0].retained, 2, "b still holds both events");
        b.commit(1);
        assert_eq!(router.streams()[0].retained, 1);
        assert_eq!(a.log().read(0), ReadResult::Truncated(1));
        assert_eq!(b.committed(), 1);
    }

    #[tokio::test]
    async fn rejects_streams_without_subscribers() {
        let router = EventRouter::new();
        let err = router.route("nobody", event("x")).await;
        assert_eq!(err, Err(RouteError::NoSubscribers("nobody".into())));

        drop(router.subscribe("gone"));
        assert!(router.route("gone", event("x")).await.is_err());
        assert_eq!(router.streams()[0].subscribers, 0);
    }

    #[tokio::test]
    async fn full_log_blocks_producers_until_a_commit() {
        let router = EventRouter::with_retention(2);
        let sub = router.subscribe("s");
        router.route("s", event("1")).await.unwrap();
        router.route("s", event("2")).await.unwrap();

        let blocked = tokio::spawn({
            let router = router.clone();
            async move { router.route("s", event("3")).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished(), "log is full");

        sub.commit(1);
        assert_eq!(blocked.await.unwrap(), Ok(2));
        assert_eq!(sub.log().read(0), ReadResult::Truncated(1));
    }

    #[tokio::test]
    async fn batches_append_in_order_and_wait_for_space() {
        let router = EventRouter::with_retention(4);
        let sub = router.subscribe("s");
        assert_eq!(router.route_batch("s", vec![event("a"), event("b")]).await, Ok(0));
        assert_eq!(router.route_batch("s", vec![]).await, Ok(2));

        let blocked = tokio::spawn({
            let router = router.clone();
            async move { router.route_batch("s", (0..5).map(|i| event(&i.to_string())).collect()).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished(), "only 2 of the 5 fit");
        sub.commit(3); // frees 3 slots: the other 3 events fit
        assert_eq!(blocked.await.unwrap(), Ok(2));
        assert!(matches!(sub.log().read(6), ReadResult::Event(e) if e.value == b"4"));
    }

    #[tokio::test]
    async fn read_wait_wakes_on_append() {
        let router = EventRouter::new();
        let sub = router.subscribe("s");
        let log = Arc::clone(sub.log());
        let reader = tokio::spawn(async move { log.read_wait(0).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        router.route("s", event("late")).await.unwrap();
        assert!(matches!(reader.await.unwrap(), ReadResult::Event(e) if e.value == b"late"));
    }
}
