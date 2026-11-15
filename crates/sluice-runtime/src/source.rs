//! Sources and source-side watermark generation.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sluice_core::{BoundedOutOfOrderness, CheckpointBarrier, Event, StreamElement, StreamStatus, Timestamp};
use tokio::sync::{Notify, mpsc};
use tokio::time::Instant;

use crate::channel::Sender;
use crate::metrics::OperatorMetrics;

/// What a [`SourceFunction`] produces.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceItem<T> {
    Record(Event<T>),
    /// An explicit watermark, forwarded as-is (for scripted sources).
    Watermark(Timestamp),
}

/// A replayable input.
///
/// For exactly-once processing a source must be able to rewind: a
/// checkpoint records [`position`](Self::position), and recovery calls
/// [`seek`](Self::seek) with it on a freshly built source.
pub trait SourceFunction<T>: Send + 'static {
    /// The next item, or `None` once the input is exhausted.
    ///
    /// Must be cancel-safe: if the returned future is dropped before it
    /// completes, no item may be lost.
    fn next(&mut self) -> impl Future<Output = Option<SourceItem<T>>> + Send;

    /// Position just past the last item returned.
    fn position(&self) -> u64;

    /// Resume reading at `position`.
    fn seek(&mut self, position: u64);

    /// Everything before `position` is covered by a completed checkpoint
    /// (or, without checkpointing, has been read) and will never be
    /// replayed; the source may release it.
    fn commit(&mut self, _position: u64) {}
}

/// Reads an iterator, optionally rate-limited. Seeking skips items, so the
/// iterator must yield the same sequence every time the job is built.
pub struct IterSource<I> {
    iter: I,
    position: u64,
    rate: Option<f64>,
    started: Option<Instant>,
}

impl<I> IterSource<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            position: 0,
            rate: None,
            started: None,
        }
    }

    /// Emits at most `events_per_sec` (useful for demos and tests that need
    /// checkpoints to happen mid-stream).
    pub fn with_rate(mut self, events_per_sec: f64) -> Self {
        assert!(events_per_sec > 0.0, "rate must be positive");
        self.rate = Some(events_per_sec);
        self
    }
}

impl<T, I> SourceFunction<T> for IterSource<I>
where
    T: Send + 'static,
    I: Iterator<Item = Event<T>> + Send + 'static,
{
    async fn next(&mut self) -> Option<SourceItem<T>> {
        if let Some(rate) = self.rate {
            let started = *self.started.get_or_insert_with(Instant::now);
            // Rate-limit in small batches; sleeping before pulling from the
            // iterator keeps this cancel-safe.
            if self.position % 16 == 0 {
                let due = started + Duration::from_secs_f64(self.position as f64 / rate);
                tokio::time::sleep_until(due).await;
            }
        }
        let event = self.iter.next()?;
        self.position += 1;
        Some(SourceItem::Record(event))
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn seek(&mut self, position: u64) {
        while self.position < position && self.iter.next().is_some() {
            self.position += 1;
        }
        self.started = None;
    }
}

/// Replays a scripted sequence of records and watermarks.
pub struct ElementsSource<T> {
    elements: VecDeque<StreamElement<T>>,
    position: u64,
}

impl<T> ElementsSource<T> {
    pub fn new(elements: Vec<StreamElement<T>>) -> Self {
        Self {
            elements: elements.into(),
            position: 0,
        }
    }
}

impl<T: Send + 'static> SourceFunction<T> for ElementsSource<T> {
    async fn next(&mut self) -> Option<SourceItem<T>> {
        loop {
            let element = self.elements.pop_front()?;
            self.position += 1;
            match element {
                StreamElement::Record(event) => return Some(SourceItem::Record(event)),
                StreamElement::Watermark(wm) => return Some(SourceItem::Watermark(wm)),
                StreamElement::Barrier(_) | StreamElement::Status(_) => continue,
                StreamElement::End => {
                    self.elements.clear();
                    return None;
                }
            }
        }
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn seek(&mut self, position: u64) {
        while self.position < position && self.elements.pop_front().is_some() {
            self.position += 1;
        }
    }
}

/// Reads from a channel. Not replayable: after a restart, events consumed
/// before the failure are gone.
pub struct ChannelSource<T> {
    rx: mpsc::Receiver<Event<T>>,
    position: u64,
}

impl<T> ChannelSource<T> {
    pub fn new(rx: mpsc::Receiver<Event<T>>) -> Self {
        Self { rx, position: 0 }
    }
}

impl<T: Send + 'static> SourceFunction<T> for ChannelSource<T> {
    async fn next(&mut self) -> Option<SourceItem<T>> {
        let event = self.rx.recv().await?;
        self.position += 1;
        Some(SourceItem::Record(event))
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn seek(&mut self, position: u64) {
        if position > 0 {
            tracing::warn!(position, "channel sources cannot replay; resuming at the current position");
        }
    }
}

/// How a source derives watermarks from the event times it sees.
#[derive(Debug, Clone)]
pub struct WatermarkStrategy {
    /// How far (ms) behind the highest event time seen so far an event may
    /// still arrive. `None` disables generated watermarks: windows then only
    /// fire when the stream ends (or on explicit watermarks).
    pub max_out_of_orderness: Option<Timestamp>,
    /// Emit a watermark after this many records.
    pub emit_every: usize,
    /// Also emit on this timer, so event time keeps moving when traffic is
    /// too light to reach `emit_every`.
    pub interval: Duration,
    /// Declare the source idle after this long without records, so it stops
    /// holding back the watermark of downstream multi-input operators.
    pub idle_timeout: Option<Duration>,
    /// Watermark alignment: sources in the same group pause reading while
    /// their watermark is more than `max_drift` ms ahead of the slowest
    /// member's, which bounds the state a join or union must buffer.
    pub alignment: Option<(String, Timestamp)>,
}

impl WatermarkStrategy {
    pub fn bounded_out_of_orderness(max_out_of_orderness_ms: Timestamp) -> Self {
        Self {
            max_out_of_orderness: Some(max_out_of_orderness_ms),
            emit_every: 1_000,
            interval: Duration::from_millis(200),
            idle_timeout: None,
            alignment: None,
        }
    }

    /// For sources whose event times never decrease.
    pub fn monotonous() -> Self {
        Self::bounded_out_of_orderness(0)
    }

    pub fn none() -> Self {
        Self {
            max_out_of_orderness: None,
            ..Self::monotonous()
        }
    }

    pub fn with_emit_every(mut self, records: usize) -> Self {
        self.emit_every = records.max(1);
        self
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_idleness(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self.interval = self.interval.min(timeout);
        self
    }

    pub fn with_alignment(mut self, group: impl Into<String>, max_drift: Duration) -> Self {
        self.alignment = Some((group.into(), max_drift.as_millis() as Timestamp));
        self
    }
}

/// Sources sharing a watermark-alignment group.
pub(crate) struct AlignmentGroup {
    max_drift: Timestamp,
    /// Each member's watermark; `None` while it has none, is idle or has
    /// finished (it then constrains nobody).
    members: Mutex<Vec<Option<Timestamp>>>,
    pub(crate) changed: Notify,
}

impl AlignmentGroup {
    pub(crate) fn new(max_drift: Timestamp) -> Self {
        Self {
            max_drift,
            members: Mutex::default(),
            changed: Notify::new(),
        }
    }

    pub(crate) fn join(self: &Arc<Self>) -> AlignmentMember {
        let mut members = self.members.lock().unwrap_or_else(PoisonError::into_inner);
        members.push(None);
        AlignmentMember {
            group: Arc::clone(self),
            id: members.len() - 1,
            reported: None,
        }
    }
}

/// One source's membership; leaves the group when dropped.
pub(crate) struct AlignmentMember {
    pub(crate) group: Arc<AlignmentGroup>,
    id: usize,
    reported: Option<Timestamp>,
}

impl AlignmentMember {
    pub(crate) fn report(&mut self, watermark: Option<Timestamp>) {
        if self.reported == watermark {
            return;
        }
        self.reported = watermark;
        self.group.members.lock().unwrap_or_else(PoisonError::into_inner)[self.id] = watermark;
        self.group.changed.notify_waiters();
    }

    /// Whether this source is too far ahead of the slowest other member.
    pub(crate) fn should_pause(&self) -> bool {
        let Some(own) = self.reported else { return false };
        let members = self.group.members.lock().unwrap_or_else(PoisonError::into_inner);
        let slowest = members
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != self.id)
            .filter_map(|(_, wm)| *wm)
            .min();
        slowest.is_some_and(|slowest| own > slowest.saturating_add(self.group.max_drift))
    }
}

impl Drop for AlignmentMember {
    fn drop(&mut self) {
        self.report(None);
    }
}

impl Default for WatermarkStrategy {
    fn default() -> Self {
        Self::monotonous()
    }
}

/// Sends a source's output downstream, interleaving watermarks according to
/// the strategy. Every async method returns `false` once the downstream
/// stage is gone.
pub(crate) struct Emitter<T> {
    tx: Sender<StreamElement<T>>,
    idle: bool,
    generator: Option<BoundedOutOfOrderness>,
    last_emitted: Option<Timestamp>,
    since_last: usize,
    emit_every: usize,
    metrics: Arc<OperatorMetrics>,
}

impl<T> Emitter<T> {
    pub(crate) fn new(tx: Sender<StreamElement<T>>, strategy: &WatermarkStrategy, metrics: Arc<OperatorMetrics>) -> Self {
        Self {
            tx,
            idle: false,
            generator: strategy.max_out_of_orderness.map(BoundedOutOfOrderness::new),
            last_emitted: None,
            since_last: 0,
            emit_every: strategy.emit_every.max(1),
            metrics,
        }
    }

    /// `(max event time seen, last watermark emitted)`, for checkpoints.
    pub(crate) fn snapshot(&self) -> (Option<Timestamp>, Option<Timestamp>) {
        (self.generator.as_ref().and_then(|g| g.max_seen()), self.last_emitted)
    }

    pub(crate) fn restore(&mut self, max_seen: Option<Timestamp>, last_emitted: Option<Timestamp>) {
        if let Some(generator) = self.generator.as_mut() {
            generator.restore(max_seen);
        }
        self.last_emitted = last_emitted;
        if let Some(wm) = last_emitted {
            self.metrics.set_watermark(wm);
        }
    }

    pub(crate) fn last_emitted(&self) -> Option<Timestamp> {
        self.last_emitted
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.idle
    }

    /// Announces an idle/active transition downstream.
    pub(crate) async fn set_idle(&mut self, idle: bool) -> bool {
        if self.idle == idle {
            return true;
        }
        self.idle = idle;
        self.metrics.set_idle(idle);
        let status = if idle { StreamStatus::Idle } else { StreamStatus::Active };
        self.tx.send(StreamElement::Status(status)).await.is_ok()
    }

    pub(crate) async fn record(&mut self, event: Event<T>) -> bool {
        if self.idle && !self.set_idle(false).await {
            return false;
        }
        if let Some(generator) = self.generator.as_mut() {
            generator.on_event(event.event_time);
        }
        self.metrics.add_in(1);
        if self.tx.send(StreamElement::Record(event)).await.is_err() {
            return false;
        }
        self.metrics.add_out(1);
        self.since_last += 1;
        if self.since_last >= self.emit_every {
            self.watermark().await
        } else {
            true
        }
    }

    /// Emits the generated watermark if it advanced.
    pub(crate) async fn watermark(&mut self) -> bool {
        self.since_last = 0;
        match self.generator.as_ref().and_then(|g| g.current_watermark()) {
            Some(wm) => self.explicit_watermark(wm).await,
            None => true,
        }
    }

    pub(crate) async fn explicit_watermark(&mut self, wm: Timestamp) -> bool {
        if self.last_emitted.is_some_and(|last| wm <= last) {
            return true;
        }
        self.last_emitted = Some(wm);
        self.metrics.set_watermark(wm);
        self.tx.send(StreamElement::Watermark(wm)).await.is_ok()
    }

    pub(crate) async fn barrier(&mut self, barrier: CheckpointBarrier) -> bool {
        self.tx.send(StreamElement::Barrier(barrier)).await.is_ok()
    }

    pub(crate) async fn finish(self) {
        let _ = self.tx.send(StreamElement::End).await;
    }
}
