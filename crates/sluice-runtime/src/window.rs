//! Event-time windowing.
//!
//! - [`TumblingWindows`] and [`SlidingWindows`] are evaluated with *panes*:
//!   time is cut into slices of `gcd(size, slide)`, each event updates the
//!   accumulator of exactly one pane, and a window's result merges its
//!   panes. A sliding window of 1h every 1m costs one state update per
//!   event instead of sixty.
//! - [`SessionWindows`] group a key's events separated by less than a gap;
//!   sessions merge as events bridge them.
//!
//! Accumulators live in keyed state (RocksDB or memory) and are
//! checkpointed with the job. A window fires once the watermark passes its
//! end. With *allowed lateness* its state is kept a while longer: a late
//! event within it updates the window and re-emits the result
//! (`late_firing`), and anything later is dropped and counted, or routed to
//! a side output ([`WithLateOutput`]).

use std::collections::BTreeMap;
use std::time::Duration;

use sluice_core::{
    Context, Either, Event, KeyedState, Operator, OperatorError, Timestamp, decode, encode, now_millis,
    prefix_successor,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Half-open interval `[start, end)` of event time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl TimeWindow {
    /// The last timestamp inside the window. The window fires once the
    /// watermark reaches it.
    pub fn max_timestamp(&self) -> Timestamp {
        self.end - 1
    }
}

fn floor_to(x: i128, step: i128) -> i128 {
    x.div_euclid(step) * step
}

fn ceil_to(x: i128, step: i128) -> i128 {
    -floor_to(-x, step)
}

fn gcd(a: Timestamp, b: Timestamp) -> Timestamp {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// An exclusive upper bound on 8-byte big-endian prefixed keys for "prefix
/// below `x`": `None` means unbounded.
fn below(x: i128) -> Option<[u8; 8]> {
    if x > u64::MAX as i128 {
        None
    } else {
        Some((x.max(0) as u64).to_be_bytes())
    }
}

fn clamp_ts(x: i128) -> Timestamp {
    x.clamp(0, u64::MAX as i128) as Timestamp
}

/// Fixed-size, overlapping windows: every `slide`, a window of `size`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlidingWindows {
    size: Timestamp,
    slide: Timestamp,
}

impl SlidingWindows {
    pub fn of(size: Duration, slide: Duration) -> Self {
        Self::of_millis(size.as_millis() as Timestamp, slide.as_millis() as Timestamp)
    }

    pub fn of_millis(size_ms: Timestamp, slide_ms: Timestamp) -> Self {
        assert!(size_ms > 0 && slide_ms > 0, "window size and slide must be positive");
        assert!(slide_ms <= size_ms, "slide larger than size would leave gaps between windows");
        Self {
            size: size_ms,
            slide: slide_ms,
        }
    }

    pub fn size_millis(&self) -> Timestamp {
        self.size
    }

    pub fn slide_millis(&self) -> Timestamp {
        self.slide
    }

    /// Width of the panes windows are assembled from.
    pub fn pane_millis(&self) -> Timestamp {
        gcd(self.size, self.slide)
    }

    /// Every window containing `t`, earliest first.
    pub fn assign(&self, t: Timestamp, out: &mut Vec<TimeWindow>) {
        let (t, size, slide) = (t as i128, self.size as i128, self.slide as i128);
        let mut start = ceil_to(t - size + 1, slide).max(0);
        while start <= t {
            out.push(TimeWindow {
                start: start as Timestamp,
                end: clamp_ts(start + size),
            });
            start += slide;
        }
    }
}

/// Fixed-size, non-overlapping windows aligned to the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TumblingWindows {
    size: Timestamp,
}

impl TumblingWindows {
    pub fn of(size: Duration) -> Self {
        Self::of_millis(size.as_millis() as Timestamp)
    }

    pub fn of_millis(size_ms: Timestamp) -> Self {
        assert!(size_ms > 0, "window size must be positive");
        Self { size: size_ms }
    }

    pub fn size_millis(&self) -> Timestamp {
        self.size
    }

    /// Every window containing `t` (exactly one).
    pub fn assign(&self, t: Timestamp, out: &mut Vec<TimeWindow>) {
        SlidingWindows::from(*self).assign(t, out)
    }
}

impl From<TumblingWindows> for SlidingWindows {
    fn from(tumbling: TumblingWindows) -> Self {
        SlidingWindows::of_millis(tumbling.size, tumbling.size)
    }
}

/// Per-key activity sessions: a session closes after `gap` without events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionWindows {
    gap: Timestamp,
}

impl SessionWindows {
    pub fn with_gap(gap: Duration) -> Self {
        Self::with_gap_millis(gap.as_millis() as Timestamp)
    }

    pub fn with_gap_millis(gap_ms: Timestamp) -> Self {
        assert!(gap_ms > 0, "session gap must be positive");
        Self { gap: gap_ms }
    }

    pub fn gap_millis(&self) -> Timestamp {
        self.gap
    }
}

/// Incremental aggregation over the events of one window and key. The
/// accumulator is serialized into keyed state between events.
pub trait AggregateFunction<In>: Send + 'static {
    type Acc: Serialize + DeserializeOwned + Send + 'static;
    type Out: Send + 'static;

    fn create_accumulator(&self) -> Self::Acc;
    fn add(&self, acc: &mut Self::Acc, value: &In);
    /// Combines two partial aggregates (pane assembly, session merging).
    fn merge(&self, a: Self::Acc, b: Self::Acc) -> Self::Acc;
    fn result(&self, acc: Self::Acc) -> Self::Out;
}

/// Number of events.
#[derive(Debug, Clone, Copy, Default)]
pub struct Count;

impl<In> AggregateFunction<In> for Count {
    type Acc = u64;
    type Out = u64;

    fn create_accumulator(&self) -> u64 {
        0
    }
    fn add(&self, acc: &mut u64, _value: &In) {
        *acc += 1;
    }
    fn merge(&self, a: u64, b: u64) -> u64 {
        a + b
    }
    fn result(&self, acc: u64) -> u64 {
        acc
    }
}

/// Sum of a numeric field.
#[derive(Debug, Clone, Copy)]
pub struct Sum<F>(pub F);

impl<In, F> AggregateFunction<In> for Sum<F>
where
    F: Fn(&In) -> f64 + Send + 'static,
{
    type Acc = f64;
    type Out = f64;

    fn create_accumulator(&self) -> f64 {
        0.0
    }
    fn add(&self, acc: &mut f64, value: &In) {
        *acc += (self.0)(value);
    }
    fn merge(&self, a: f64, b: f64) -> f64 {
        a + b
    }
    fn result(&self, acc: f64) -> f64 {
        acc
    }
}

/// Mean of a numeric field.
#[derive(Debug, Clone, Copy)]
pub struct Avg<F>(pub F);

impl<In, F> AggregateFunction<In> for Avg<F>
where
    F: Fn(&In) -> f64 + Send + 'static,
{
    type Acc = (f64, u64);
    type Out = f64;

    fn create_accumulator(&self) -> (f64, u64) {
        (0.0, 0)
    }
    fn add(&self, acc: &mut (f64, u64), value: &In) {
        acc.0 += (self.0)(value);
        acc.1 += 1;
    }
    fn merge(&self, a: (f64, u64), b: (f64, u64)) -> (f64, u64) {
        (a.0 + b.0, a.1 + b.1)
    }
    fn result(&self, (sum, n): (f64, u64)) -> f64 {
        if n == 0 { 0.0 } else { sum / n as f64 }
    }
}

/// The aggregated result of one window for one key.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WindowResult<T> {
    pub key: Vec<u8>,
    pub window: TimeWindow,
    pub value: T,
    /// An updated result caused by a late event within the allowed
    /// lateness, replacing an earlier result for the same window and key.
    pub late_firing: bool,
}

impl<T> WindowResult<T> {
    pub fn key_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.key)
    }
}

/// Window operators can hand over the events they reject as too late.
pub trait LateEvents<In> {
    /// Keep late events for [`take_late_events`](Self::take_late_events)
    /// instead of dropping them.
    fn keep_late_events(&mut self);
    fn take_late_events(&mut self, out: &mut Vec<Event<In>>);
}

/// A window definition that knows how to build its operator.
pub trait WindowSpec: Clone + Send + 'static {
    type Operator<In: Send + 'static, Agg: AggregateFunction<In>>: Operator<In = In, Out = WindowResult<Agg::Out>>
        + LateEvents<In>;

    fn operator<In: Send + 'static, Agg: AggregateFunction<In>>(
        &self,
        aggregate: Agg,
        allowed_lateness: Timestamp,
    ) -> Self::Operator<In, Agg>;
}

impl WindowSpec for SlidingWindows {
    type Operator<In: Send + 'static, Agg: AggregateFunction<In>> = WindowOperator<In, Agg>;

    fn operator<In: Send + 'static, Agg: AggregateFunction<In>>(&self, aggregate: Agg, lateness: Timestamp) -> WindowOperator<In, Agg> {
        WindowOperator::new(*self, aggregate).with_allowed_lateness(lateness)
    }
}

impl WindowSpec for TumblingWindows {
    type Operator<In: Send + 'static, Agg: AggregateFunction<In>> = WindowOperator<In, Agg>;

    fn operator<In: Send + 'static, Agg: AggregateFunction<In>>(&self, aggregate: Agg, lateness: Timestamp) -> WindowOperator<In, Agg> {
        WindowOperator::new(*self, aggregate).with_allowed_lateness(lateness)
    }
}

impl WindowSpec for SessionWindows {
    type Operator<In: Send + 'static, Agg: AggregateFunction<In>> = SessionWindowOperator<In, Agg>;

    fn operator<In: Send + 'static, Agg: AggregateFunction<In>>(
        &self,
        aggregate: Agg,
        lateness: Timestamp,
    ) -> SessionWindowOperator<In, Agg> {
        SessionWindowOperator::new(*self, aggregate).with_allowed_lateness(lateness)
    }
}

/// Keyed state holding one accumulator per (pane, key).
pub const PANE_STATE: &str = "window-panes";

/// `pane end ++ pane start ++ key`, big-endian, so keys sort by pane end:
/// every pane of a window is one contiguous range per end, and expired
/// panes are a prefix.
fn pane_key(pane: TimeWindow, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + key.len());
    out.extend_from_slice(&pane.end.to_be_bytes());
    out.extend_from_slice(&pane.start.to_be_bytes());
    out.extend_from_slice(key);
    out
}

fn parse_pane_key(bytes: &[u8]) -> Result<(TimeWindow, &[u8]), OperatorError> {
    if bytes.len() < 16 {
        return Err(OperatorError::Other(format!("corrupt window state key ({} bytes)", bytes.len())));
    }
    let end = u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes"));
    let start = u64::from_be_bytes(bytes[8..16].try_into().expect("8 bytes"));
    Ok((TimeWindow { start, end }, &bytes[16..]))
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PaneOperatorState {
    watermark: Option<Timestamp>,
    /// End of the last window fired on time.
    fired_through: Option<Timestamp>,
    late: u64,
}

fn emit_result<Out>(ctx: &mut Context<'_, WindowResult<Out>>, window: TimeWindow, key: Vec<u8>, value: Out, late_firing: bool) {
    ctx.emit(Event {
        key: Some(key.clone()),
        value: WindowResult {
            key,
            window,
            value,
            late_firing,
        },
        event_time: window.max_timestamp(),
        ingestion_time: now_millis(),
    });
}

/// Tumbling and sliding window aggregation over panes in keyed state.
///
/// The current watermark and the last fired window are checkpointed with
/// the panes, so a restored operator neither re-fires windows nor treats
/// late events differently from the one that failed.
pub struct WindowOperator<In, Agg> {
    windows: SlidingWindows,
    pane: Timestamp,
    allowed_lateness: Timestamp,
    aggregate: Agg,
    state: PaneOperatorState,
    keep_late: bool,
    late_events: Vec<Event<In>>,
    scratch: Vec<TimeWindow>,
}

impl<In, Agg> WindowOperator<In, Agg>
where
    Agg: AggregateFunction<In>,
{
    pub fn new(windows: impl Into<SlidingWindows>, aggregate: Agg) -> Self {
        let windows = windows.into();
        Self {
            pane: windows.pane_millis(),
            windows,
            allowed_lateness: 0,
            aggregate,
            state: PaneOperatorState::default(),
            keep_late: false,
            late_events: Vec::new(),
            scratch: Vec::new(),
        }
    }

    pub fn with_allowed_lateness(mut self, lateness_ms: Timestamp) -> Self {
        self.allowed_lateness = lateness_ms;
        self
    }

    fn pane_of(&self, t: Timestamp) -> TimeWindow {
        let start = t - t % self.pane;
        TimeWindow {
            start,
            end: start.saturating_add(self.pane),
        }
    }

    fn merge_into(&self, slot: &mut Option<Agg::Acc>, acc: Agg::Acc) {
        *slot = Some(match slot.take() {
            Some(existing) => self.aggregate.merge(existing, acc),
            None => acc,
        });
    }

    /// One key's accumulator for `window`, merged from its panes.
    fn window_acc(&self, state: &dyn KeyedState, window: TimeWindow, key: &[u8]) -> Result<Option<Agg::Acc>, OperatorError> {
        let mut acc = None;
        let mut start = window.start;
        while start < window.end {
            let pane = TimeWindow {
                start,
                end: start.saturating_add(self.pane),
            };
            if let Some(bytes) = state.get(PANE_STATE, &pane_key(pane, key))? {
                self.merge_into(&mut acc, decode(&bytes)?);
            }
            start = pane.end;
        }
        Ok(acc)
    }

    /// Emits every key's result for `window`, in key order.
    fn fire(&self, window: TimeWindow, ctx: &mut Context<'_, WindowResult<Agg::Out>>) -> Result<(), OperatorError> {
        let from = window.start.saturating_add(self.pane).to_be_bytes();
        let to = below(window.end as i128 + 1);
        let panes = ctx.state().scan(PANE_STATE, &from, to.as_ref().map(|b| &b[..]))?;
        let mut by_key: BTreeMap<Vec<u8>, Option<Agg::Acc>> = BTreeMap::new();
        for (pane, acc) in panes {
            let (_, key) = parse_pane_key(&pane)?;
            let slot = by_key.entry(key.to_vec()).or_default();
            self.merge_into(slot, decode(&acc)?);
        }
        for (key, acc) in by_key {
            if let Some(acc) = acc {
                emit_result(ctx, window, key, self.aggregate.result(acc), false);
            }
        }
        Ok(())
    }
}

impl<In, Agg> LateEvents<In> for WindowOperator<In, Agg> {
    fn keep_late_events(&mut self) {
        self.keep_late = true;
    }
    fn take_late_events(&mut self, out: &mut Vec<Event<In>>) {
        out.append(&mut self.late_events);
    }
}

impl<In, Agg> Operator for WindowOperator<In, Agg>
where
    In: Send + 'static,
    Agg: AggregateFunction<In>,
{
    type In = In;
    type Out = WindowResult<Agg::Out>;

    fn state_names(&self) -> &'static [&'static str] {
        &[PANE_STATE]
    }

    fn process(&mut self, event: Event<In>, ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError> {
        let t = event.event_time;
        let pane = self.pane_of(t);
        let (size, slide) = (self.windows.size as i128, self.windows.slide as i128);
        let watermark = self.state.watermark.map(|wm| wm as i128);
        let lateness = self.allowed_lateness as i128;

        // Too late even for the last window containing it?
        let last_window_end = floor_to(pane.start as i128, slide) + size;
        if watermark.is_some_and(|wm| last_window_end - 1 + lateness <= wm) {
            self.state.late += 1;
            if self.keep_late {
                self.late_events.push(event);
            }
            return Ok(());
        }

        let state_key = pane_key(pane, event.key_bytes());
        let mut acc = match ctx.state().get(PANE_STATE, &state_key)? {
            Some(bytes) => decode(&bytes)?,
            None => self.aggregate.create_accumulator(),
        };
        self.aggregate.add(&mut acc, &event.value);
        ctx.state().put(PANE_STATE, &state_key, &encode(&acc)?)?;

        // Late but within the allowed lateness: re-fire, for this key, the
        // windows that already fired and have not expired.
        if let Some(wm) = watermark {
            self.scratch.clear();
            self.windows.assign(t, &mut self.scratch);
            let windows = std::mem::take(&mut self.scratch);
            for window in &windows {
                let max = window.max_timestamp() as i128;
                if max <= wm && max + lateness > wm {
                    if let Some(acc) = self.window_acc(ctx.state(), *window, event.key_bytes())? {
                        emit_result(ctx, *window, event.key_bytes().to_vec(), self.aggregate.result(acc), true);
                    }
                }
            }
            self.scratch = windows;
        }
        Ok(())
    }

    fn on_watermark(&mut self, watermark: Timestamp, ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError> {
        self.state.watermark = Some(watermark);
        let (size, slide) = (self.windows.size as i128, self.windows.slide as i128);
        let wm = watermark as i128;

        // Fire due windows in order, jumping over stretches without data:
        // the next window to fire is the first one, after the last fired,
        // that contains the earliest pane still relevant to it.
        loop {
            let lower = match self.state.fired_through {
                Some(fired) => fired as i128 + slide - size + 1,
                None => 0,
            };
            if lower > u64::MAX as i128 {
                break;
            }
            let from = clamp_ts(lower).to_be_bytes();
            let Some((first, _)) = ctx.state().first(PANE_STATE, &from, None)? else {
                break;
            };
            let (first_pane, _) = parse_pane_key(&first)?;
            let mut end = ceil_to(first_pane.end as i128 - size, slide).max(0) + size;
            if let Some(fired) = self.state.fired_through {
                end = end.max(fired as i128 + slide);
            }
            if end - 1 > wm {
                break;
            }
            let window = TimeWindow {
                start: clamp_ts(end - size),
                end: clamp_ts(end),
            };
            self.fire(window, ctx)?;
            self.state.fired_through = Some(window.end);
        }

        // Drop panes whose last window has passed the allowed lateness: a
        // pane's last window starts at floor(pane start, slide), so it is
        // expired when pane start < floor(wm + 1 - size - lateness, slide) +
        // slide, i.e. pane end <= that cutoff.
        let cutoff = floor_to(wm + 1 - size - self.allowed_lateness as i128, slide) + slide;
        let bound = below(cutoff + 1);
        if cutoff > 0 {
            ctx.state().delete_range(PANE_STATE, &[], bound.as_ref().map(|b| &b[..]))?;
        }
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>, OperatorError> {
        Ok(encode(&self.state)?)
    }

    fn restore_state(&mut self, state: &[u8]) -> Result<(), OperatorError> {
        self.state = decode(state)?;
        Ok(())
    }

    fn late_records(&self) -> u64 {
        self.state.late
    }
}

/// Sessions by key: `len(key) ++ key ++ start`.
pub const SESSION_STATE: &str = "sessions";
/// Session deadlines (fire, then purge): `deadline ++ len(key) ++ key ++ start`.
pub const SESSION_TIMERS: &str = "session-timers";

fn session_prefix(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len() + 8);
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

fn session_key(key: &[u8], start: Timestamp) -> Vec<u8> {
    let mut out = session_prefix(key);
    out.extend_from_slice(&start.to_be_bytes());
    out
}

fn timer_key(deadline: Timestamp, key: &[u8], start: Timestamp) -> Vec<u8> {
    let mut out = deadline.to_be_bytes().to_vec();
    out.extend_from_slice(&session_key(key, start));
    out
}

/// `(key, start)` from a session key.
fn parse_session_key(bytes: &[u8]) -> Result<(&[u8], Timestamp), OperatorError> {
    let corrupt = || OperatorError::Other(format!("corrupt session state key ({} bytes)", bytes.len()));
    let len = u32::from_be_bytes(bytes.get(..4).ok_or_else(corrupt)?.try_into().expect("4 bytes")) as usize;
    let key = bytes.get(4..4 + len).ok_or_else(corrupt)?;
    let start = bytes.get(4 + len..12 + len).ok_or_else(corrupt)?;
    Ok((key, u64::from_be_bytes(start.try_into().expect("8 bytes"))))
}

#[derive(Debug, Serialize, Deserialize)]
struct Session<Acc> {
    end: Timestamp,
    fired: bool,
    acc: Acc,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SessionOperatorState {
    watermark: Option<Timestamp>,
    late: u64,
}

/// Session window aggregation in keyed state.
///
/// Every event opens a `[t, t + gap)` window that merges with any of the
/// key's sessions it overlaps or touches. Each session has one timer in a
/// state ordered by deadline: first its firing time, then (with allowed
/// lateness) its expiry, so a watermark only touches due sessions.
pub struct SessionWindowOperator<In, Agg> {
    gap: Timestamp,
    allowed_lateness: Timestamp,
    aggregate: Agg,
    state: SessionOperatorState,
    keep_late: bool,
    late_events: Vec<Event<In>>,
}

impl<In, Agg: AggregateFunction<In>> SessionWindowOperator<In, Agg> {
    pub fn new(windows: SessionWindows, aggregate: Agg) -> Self {
        Self {
            gap: windows.gap,
            allowed_lateness: 0,
            aggregate,
            state: SessionOperatorState::default(),
            keep_late: false,
            late_events: Vec::new(),
        }
    }

    pub fn with_allowed_lateness(mut self, lateness_ms: Timestamp) -> Self {
        self.allowed_lateness = lateness_ms;
        self
    }

    fn deadline(&self, session: &Session<Agg::Acc>) -> Timestamp {
        let fire = session.end - 1;
        if session.fired { fire.saturating_add(self.allowed_lateness) } else { fire }
    }
}

impl<In, Agg> LateEvents<In> for SessionWindowOperator<In, Agg> {
    fn keep_late_events(&mut self) {
        self.keep_late = true;
    }
    fn take_late_events(&mut self, out: &mut Vec<Event<In>>) {
        out.append(&mut self.late_events);
    }
}

impl<In, Agg> Operator for SessionWindowOperator<In, Agg>
where
    In: Send + 'static,
    Agg: AggregateFunction<In>,
{
    type In = In;
    type Out = WindowResult<Agg::Out>;

    fn state_names(&self) -> &'static [&'static str] {
        &[SESSION_STATE, SESSION_TIMERS]
    }

    fn process(&mut self, event: Event<In>, ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError> {
        let t = event.event_time;
        let key = event.key_bytes().to_vec();
        let mut window = TimeWindow {
            start: t,
            end: t.saturating_add(self.gap),
        };
        let mut acc = self.aggregate.create_accumulator();
        self.aggregate.add(&mut acc, &event.value);
        let mut fired = false;

        // Sessions of one key never overlap, so one pass in start order
        // catches every session the growing window reaches.
        let prefix = session_prefix(&key);
        let existing = ctx
            .state()
            .scan(SESSION_STATE, &prefix, prefix_successor(&prefix).as_deref())?;
        let mut absorbed = Vec::new();
        for (state_key, bytes) in existing {
            let (_, start) = parse_session_key(&state_key)?;
            let session: Session<Agg::Acc> = decode(&bytes)?;
            if start <= window.end && window.start <= session.end {
                window = TimeWindow {
                    start: window.start.min(start),
                    end: window.end.max(session.end),
                };
                fired |= session.fired;
                absorbed.push((start, self.deadline(&session)));
                acc = self.aggregate.merge(acc, session.acc);
            }
        }

        let watermark = self.state.watermark;
        let lateness = self.allowed_lateness;
        if watermark.is_some_and(|wm| window.max_timestamp().saturating_add(lateness) <= wm) {
            self.state.late += 1;
            if self.keep_late {
                self.late_events.push(event);
            }
            return Ok(());
        }

        for (start, deadline) in absorbed {
            ctx.state().delete(SESSION_STATE, &session_key(&key, start))?;
            ctx.state().delete(SESSION_TIMERS, &timer_key(deadline, &key, start))?;
        }

        // Already past its firing time: emit the updated session now.
        let late_firing = watermark.is_some_and(|wm| window.max_timestamp() <= wm);
        let session = Session {
            end: window.end,
            fired: fired || late_firing,
            acc,
        };
        let bytes = encode(&session)?;
        ctx.state().put(SESSION_STATE, &session_key(&key, window.start), &bytes)?;
        ctx.state()
            .put(SESSION_TIMERS, &timer_key(self.deadline(&session), &key, window.start), &[])?;
        if late_firing {
            let acc: Agg::Acc = decode::<Session<Agg::Acc>>(&bytes)?.acc;
            emit_result(ctx, window, key, self.aggregate.result(acc), true);
        }
        Ok(())
    }

    fn on_watermark(&mut self, watermark: Timestamp, ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError> {
        self.state.watermark = Some(watermark);
        let bound = below(watermark as i128 + 1);
        let due = ctx.state().scan(SESSION_TIMERS, &[], bound.as_ref().map(|b| &b[..]))?;
        for (timer, _) in due {
            ctx.state().delete(SESSION_TIMERS, &timer)?;
            let (key, start) = parse_session_key(&timer[8..])?;
            let state_key = session_key(key, start);
            let Some(bytes) = ctx.state().get(SESSION_STATE, &state_key)? else {
                continue;
            };
            let mut session: Session<Agg::Acc> = decode(&bytes)?;
            let window = TimeWindow { start, end: session.end };
            if !session.fired {
                let copy: Session<Agg::Acc> = decode(&bytes)?;
                emit_result(ctx, window, key.to_vec(), self.aggregate.result(copy.acc), false);
                session.fired = true;
            }
            let expiry = self.deadline(&session);
            if expiry <= watermark {
                ctx.state().delete(SESSION_STATE, &state_key)?;
            } else {
                ctx.state().put(SESSION_STATE, &state_key, &encode(&session)?)?;
                ctx.state().put(SESSION_TIMERS, &timer_key(expiry, key, start), &[])?;
            }
        }
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>, OperatorError> {
        Ok(encode(&self.state)?)
    }

    fn restore_state(&mut self, state: &[u8]) -> Result<(), OperatorError> {
        self.state = decode(state)?;
        Ok(())
    }

    fn late_records(&self) -> u64 {
        self.state.late
    }
}

/// Wraps a window operator so events it rejects as too late come out as
/// `Either::Right` instead of being dropped (a side output); results come
/// out as `Either::Left`.
pub struct WithLateOutput<W: Operator> {
    inner: W,
    results: Vec<Event<W::Out>>,
    late: Vec<Event<W::In>>,
}

impl<W: Operator + LateEvents<W::In>> WithLateOutput<W> {
    pub fn new(mut inner: W) -> Self {
        inner.keep_late_events();
        Self {
            inner,
            results: Vec::new(),
            late: Vec::new(),
        }
    }

    fn emit(&mut self, ctx: &mut Context<'_, Either<W::Out, W::In>>) {
        for event in self.results.drain(..) {
            ctx.emit(event.map_value(Either::Left));
        }
        self.inner.take_late_events(&mut self.late);
        for event in self.late.drain(..) {
            ctx.emit(event.map_value(Either::Right));
        }
    }
}

impl<W: Operator + LateEvents<W::In>> Operator for WithLateOutput<W> {
    type In = W::In;
    type Out = Either<W::Out, W::In>;

    fn state_names(&self) -> &'static [&'static str] {
        self.inner.state_names()
    }

    fn process(&mut self, event: Event<W::In>, ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError> {
        self.inner.process(event, &mut Context::new(&mut self.results, ctx.state()))?;
        self.emit(ctx);
        Ok(())
    }

    fn on_watermark(&mut self, watermark: Timestamp, ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError> {
        self.inner.on_watermark(watermark, &mut Context::new(&mut self.results, ctx.state()))?;
        self.emit(ctx);
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>, OperatorError> {
        self.inner.snapshot_state()
    }

    fn restore_state(&mut self, state: &[u8]) -> Result<(), OperatorError> {
        self.inner.restore_state(state)
    }

    fn late_records(&self) -> u64 {
        self.inner.late_records()
    }
}

/// One decoded entry of a window operator's keyed state, for debugging
/// tools (the time-travel state inspector).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WindowStateEntry<Acc> {
    /// `pane` (tumbling/sliding) or `session`.
    pub kind: &'static str,
    pub window: TimeWindow,
    pub key: Vec<u8>,
    /// Sessions: already emitted (kept only for allowed lateness).
    pub fired: Option<bool>,
    pub accumulator: Acc,
}

/// Decodes an entry of [`PANE_STATE`] or [`SESSION_STATE`]; `None` for
/// other (internal) states such as session timers.
pub fn decode_state_entry<Acc: DeserializeOwned>(
    state: &str,
    key: &[u8],
    value: &[u8],
) -> Result<Option<WindowStateEntry<Acc>>, OperatorError> {
    match state {
        PANE_STATE => {
            let (window, key) = parse_pane_key(key)?;
            Ok(Some(WindowStateEntry {
                kind: "pane",
                window,
                key: key.to_vec(),
                fired: None,
                accumulator: decode(value)?,
            }))
        }
        SESSION_STATE => {
            let (key, start) = parse_session_key(key)?;
            let session: Session<Acc> = decode(value)?;
            Ok(Some(WindowStateEntry {
                kind: "session",
                window: TimeWindow { start, end: session.end },
                key: key.to_vec(),
                fired: Some(session.fired),
                accumulator: session.acc,
            }))
        }
        _ => Ok(None),
    }
}

/// A window operator's own checkpointed state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WindowOperatorInfo {
    pub watermark: Option<Timestamp>,
    /// Tumbling/sliding: end of the last window fired on time.
    pub fired_through: Option<Timestamp>,
    pub late_records: u64,
}

/// Decodes a window operator's operator state (`sessions` for a session
/// window operator).
pub fn decode_operator_state(bytes: &[u8], sessions: bool) -> Option<WindowOperatorInfo> {
    if sessions {
        let state: SessionOperatorState = decode(bytes).ok()?;
        Some(WindowOperatorInfo {
            watermark: state.watermark,
            fired_through: None,
            late_records: state.late,
        })
    } else {
        let state: PaneOperatorState = decode(bytes).ok()?;
        Some(WindowOperatorInfo {
            watermark: state.watermark,
            fired_through: state.fired_through,
            late_records: state.late,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sluice_core::{Context, KeyedState};
    use sluice_state::MemoryBackend;

    use super::*;

    struct Harness<O: Operator> {
        op: O,
        state: MemoryBackend,
        out: Vec<Event<O::Out>>,
    }

    impl<O: Operator> Harness<O> {
        fn new(op: O) -> Self {
            Self {
                op,
                state: MemoryBackend::new("w"),
                out: Vec::new(),
            }
        }
        fn process(&mut self, event: Event<O::In>) {
            self.op.process(event, &mut Context::new(&mut self.out, &mut self.state)).unwrap();
        }
        fn watermark(&mut self, wm: Timestamp) {
            self.op.on_watermark(wm, &mut Context::new(&mut self.out, &mut self.state)).unwrap();
        }
        fn entries(&self, state: &str) -> usize {
            self.state.scan(state, &[], None).unwrap().len()
        }
    }

    fn keyed(key: &str, value: f64, t: Timestamp) -> Event<f64> {
        Event::new(value, t).with_key(key)
    }

    type Row = (Timestamp, Timestamp, String, u64, bool);

    fn rows(out: &[Event<WindowResult<u64>>]) -> Vec<Row> {
        out.iter()
            .map(|e| {
                let r = &e.value;
                (r.window.start, r.window.end, r.key_str().into_owned(), r.value, r.late_firing)
            })
            .collect()
    }

    #[test]
    fn assigners_list_every_containing_window() {
        let mut out = Vec::new();
        SlidingWindows::of_millis(10, 4).assign(9, &mut out);
        let spans: Vec<_> = out.iter().map(|w| (w.start, w.end)).collect();
        assert_eq!(spans, vec![(0, 10), (4, 14), (8, 18)]);
        assert_eq!(SlidingWindows::of_millis(10, 4).pane_millis(), 2);

        out.clear();
        for t in [0, 999, 1_000, 12_345] {
            TumblingWindows::of_millis(1_000).assign(t, &mut out);
        }
        let starts: Vec<_> = out.iter().map(|w| w.start).collect();
        assert_eq!(starts, vec![0, 0, 1_000, 12_000]);
    }

    #[test]
    fn tumbling_fires_only_windows_the_watermark_has_passed() {
        let mut h = Harness::new(WindowOperator::new(TumblingWindows::of_millis(1_000), Count));
        for t in [100, 200, 1_100, 2_500] {
            h.process(keyed("a", 0.0, t));
        }
        h.watermark(998);
        assert!(h.out.is_empty(), "window [0,1000) still open at 998");
        h.watermark(999);
        assert_eq!(rows(&h.out), vec![(0, 1_000, "a".into(), 2, false)]);
        assert_eq!(h.out[0].event_time, 999);

        h.out.clear();
        h.watermark(5_000);
        assert_eq!(
            rows(&h.out),
            vec![(1_000, 2_000, "a".into(), 1, false), (2_000, 3_000, "a".into(), 1, false)]
        );
        assert_eq!(h.entries(PANE_STATE), 0, "fired panes are removed");
    }

    #[test]
    fn results_per_key_come_out_sorted() {
        let mut h = Harness::new(WindowOperator::new(TumblingWindows::of_millis(1_000), Sum(|v: &f64| *v)));
        h.process(keyed("bob", 5.0, 10));
        h.process(keyed("alice", 1.0, 20));
        h.process(keyed("bob", 2.5, 30));
        h.watermark(999);
        let sums: Vec<_> = h.out.iter().map(|e| (e.value.key_str().into_owned(), e.value.value)).collect();
        assert_eq!(sums, vec![("alice".into(), 1.0), ("bob".into(), 7.5)]);
    }

    #[test]
    fn sliding_windows_match_a_naive_evaluation() {
        let windows = SlidingWindows::of_millis(1_000, 250);
        let mut h = Harness::new(WindowOperator::new(windows, Count));
        let mut expected: BTreeMap<(Timestamp, String), u64> = BTreeMap::new();
        let mut scratch = Vec::new();
        for i in 0..400u64 {
            let t = 3_000 + i * 20 + i * 37 % 7; // a little out of order, never late
            let key = format!("k{}", i % 3);
            scratch.clear();
            windows.assign(t, &mut scratch);
            for w in &scratch {
                *expected.entry((w.start, key.clone())).or_default() += 1;
            }
            h.process(keyed(&key, 0.0, t));
            if i % 25 == 0 {
                h.watermark(t.saturating_sub(10));
            }
        }
        h.watermark(u64::MAX);

        let mut got = BTreeMap::new();
        for (start, end, key, count, late) in rows(&h.out) {
            assert_eq!(end - start, 1_000);
            assert!(!late);
            assert!(got.insert((start, key), count).is_none(), "each window fires once");
        }
        assert_eq!(got, expected);
        assert_eq!(h.entries(PANE_STATE), 0);
        assert_eq!(h.op.late_records(), 0);
    }

    #[test]
    fn allowed_lateness_refires_then_drops() {
        let op = WindowOperator::new(TumblingWindows::of_millis(1_000), Count).with_allowed_lateness(500);
        let mut h = Harness::new(op);
        h.process(keyed("k", 0.0, 100));
        h.watermark(1_200);
        assert_eq!(rows(&h.out), vec![(0, 1_000, "k".into(), 1, false)]);

        h.out.clear();
        h.process(keyed("k", 0.0, 200)); // 1200 < 999 + 500: within lateness
        assert_eq!(rows(&h.out), vec![(0, 1_000, "k".into(), 2, true)]);

        h.out.clear();
        h.watermark(1_500); // expired: state purged
        assert!(h.out.is_empty());
        assert_eq!(h.entries(PANE_STATE), 0);
        h.process(keyed("k", 0.0, 300));
        assert!(h.out.is_empty());
        assert_eq!(h.op.late_records(), 1);
    }

    #[test]
    fn late_events_go_to_the_side_output() {
        let mut h = Harness::new(WithLateOutput::new(WindowOperator::new(TumblingWindows::of_millis(1_000), Count)));
        h.process(keyed("k", 1.0, 100));
        h.watermark(2_000);
        h.process(keyed("k", 7.0, 500));
        assert!(matches!(&h.out[0].value, Either::Left(r) if r.value == 1));
        assert!(matches!(&h.out[1].value, Either::Right(v) if *v == 7.0));
        assert_eq!(h.out[1].event_time, 500);
        assert_eq!(h.op.late_records(), 1);
    }

    #[test]
    fn sessions_close_after_the_gap() {
        let mut h = Harness::new(SessionWindowOperator::new(SessionWindows::with_gap_millis(100), Count));
        for (key, t) in [("k", 0), ("k", 50), ("k", 300), ("j", 10)] {
            h.process(keyed(key, 0.0, t));
        }
        h.watermark(400);
        assert_eq!(
            rows(&h.out),
            vec![
                (10, 110, "j".into(), 1, false),
                (0, 150, "k".into(), 2, false),
                (300, 400, "k".into(), 1, false)
            ]
        );
        assert_eq!(h.entries(SESSION_STATE), 0);
        assert_eq!(h.entries(SESSION_TIMERS), 0);
    }

    #[test]
    fn sessions_merge_when_an_event_bridges_them() {
        let mut h = Harness::new(SessionWindowOperator::new(SessionWindows::with_gap_millis(100), Count));
        for t in [0, 250, 120] {
            h.process(keyed("k", 0.0, t));
        }
        assert_eq!(h.entries(SESSION_STATE), 3);
        h.process(keyed("k", 0.0, 170)); // bridges [120,220) and [250,350)
        assert_eq!(h.entries(SESSION_STATE), 2);
        h.process(keyed("k", 0.0, 100)); // touches [0,100): everything merges
        assert_eq!(h.entries(SESSION_STATE), 1);
        assert_eq!(h.entries(SESSION_TIMERS), 1);
        h.watermark(u64::MAX);
        assert_eq!(rows(&h.out), vec![(0, 350, "k".into(), 5, false)]);
    }

    #[test]
    fn sessions_accept_late_events_within_lateness() {
        let op = SessionWindowOperator::new(SessionWindows::with_gap_millis(100), Count).with_allowed_lateness(1_000);
        let mut h = Harness::new(op);
        h.process(keyed("k", 0.0, 0));
        h.watermark(200);
        assert_eq!(rows(&h.out), vec![(0, 100, "k".into(), 1, false)]);

        h.out.clear();
        h.process(keyed("k", 0.0, 50)); // merges into the fired session
        assert_eq!(rows(&h.out), vec![(0, 150, "k".into(), 2, true)]);

        h.out.clear();
        h.watermark(2_000); // 149 + 1000 passed: expired, nothing re-emitted
        assert!(h.out.is_empty());
        assert_eq!(h.entries(SESSION_STATE), 0);
        h.process(keyed("k", 0.0, 60));
        assert_eq!(h.op.late_records(), 1);
    }

    #[test]
    fn operator_state_round_trips() {
        let mut h = Harness::new(WindowOperator::new(TumblingWindows::of_millis(1_000), Count));
        h.watermark(5_000);
        h.process(keyed("a", 0.0, 10));
        let snapshot = h.op.snapshot_state().unwrap();
        let mut restored = WindowOperator::<f64, _>::new(TumblingWindows::of_millis(1_000), Count);
        restored.restore_state(&snapshot).unwrap();
        assert_eq!(restored.late_records(), 1);
    }

    #[test]
    fn state_entries_decode_for_inspection() {
        let mut h = Harness::new(WindowOperator::new(TumblingWindows::of_millis(1_000), Count));
        h.process(keyed("ann", 0.0, 1_500));
        let (key, value) = h.state.scan(PANE_STATE, &[], None).unwrap().remove(0);
        let entry = decode_state_entry::<u64>(PANE_STATE, &key, &value).unwrap().unwrap();
        assert_eq!((entry.kind, entry.window, entry.key.as_slice(), entry.accumulator), ("pane", TimeWindow { start: 1_000, end: 2_000 }, &b"ann"[..], 1));
        h.watermark(500);
        let info = decode_operator_state(&h.op.snapshot_state().unwrap(), false).unwrap();
        assert_eq!(info.watermark, Some(500));

        let mut h = Harness::new(SessionWindowOperator::new(SessionWindows::with_gap_millis(100), Count));
        h.process(keyed("bo", 0.0, 10));
        let (key, value) = h.state.scan(SESSION_STATE, &[], None).unwrap().remove(0);
        let entry = decode_state_entry::<u64>(SESSION_STATE, &key, &value).unwrap().unwrap();
        assert_eq!((entry.window, entry.fired), (TimeWindow { start: 10, end: 110 }, Some(false)));
        let (timer, _) = h.state.scan(SESSION_TIMERS, &[], None).unwrap().remove(0);
        assert!(decode_state_entry::<u64>(SESSION_TIMERS, &timer, &[]).unwrap().is_none());
    }

    #[test]
    fn aggregates_merge() {
        assert_eq!(AggregateFunction::<f64>::merge(&Count, 2, 3), 5);
        let avg = Avg(|v: &f64| *v);
        assert_eq!(avg.result(avg.merge((3.0, 1), (9.0, 2))), 4.0);
    }
}
