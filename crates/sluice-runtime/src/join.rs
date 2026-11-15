//! Windowed stream-stream inner join.
//!
//! Both inputs are assigned to tumbling or sliding windows and buffered in
//! keyed state per (window, key). When the watermark (the minimum across
//! both inputs) passes a window, every left element is paired with every
//! right element of the same key in that window, and the window's state is
//! dropped. An element whose windows have all fired is late and dropped.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use sluice_core::{Context, Either, Event, Operator, OperatorError, Timestamp, decode, encode, now_millis};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::window::{SlidingWindows, TimeWindow};

pub const JOIN_LEFT: &str = "join-left";
pub const JOIN_RIGHT: &str = "join-right";

/// `window end ++ window start ++ len(key) ++ key ++ sequence`: ordered by
/// window end, unique per buffered element.
fn buffer_key(window: TimeWindow, key: &[u8], seq: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(28 + key.len());
    out.extend_from_slice(&window.end.to_be_bytes());
    out.extend_from_slice(&window.start.to_be_bytes());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&seq.to_be_bytes());
    out
}

fn parse_buffer_key(bytes: &[u8]) -> Result<(TimeWindow, &[u8]), OperatorError> {
    let corrupt = || OperatorError::Other(format!("corrupt join state key ({} bytes)", bytes.len()));
    let header = bytes.get(..20).ok_or_else(corrupt)?;
    let end = u64::from_be_bytes(header[..8].try_into().expect("8 bytes"));
    let start = u64::from_be_bytes(header[8..16].try_into().expect("8 bytes"));
    let len = u32::from_be_bytes(header[16..20].try_into().expect("4 bytes")) as usize;
    let key = bytes.get(20..20 + len).ok_or_else(corrupt)?;
    Ok((TimeWindow { start, end }, key))
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct JoinState {
    watermark: Option<Timestamp>,
    next_seq: u64,
    late: u64,
}

type Pending<L, R> = BTreeMap<(Timestamp, Timestamp, Vec<u8>), (Vec<L>, Vec<R>)>;

pub struct WindowJoinOperator<L, R, KL, KR, F, O> {
    windows: SlidingWindows,
    left_key: KL,
    right_key: KR,
    join: F,
    state: JoinState,
    scratch: Vec<TimeWindow>,
    _types: PhantomData<fn(L, R) -> O>,
}

impl<L, R, KL, KR, F, O> WindowJoinOperator<L, R, KL, KR, F, O>
where
    KL: Fn(&L) -> Vec<u8>,
    KR: Fn(&R) -> Vec<u8>,
    F: Fn(&L, &R) -> O,
{
    pub fn new(windows: impl Into<SlidingWindows>, left_key: KL, right_key: KR, join: F) -> Self {
        Self {
            windows: windows.into(),
            left_key,
            right_key,
            join,
            state: JoinState::default(),
            scratch: Vec::new(),
            _types: PhantomData,
        }
    }
}

impl<L, R, KL, KR, F, O> Operator for WindowJoinOperator<L, R, KL, KR, F, O>
where
    L: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    KL: Fn(&L) -> Vec<u8> + Send + 'static,
    KR: Fn(&R) -> Vec<u8> + Send + 'static,
    F: Fn(&L, &R) -> O + Send + 'static,
    O: Send + 'static,
{
    type In = Either<L, R>;
    type Out = O;

    fn state_names(&self) -> &'static [&'static str] {
        &[JOIN_LEFT, JOIN_RIGHT]
    }

    fn process(&mut self, event: Event<Either<L, R>>, ctx: &mut Context<'_, O>) -> Result<(), OperatorError> {
        let (state, key, bytes) = match &event.value {
            Either::Left(left) => (JOIN_LEFT, (self.left_key)(left), encode(left)?),
            Either::Right(right) => (JOIN_RIGHT, (self.right_key)(right), encode(right)?),
        };
        self.scratch.clear();
        self.windows.assign(event.event_time, &mut self.scratch);
        let mut buffered = false;
        for window in &self.scratch {
            if self.state.watermark.is_some_and(|wm| window.max_timestamp() <= wm) {
                continue; // this window already joined
            }
            let seq = self.state.next_seq;
            self.state.next_seq += 1;
            ctx.state().put(state, &buffer_key(*window, &key, seq), &bytes)?;
            buffered = true;
        }
        if !buffered {
            self.state.late += 1;
        }
        Ok(())
    }

    fn on_watermark(&mut self, watermark: Timestamp, ctx: &mut Context<'_, O>) -> Result<(), OperatorError> {
        self.state.watermark = Some(watermark);
        // Windows with end - 1 <= watermark.
        let bound = (watermark as u128 + 2 <= u64::MAX as u128).then(|| (watermark + 2).to_be_bytes());
        let bound = bound.as_ref().map(|b| &b[..]);

        let mut pending: Pending<L, R> = BTreeMap::new();
        for (key, bytes) in ctx.state().scan(JOIN_LEFT, &[], bound)? {
            let (window, join_key) = parse_buffer_key(&key)?;
            let entry = pending.entry((window.end, window.start, join_key.to_vec())).or_default();
            entry.0.push(decode(&bytes)?);
        }
        for (key, bytes) in ctx.state().scan(JOIN_RIGHT, &[], bound)? {
            let (window, join_key) = parse_buffer_key(&key)?;
            let entry = pending.entry((window.end, window.start, join_key.to_vec())).or_default();
            entry.1.push(decode(&bytes)?);
        }
        if pending.is_empty() {
            return Ok(());
        }
        ctx.state().delete_range(JOIN_LEFT, &[], bound)?;
        ctx.state().delete_range(JOIN_RIGHT, &[], bound)?;

        let now = now_millis();
        for ((end, _start, key), (lefts, rights)) in pending {
            for left in &lefts {
                for right in &rights {
                    ctx.emit(Event {
                        key: Some(key.clone()),
                        value: (self.join)(left, right),
                        event_time: end - 1,
                        ingestion_time: now,
                    });
                }
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

#[cfg(test)]
mod tests {
    use sluice_core::KeyedState;
    use sluice_state::MemoryBackend;

    use super::*;
    use crate::window::TumblingWindows;

    type Op = WindowJoinOperator<
        (String, u32),
        (String, char),
        fn(&(String, u32)) -> Vec<u8>,
        fn(&(String, char)) -> Vec<u8>,
        fn(&(String, u32), &(String, char)) -> String,
        String,
    >;

    fn op() -> Op {
        WindowJoinOperator::new(
            TumblingWindows::of_millis(100),
            |l: &(String, u32)| l.0.clone().into_bytes(),
            |r: &(String, char)| r.0.clone().into_bytes(),
            |l: &(String, u32), r: &(String, char)| format!("{}:{}{}", l.0, l.1, r.1),
        )
    }

    fn left(key: &str, v: u32, t: Timestamp) -> Event<Either<(String, u32), (String, char)>> {
        Event::new(Either::Left((key.to_string(), v)), t)
    }

    fn right(key: &str, v: char, t: Timestamp) -> Event<Either<(String, u32), (String, char)>> {
        Event::new(Either::Right((key.to_string(), v)), t)
    }

    #[test]
    fn pairs_matching_keys_within_a_window() {
        let mut op = op();
        let mut state = MemoryBackend::new("join");
        let mut out = Vec::new();
        for event in [
            left("a", 1, 10),
            left("a", 2, 20),
            right("a", 'x', 30),
            right("b", 'y', 40),  // no left partner
            right("a", 'z', 150), // next window: no left partner
            left("a", 3, 120),
        ] {
            op.process(event, &mut Context::new(&mut out, &mut state)).unwrap();
        }
        op.on_watermark(99, &mut Context::new(&mut out, &mut state)).unwrap();
        let values: Vec<_> = out.iter().map(|e| e.value.clone()).collect();
        assert_eq!(values, vec!["a:1x", "a:2x"]);
        assert!(out.iter().all(|e| e.event_time == 99 && e.key_bytes() == b"a"));

        out.clear();
        op.on_watermark(199, &mut Context::new(&mut out, &mut state)).unwrap();
        assert_eq!(out.iter().map(|e| e.value.clone()).collect::<Vec<_>>(), vec!["a:3z"]);
        assert!(state.scan(JOIN_LEFT, &[], None).unwrap().is_empty());
        assert!(state.scan(JOIN_RIGHT, &[], None).unwrap().is_empty());

        op.process(left("a", 4, 50), &mut Context::new(&mut out, &mut state)).unwrap();
        assert_eq!(op.late_records(), 1);
    }
}
