use std::marker::PhantomData;

use crate::event::{CodecError, Event, Timestamp};
use crate::state::{KeyedState, StateError};

/// What an operator gets on every call: somewhere to emit results and its
/// keyed state.
pub struct Context<'a, Out> {
    output: &'a mut Vec<Event<Out>>,
    state: &'a mut dyn KeyedState,
}

impl<'a, Out> Context<'a, Out> {
    pub fn new(output: &'a mut Vec<Event<Out>>, state: &'a mut dyn KeyedState) -> Self {
        Self { output, state }
    }

    pub fn emit(&mut self, event: Event<Out>) {
        self.output.push(event);
    }

    pub fn state(&mut self) -> &mut dyn KeyedState {
        self.state
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("{0}")]
    Other(String),
}

/// A stream transformation.
///
/// The runtime gives every operator its own Tokio task and calls these
/// methods from it, so implementations are plain synchronous code that owns
/// its state outright (no locks).
///
/// State comes in two kinds, both included in checkpoints:
/// - **keyed state**, read and written through [`Context::state`], for
///   anything that grows with the number of keys (window contents). It
///   lives in the job's state backend (RocksDB or in-memory).
/// - **operator state**, a small blob from [`snapshot_state`](Self::snapshot_state)
///   for per-operator scalars such as the current watermark.
pub trait Operator: Send + 'static {
    type In: Send + 'static;
    type Out: Send + 'static;

    /// Names of the keyed states this operator uses. Operators that return
    /// none get no state backend (and nothing to snapshot).
    fn state_names(&self) -> &'static [&'static str] {
        &[]
    }

    /// Handles one record, emitting zero or more results.
    fn process(&mut self, event: Event<Self::In>, ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError>;

    /// Event time on the operator's input advanced to `watermark`. Stateful
    /// operators emit whatever is now complete. Called with
    /// [`MAX_WATERMARK`](crate::MAX_WATERMARK) when the input ends.
    fn on_watermark(&mut self, _watermark: Timestamp, _ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError> {
        Ok(())
    }

    /// Serializes operator state for a checkpoint.
    fn snapshot_state(&self) -> Result<Vec<u8>, OperatorError> {
        Ok(Vec::new())
    }

    /// Restores what [`snapshot_state`](Self::snapshot_state) produced.
    /// Called before any other method when a job recovers.
    fn restore_state(&mut self, _state: &[u8]) -> Result<(), OperatorError> {
        Ok(())
    }

    /// Number of records dropped so far for arriving behind the watermark.
    fn late_records(&self) -> u64 {
        0
    }
}

/// Applies a function to every value.
pub struct MapOperator<F, In, Out> {
    f: F,
    _types: PhantomData<fn(In) -> Out>,
}

impl<F, In, Out> MapOperator<F, In, Out>
where
    F: FnMut(In) -> Out,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _types: PhantomData,
        }
    }
}

impl<F, In, Out> Operator for MapOperator<F, In, Out>
where
    F: FnMut(In) -> Out + Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
{
    type In = In;
    type Out = Out;

    fn process(&mut self, event: Event<In>, ctx: &mut Context<'_, Out>) -> Result<(), OperatorError> {
        ctx.emit(event.map_value(&mut self.f));
        Ok(())
    }
}

/// Keeps only the events whose value satisfies the predicate.
pub struct FilterOperator<P, T> {
    predicate: P,
    _types: PhantomData<fn(T)>,
}

impl<P, T> FilterOperator<P, T>
where
    P: FnMut(&T) -> bool,
{
    pub fn new(predicate: P) -> Self {
        Self {
            predicate,
            _types: PhantomData,
        }
    }
}

impl<P, T> Operator for FilterOperator<P, T>
where
    P: FnMut(&T) -> bool + Send + 'static,
    T: Send + 'static,
{
    type In = T;
    type Out = T;

    fn process(&mut self, event: Event<T>, ctx: &mut Context<'_, T>) -> Result<(), OperatorError> {
        if (self.predicate)(&event.value) {
            ctx.emit(event);
        }
        Ok(())
    }
}

/// Map and filter in one step: `None` drops the event. Useful for decoding,
/// where a value that fails to parse should simply be skipped.
pub struct FilterMapOperator<F, In, Out> {
    f: F,
    _types: PhantomData<fn(In) -> Out>,
}

impl<F, In, Out> FilterMapOperator<F, In, Out>
where
    F: FnMut(In) -> Option<Out>,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _types: PhantomData,
        }
    }
}

impl<F, In, Out> Operator for FilterMapOperator<F, In, Out>
where
    F: FnMut(In) -> Option<Out> + Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
{
    type In = In;
    type Out = Out;

    fn process(&mut self, event: Event<In>, ctx: &mut Context<'_, Out>) -> Result<(), OperatorError> {
        let Event {
            key,
            value,
            event_time,
            ingestion_time,
        } = event;
        if let Some(value) = (self.f)(value) {
            ctx.emit(Event {
                key,
                value,
                event_time,
                ingestion_time,
            });
        }
        Ok(())
    }
}

/// Sets each event's key from its value. Downstream stateful operators
/// (windows) partition their state by this key.
pub struct KeyByOperator<F, T> {
    key_fn: F,
    _types: PhantomData<fn(T)>,
}

impl<F, T> KeyByOperator<F, T>
where
    F: FnMut(&T) -> Vec<u8>,
{
    pub fn new(key_fn: F) -> Self {
        Self {
            key_fn,
            _types: PhantomData,
        }
    }
}

impl<F, T> Operator for KeyByOperator<F, T>
where
    F: FnMut(&T) -> Vec<u8> + Send + 'static,
    T: Send + 'static,
{
    type In = T;
    type Out = T;

    fn process(&mut self, mut event: Event<T>, ctx: &mut Context<'_, T>) -> Result<(), OperatorError> {
        event.key = Some((self.key_fn)(&event.value));
        ctx.emit(event);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::NoState;

    fn run<O: Operator>(op: &mut O, inputs: impl IntoIterator<Item = Event<O::In>>) -> Vec<Event<O::Out>> {
        let mut out = Vec::new();
        let mut state = NoState;
        for event in inputs {
            op.process(event, &mut Context::new(&mut out, &mut state)).unwrap();
        }
        out
    }

    fn hundred_events() -> Vec<Event<u64>> {
        (1..=100).map(|i| Event::new(i, i * 10)).collect()
    }

    #[test]
    fn map_transforms_every_value_and_keeps_timestamps() {
        let mut op = MapOperator::new(|x: u64| x * 2);
        let out = run(&mut op, hundred_events());
        assert_eq!(out.len(), 100);
        for (i, event) in out.iter().enumerate() {
            let n = i as u64 + 1;
            assert_eq!(event.value, n * 2);
            assert_eq!(event.event_time, n * 10);
        }
    }

    #[test]
    fn map_can_change_type() {
        let mut op = MapOperator::new(|x: u64| format!("#{x}"));
        let out = run(&mut op, hundred_events());
        assert_eq!(out[0].value, "#1");
        assert_eq!(out[99].value, "#100");
    }

    #[test]
    fn filter_drops_non_matching() {
        let mut op = FilterOperator::new(|x: &u64| x % 2 == 0);
        let out = run(&mut op, hundred_events());
        assert_eq!(out.len(), 50);
        assert!(out.iter().all(|e| e.value % 2 == 0));
    }

    #[test]
    fn filter_map_drops_none_and_transforms_some() {
        let mut op = FilterMapOperator::new(|x: u64| (x > 90).then(|| x.to_string()));
        let out = run(&mut op, hundred_events());
        assert_eq!(out.len(), 10);
        assert_eq!(out[0].value, "91");
        assert_eq!(out[0].event_time, 910);
    }

    #[test]
    fn key_by_sets_key_from_value() {
        let mut op = KeyByOperator::new(|x: &u64| (x % 3).to_string().into_bytes());
        let out = run(&mut op, hundred_events());
        assert_eq!(out.len(), 100);
        assert_eq!(out[0].key_bytes(), b"1");
        assert_eq!(out[1].key_bytes(), b"2");
        assert_eq!(out[2].key_bytes(), b"0");
    }

    #[test]
    fn key_by_overwrites_existing_key() {
        let mut op = KeyByOperator::new(|_: &u64| b"new".to_vec());
        let out = run(&mut op, [Event::new(1, 1).with_key("old")]);
        assert_eq!(out[0].key_bytes(), b"new");
    }

    #[test]
    fn stateless_operators_cannot_touch_state() {
        let state = NoState;
        let err = state.get("panes", b"k").unwrap_err();
        assert!(err.0.contains("panes"));
    }
}
