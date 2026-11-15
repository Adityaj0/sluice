//! Core building blocks shared by every Sluice crate: the event envelope
//! and checkpoint barriers, the [`Operator`] trait with the stateless
//! operators, the [`KeyedState`] interface, and watermark logic.

pub mod event;
pub mod operator;
pub mod state;
pub mod watermark;

pub use event::{
    CheckpointBarrier, CheckpointId, CodecError, Either, Event, MAX_WATERMARK, StreamElement, StreamStatus, Timestamp, decode, encode,
    now_millis,
};
pub use operator::{
    Context, FilterMapOperator, FilterOperator, KeyByOperator, MapOperator, Operator, OperatorError,
};
pub use state::{Entries, Entry, KeyedState, NoState, StateError, prefix_successor};
pub use watermark::{BoundedOutOfOrderness, WatermarkTracker};
