use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Milliseconds since the Unix epoch.
pub type Timestamp = u64;

/// The watermark a stage sees once all of its inputs have ended: every
/// pending window is complete and may fire.
pub const MAX_WATERMARK: Timestamp = Timestamp::MAX;

/// Current wall-clock time in milliseconds.
pub fn now_millis() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as Timestamp)
        .unwrap_or(0)
}

/// The envelope every record travels in.
///
/// `key` is set by a key-by operator and drives partitioned state (windows
/// are kept per key). `event_time` is when the event happened according to
/// the producer; `ingestion_time` is when Sluice first saw it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event<T> {
    pub key: Option<Vec<u8>>,
    pub value: T,
    pub event_time: Timestamp,
    pub ingestion_time: Timestamp,
}

impl<T> Event<T> {
    /// A key-less event, stamped with the current time as ingestion time.
    pub fn new(value: T, event_time: Timestamp) -> Self {
        Self {
            key: None,
            value,
            event_time,
            ingestion_time: now_millis(),
        }
    }

    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// The key, or the empty key for un-keyed events.
    pub fn key_bytes(&self) -> &[u8] {
        self.key.as_deref().unwrap_or(&[])
    }

    /// Replaces the value while keeping key and timestamps.
    pub fn map_value<U>(self, f: impl FnOnce(T) -> U) -> Event<U> {
        Event {
            key: self.key,
            value: f(self.value),
            event_time: self.event_time,
            ingestion_time: self.ingestion_time,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("codec error: {0}")]
pub struct CodecError(#[from] bincode::Error);

/// Binary encoding used for state values and snapshots.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, CodecError> {
    Ok(bincode::serialize(value)?)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    Ok(bincode::deserialize(bytes)?)
}

/// Identifies a checkpoint. Ids increase monotonically over a job's life,
/// across restarts.
pub type CheckpointId = u64;

/// Marker injected into the stream by sources when a checkpoint starts.
/// Everything before it on a channel belongs to the checkpoint; everything
/// after it does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointBarrier {
    pub checkpoint_id: CheckpointId,
    /// Wall-clock time the checkpoint was triggered.
    pub timestamp: Timestamp,
}

impl<T: Serialize> Event<T> {
    /// Compact binary encoding (bincode) used for state and network transfer.
    pub fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        Ok(bincode::serialize(self)?)
    }
}

impl<T: DeserializeOwned> Event<T> {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(bincode::deserialize(bytes)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamStatus {
    Active,
    Idle,
}

/// A value from one of two inputs: what a two-input operator (a join)
/// receives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

/// What travels over the channel between two pipeline stages.
///
/// Watermarks are in-band control messages rather than a field on each
/// event, so a quiet stream can still advance event time and an operator
/// sees watermarks in order with the records around them.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamElement<T> {
    Record(Event<T>),
    /// No record with `event_time <= watermark` will follow on this channel.
    Watermark(Timestamp),
    /// Checkpoint boundary; see [`CheckpointBarrier`].
    Barrier(CheckpointBarrier),
    /// The producer went idle (no data for a while) or became active again.
    /// Idle inputs do not hold back the watermark of multi-input operators.
    Status(StreamStatus),
    /// The producer finished; nothing follows.
    End,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Purchase {
        user_id: String,
        amount: f64,
    }

    #[test]
    fn bincode_round_trip_preserves_everything() {
        let event = Event::new(
            Purchase {
                user_id: "u-1".into(),
                amount: 12.5,
            },
            1_700_000_000_000,
        )
        .with_key("u-1");

        let bytes = event.to_bytes().unwrap();
        let decoded: Event<Purchase> = Event::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, event);
    }

    #[test]
    fn encode_decode_round_trip() {
        let bytes = encode(&(3u64, 2.5f64)).unwrap();
        assert_eq!(decode::<(u64, f64)>(&bytes).unwrap(), (3, 2.5));
        assert!(decode::<(u64, f64)>(&bytes[..3]).is_err());
    }

    #[test]
    fn decoding_garbage_fails() {
        assert!(Event::<Purchase>::from_bytes(&[1, 2, 3]).is_err());
    }

    #[test]
    fn map_value_keeps_metadata() {
        let event = Event::new(21, 42).with_key("k");
        let mapped = event.clone().map_value(|v| v * 2);
        assert_eq!(mapped.value, 42);
        assert_eq!(mapped.key, event.key);
        assert_eq!(mapped.event_time, 42);
        assert_eq!(mapped.ingestion_time, event.ingestion_time);
    }

    #[test]
    fn unkeyed_event_has_empty_key() {
        assert_eq!(Event::new((), 0).key_bytes(), b"");
        assert_eq!(Event::new((), 0).with_key("ab").key_bytes(), b"ab");
    }
}
