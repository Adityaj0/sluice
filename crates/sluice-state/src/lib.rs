//! Keyed state backends and their snapshots.
//!
//! - [`MemoryBackend`]: ordered in-memory maps; each checkpoint writes a full copy.
//! - [`RocksDbBackend`]: an embedded RocksDB per stateful operator, one column
//!   family per named state. Checkpoints are **incremental**: SST files are
//!   immutable, so a checkpoint only uploads SST files that no earlier
//!   checkpoint uploaded, and references the rest.
//!
//! A snapshot has a synchronous part ([`StateBackend::snapshot`]), run while
//! the operator is paused at a barrier, and an asynchronous part
//! ([`PendingSnapshot::upload`]) run on a blocking thread while the operator
//! carries on processing.

pub mod backend;
pub mod checkpoint;
pub mod memory;
pub mod rocks;

pub use backend::{PendingSnapshot, StateBackend, StateBackendKind, create_backend};
pub use checkpoint::{CheckpointStorage, FileRef, SharedFile, SnapshotResult, StateHandle};
pub use memory::MemoryBackend;
pub use rocks::RocksDbBackend;
