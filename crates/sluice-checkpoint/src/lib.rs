//! Fault tolerance for Sluice jobs: aligned-barrier checkpoints (the
//! Chandy-Lamport snapshot algorithm specialised to a DAG with FIFO
//! channels) and rollback recovery.
//!
//! 1. The [`CheckpointCoordinator`] periodically sends
//!    [`ControlMessage::Trigger`] to every source.
//! 2. A source records its read position and injects a
//!    [`CheckpointBarrier`] into its output.
//! 3. An operator that has seen the barrier on *all* its inputs (pausing
//!    each input as its barrier arrives) snapshots its state, forwards the
//!    barrier and acknowledges with a [`StageAck`].
//! 4. Sinks pre-commit their output when the barrier arrives.
//! 5. Once every stage has acknowledged, the coordinator writes the
//!    [`CheckpointManifest`] and broadcasts [`ControlMessage::Complete`]:
//!    sinks commit, sources release replay data.
//!
//! On failure, the job restarts every stage from the latest manifest
//! ([`recovery`]).

pub mod coordinator;
pub mod manifest;
pub mod recovery;

pub use coordinator::{
    CheckpointConfig, CheckpointCoordinator, CheckpointHistory, CheckpointRecord, CheckpointStatus, ControlMessage,
    StageAck, StageCheckpointStats, StageInfo, StageReport,
};
pub use sluice_core::{CheckpointBarrier, CheckpointId};
pub use manifest::{CheckpointManifest, GcStats, StageState, cleanup_incomplete, garbage_collect};
pub use recovery::{RestartStrategy, latest_manifest, next_checkpoint_id};
