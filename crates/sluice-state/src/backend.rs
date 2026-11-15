use std::path::Path;

use sluice_core::{CheckpointId, KeyedState, StateError};
use serde::{Deserialize, Serialize};

use crate::checkpoint::{CheckpointStorage, SnapshotResult, StateHandle};
use crate::memory::MemoryBackend;
use crate::rocks::RocksDbBackend;

/// A [`KeyedState`] that can be checkpointed.
pub trait StateBackend: KeyedState {
    /// Synchronous part of a snapshot: captures an immutable view of the
    /// current state. Runs while the operator is paused at a barrier, so it
    /// must be cheap (RocksDB: flush + hard links).
    fn snapshot(&mut self, checkpoint_id: CheckpointId) -> Result<Box<dyn PendingSnapshot>, StateError>;

    /// The named states holding data, sorted.
    fn state_names(&self) -> Vec<String>;
}

/// Asynchronous part of a snapshot: persists the captured state to
/// checkpoint storage. Blocking; run it off the async runtime.
pub trait PendingSnapshot: Send {
    fn upload(self: Box<Self>, storage: &CheckpointStorage) -> Result<SnapshotResult, StateError>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateBackendKind {
    #[default]
    Memory,
    #[serde(rename = "rocksdb")]
    RocksDb,
}

/// Creates the backend for one stateful stage, restoring `restore` if given.
///
/// `work_dir` holds live state for this attempt of the job (RocksDB files);
/// `storage` is where restored state is read from.
pub fn create_backend(
    kind: StateBackendKind,
    stage: &str,
    states: &[&str],
    work_dir: &Path,
    storage: Option<&CheckpointStorage>,
    restore: Option<&StateHandle>,
) -> Result<Box<dyn StateBackend>, StateError> {
    let restore = match (restore, storage) {
        (Some(handle), Some(storage)) => Some((handle, storage)),
        (Some(_), None) => return Err(StateError::new("cannot restore state without checkpoint storage")),
        (None, _) => None,
    };
    Ok(match kind {
        StateBackendKind::Memory => Box::new(MemoryBackend::open(stage, restore)?),
        StateBackendKind::RocksDb => Box::new(RocksDbBackend::open(stage, states, work_dir, restore)?),
    })
}
