//! Restart policy and restore-point selection.

use std::time::Duration;

use sluice_core::CheckpointId;
use sluice_state::{CheckpointStorage, StateHandle};

use crate::manifest::{CheckpointManifest, checkpoint_dirs, completed_checkpoints, read_manifest};

/// How often a failed job is restarted, and how long to wait in between.
/// After `max_restarts` restarts, the next failure is final.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartStrategy {
    pub max_restarts: u32,
    pub delay: Duration,
}

impl RestartStrategy {
    pub fn none() -> Self {
        Self {
            max_restarts: 0,
            delay: Duration::ZERO,
        }
    }

    pub fn fixed_delay(max_restarts: u32, delay: Duration) -> Self {
        Self { max_restarts, delay }
    }
}

impl Default for RestartStrategy {
    fn default() -> Self {
        Self::fixed_delay(3, Duration::from_secs(1))
    }
}

/// The newest completed checkpoint whose files are all present, if any.
/// Unreadable or damaged checkpoints are skipped in favour of older ones.
pub fn latest_manifest(storage: &CheckpointStorage) -> Option<CheckpointManifest> {
    let completed = completed_checkpoints(storage).ok()?;
    for id in completed.into_iter().rev() {
        match read_manifest(storage, id) {
            Ok(manifest) if is_restorable(storage, &manifest) => return Some(manifest),
            Ok(_) => tracing::warn!(checkpoint = id, "checkpoint is missing files; trying an older one"),
            Err(error) => tracing::warn!(checkpoint = id, %error, "unreadable checkpoint manifest; trying an older one"),
        }
    }
    None
}

fn is_restorable(storage: &CheckpointStorage, manifest: &CheckpointManifest) -> bool {
    manifest.referenced_paths().all(|path| storage.resolve(path).is_file())
}

/// The id to use for the next checkpoint: one past any id ever used in
/// `storage`, so ids stay unique across restarts and re-runs.
pub fn next_checkpoint_id(storage: &CheckpointStorage) -> CheckpointId {
    let from_dirs = checkpoint_dirs(storage).unwrap_or_default().into_iter().max().unwrap_or(0);
    let from_shared = std::fs::read_dir(storage.resolve("shared"))
        .into_iter()
        .flatten()
        .flatten()
        .flat_map(|stage| std::fs::read_dir(stage.path()).into_iter().flatten().flatten())
        .filter_map(|f| CheckpointStorage::shared_file_checkpoint(&f.file_name().to_string_lossy()))
        .max()
        .unwrap_or(0);
    from_dirs.max(from_shared) + 1
}

/// Bytes of keyed state across all stages of a manifest, by backend kind.
pub fn describe(manifest: &CheckpointManifest) -> String {
    let (mut memory, mut rocksdb) = (0, 0);
    for stage in manifest.stages.values() {
        match &stage.keyed_state {
            Some(handle @ StateHandle::Memory { .. }) => memory += handle.size(),
            Some(handle @ StateHandle::RocksDb { .. }) => rocksdb += handle.size(),
            None => {}
        }
    }
    format!(
        "checkpoint {} ({} stages, {memory} B memory state, {rocksdb} B rocksdb state)",
        manifest.checkpoint_id,
        manifest.stages.len()
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sluice_state::FileRef;

    use super::*;
    use crate::manifest::{StageState, write_manifest};

    #[test]
    fn picks_newest_restorable_checkpoint_and_continues_ids() {
        let dir = tempfile::TempDir::new().unwrap();
        let storage = CheckpointStorage::new(dir.path()).unwrap();
        assert!(latest_manifest(&storage).is_none());
        assert_eq!(next_checkpoint_id(&storage), 1);

        let make = |id: CheckpointId| {
            let file = storage
                .write(&CheckpointStorage::private_path(id, "00-src", "operator-state.bin"), b"x")
                .unwrap();
            let manifest = CheckpointManifest {
                job: "j".into(),
                checkpoint_id: id,
                triggered_at: 0,
                completed_at: 0,
                stages: BTreeMap::from([(
                    "00-src".to_owned(),
                    StageState {
                        operator_state: Some(file),
                        keyed_state: None,
                    },
                )]),
            };
            write_manifest(&storage, &manifest).unwrap();
            manifest
        };
        make(1);
        let second = make(2);
        assert_eq!(latest_manifest(&storage).unwrap().checkpoint_id, 2);

        // Damage checkpoint 2: recovery falls back to 1.
        let FileRef { path, .. } = second.stages["00-src"].operator_state.clone().unwrap();
        std::fs::remove_file(storage.resolve(&path)).unwrap();
        assert_eq!(latest_manifest(&storage).unwrap().checkpoint_id, 1);

        storage.write("shared/01-w/0000000009-000001.sst", b"").unwrap();
        assert_eq!(next_checkpoint_id(&storage), 10);
    }
}
