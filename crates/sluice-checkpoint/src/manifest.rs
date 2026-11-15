//! JSON checkpoint manifests and checkpoint garbage collection.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io;

use sluice_core::{CheckpointId, Timestamp};
use sluice_state::{CheckpointStorage, FileRef, StateHandle};
use serde::{Deserialize, Serialize};

pub const METADATA_FILE: &str = "_metadata.json";

/// Everything one stage needs to resume from a checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageState {
    /// Small per-operator state (source offsets, operator watermark).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_state: Option<FileRef>,
    /// Keyed state backend snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyed_state: Option<StateHandle>,
}

impl StageState {
    pub fn size(&self) -> u64 {
        self.operator_state.as_ref().map_or(0, |f| f.size) + self.keyed_state.as_ref().map_or(0, StateHandle::size)
    }

    fn paths(&self) -> impl Iterator<Item = &str> {
        self.operator_state
            .iter()
            .map(|f| f.path.as_str())
            .chain(self.keyed_state.iter().flat_map(StateHandle::paths))
    }
}

/// A completed checkpoint. Its presence on disk is what makes the
/// checkpoint complete: it is written last, atomically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub job: String,
    pub checkpoint_id: CheckpointId,
    pub triggered_at: Timestamp,
    pub completed_at: Timestamp,
    /// Keyed by stage uid (`<index>-<name>`).
    pub stages: BTreeMap<String, StageState>,
}

impl CheckpointManifest {
    pub fn size(&self) -> u64 {
        self.stages.values().map(StageState::size).sum()
    }

    /// Every storage path the checkpoint needs.
    pub fn referenced_paths(&self) -> impl Iterator<Item = &str> {
        self.stages.values().flat_map(StageState::paths)
    }
}

fn metadata_path(id: CheckpointId) -> String {
    format!("{}/{METADATA_FILE}", CheckpointStorage::checkpoint_dir_name(id))
}

/// Writes the manifest via write-to-temp + rename, so a crash never leaves
/// a half-written manifest that looks complete.
pub fn write_manifest(storage: &CheckpointStorage, manifest: &CheckpointManifest) -> io::Result<()> {
    let path = storage.resolve(&metadata_path(manifest.checkpoint_id));
    fs::create_dir_all(path.parent().expect("manifest has a parent directory"))?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(manifest).map_err(io::Error::other)?)?;
    fs::rename(&tmp, &path)
}

pub fn read_manifest(storage: &CheckpointStorage, id: CheckpointId) -> io::Result<CheckpointManifest> {
    let bytes = fs::read(storage.resolve(&metadata_path(id)))?;
    serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Ids of every checkpoint directory, complete or not, ascending.
pub fn checkpoint_dirs(storage: &CheckpointStorage) -> io::Result<Vec<CheckpointId>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(storage.root())? {
        if let Some(id) = CheckpointStorage::parse_checkpoint_dir(&entry?.file_name().to_string_lossy()) {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Ids of completed checkpoints (those with a manifest), ascending.
pub fn completed_checkpoints(storage: &CheckpointStorage) -> io::Result<Vec<CheckpointId>> {
    Ok(checkpoint_dirs(storage)?
        .into_iter()
        .filter(|id| storage.resolve(&metadata_path(*id)).is_file())
        .collect())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcStats {
    pub checkpoints_removed: usize,
    pub files_removed: usize,
}

/// Keeps the newest `retain` completed checkpoints and deletes everything
/// else that can no longer be needed:
///
/// - checkpoint directories older than the newest completed one that are
///   not retained (including failed and abandoned ones), and
/// - shared SST files referenced by no retained checkpoint, provided they
///   were uploaded by a checkpoint no newer than the newest completed one
///   (newer files may belong to a checkpoint still in flight).
pub fn garbage_collect(storage: &CheckpointStorage, retain: usize) -> io::Result<GcStats> {
    collect(storage, retain, false)
}

/// Like [`garbage_collect`], for when no checkpoint can be in flight (the
/// job attempt that ran them has ended): incomplete checkpoints newer than
/// the latest completed one, and files only they uploaded, go too.
pub fn cleanup_incomplete(storage: &CheckpointStorage, retain: usize) -> io::Result<GcStats> {
    collect(storage, retain, true)
}

fn collect(storage: &CheckpointStorage, retain: usize, nothing_in_flight: bool) -> io::Result<GcStats> {
    let mut stats = GcStats::default();
    let completed = completed_checkpoints(storage)?;
    let latest = match (completed.last(), nothing_in_flight) {
        (_, true) => CheckpointId::MAX,
        (Some(&latest), false) => latest,
        (None, false) => return Ok(stats),
    };
    let keep = &completed[completed.len().saturating_sub(retain.max(1))..];

    let mut referenced = HashSet::new();
    for id in keep {
        let manifest = read_manifest(storage, *id)?;
        referenced.extend(manifest.referenced_paths().map(str::to_owned));
    }

    for id in checkpoint_dirs(storage)? {
        if id <= latest && !keep.contains(&id) {
            fs::remove_dir_all(storage.checkpoint_dir(id))?;
            stats.checkpoints_removed += 1;
        }
    }

    let shared = storage.resolve("shared");
    for stage in fs::read_dir(&shared)? {
        let stage = stage?;
        if !stage.file_type()?.is_dir() {
            continue;
        }
        let stage_name = stage.file_name().to_string_lossy().into_owned();
        for file in fs::read_dir(stage.path())? {
            let file = file?;
            let path = format!("shared/{stage_name}/{}", file.file_name().to_string_lossy());
            if referenced.contains(&path) {
                continue;
            }
            if CheckpointStorage::shared_file_checkpoint(&path).is_some_and(|created| created <= latest) {
                fs::remove_file(file.path())?;
                stats.files_removed += 1;
            }
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sluice_state::SharedFile;

    fn storage() -> (tempfile::TempDir, CheckpointStorage) {
        let dir = tempfile::TempDir::new().unwrap();
        let storage = CheckpointStorage::new(dir.path()).unwrap();
        (dir, storage)
    }

    /// Writes a checkpoint whose single stage references the given shared
    /// files (creating them if needed).
    fn checkpoint(storage: &CheckpointStorage, id: CheckpointId, shared: &[&str], complete: bool) {
        let mut sst_files = Vec::new();
        for path in shared {
            if !storage.resolve(path).exists() {
                storage.write(path, b"sst").unwrap();
            }
            sst_files.push(SharedFile {
                local_name: path.rsplit('-').next().unwrap().to_owned(),
                file: FileRef {
                    path: (*path).to_owned(),
                    size: 3,
                },
            });
        }
        let private = storage
            .write(&CheckpointStorage::private_path(id, "01-w", "MANIFEST"), b"m")
            .unwrap();
        let state = StageState {
            operator_state: None,
            keyed_state: Some(StateHandle::RocksDb {
                sst_files,
                private_files: vec![SharedFile {
                    local_name: "MANIFEST".into(),
                    file: private,
                }],
            }),
        };
        if complete {
            let manifest = CheckpointManifest {
                job: "j".into(),
                checkpoint_id: id,
                triggered_at: 0,
                completed_at: 0,
                stages: BTreeMap::from([("01-w".to_owned(), state)]),
            };
            write_manifest(storage, &manifest).unwrap();
        }
    }

    #[test]
    fn manifest_round_trip_and_discovery() {
        let (_dir, storage) = storage();
        checkpoint(&storage, 1, &["shared/01-w/0000000001-000005.sst"], true);
        checkpoint(&storage, 2, &[], false);
        assert_eq!(checkpoint_dirs(&storage).unwrap(), vec![1, 2]);
        assert_eq!(completed_checkpoints(&storage).unwrap(), vec![1]);
        let manifest = read_manifest(&storage, 1).unwrap();
        assert_eq!(manifest.size(), 4);
        assert_eq!(manifest.referenced_paths().count(), 2);
    }

    #[test]
    fn gc_keeps_retained_checkpoints_and_their_files() {
        let (_dir, storage) = storage();
        let a = "shared/01-w/0000000001-000005.sst"; // shared by 1, 2 and 3
        let b = "shared/01-w/0000000002-000007.sst"; // only in 2
        let c = "shared/01-w/0000000003-000009.sst"; // in 3
        let in_flight = "shared/01-w/0000000005-000011.sst"; // checkpoint 5 still running
        checkpoint(&storage, 1, &[a], true);
        checkpoint(&storage, 2, &[a, b], true);
        checkpoint(&storage, 4, &[], false); // failed
        checkpoint(&storage, 3, &[a, c], true);
        storage.write(in_flight, b"sst").unwrap();

        let stats = garbage_collect(&storage, 1).unwrap();
        assert_eq!(completed_checkpoints(&storage).unwrap(), vec![3]);
        assert_eq!(
            checkpoint_dirs(&storage).unwrap(),
            vec![3, 4],
            "4 is newer than the latest completed checkpoint, so it may still be running"
        );
        assert!(storage.resolve(a).exists() && storage.resolve(c).exists());
        assert!(!storage.resolve(b).exists());
        assert!(storage.resolve(in_flight).exists());
        assert_eq!(stats, GcStats { checkpoints_removed: 2, files_removed: 1 });

        // Restoring checkpoint 3 still works from what is left.
        for path in read_manifest(&storage, 3).unwrap().referenced_paths() {
            assert!(storage.resolve(path).exists(), "{path}");
        }

        // Once the attempt is over, the incomplete checkpoint 4 and the
        // in-flight file go as well.
        let stats = cleanup_incomplete(&storage, 1).unwrap();
        assert_eq!(checkpoint_dirs(&storage).unwrap(), vec![3]);
        assert!(!storage.resolve(in_flight).exists());
        assert!(storage.resolve(a).exists() && storage.resolve(c).exists());
        assert_eq!(stats, GcStats { checkpoints_removed: 1, files_removed: 1 });
    }

    #[test]
    fn cleanup_without_any_completed_checkpoint_removes_everything() {
        let (_dir, storage) = storage();
        checkpoint(&storage, 1, &["shared/01-w/0000000001-000005.sst"], false);
        assert_eq!(garbage_collect(&storage, 1).unwrap(), GcStats::default(), "1 may still be running");
        cleanup_incomplete(&storage, 1).unwrap();
        assert!(checkpoint_dirs(&storage).unwrap().is_empty());
        assert!(!storage.resolve("shared/01-w/0000000001-000005.sst").exists());
    }
}
