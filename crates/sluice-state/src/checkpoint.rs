//! Checkpoint storage layout and state handles.
//!
//! ```text
//! <root>/
//!   shared/<stage>/<chk>-<sst>   immutable RocksDB SST files, referenced by
//!                                every checkpoint that still contains them
//!   chk-<id>/
//!     _metadata.json             manifest, written last (marks completion)
//!     <stage>/...                files private to this checkpoint
//!   _work/                       live state of running attempts (not part
//!                                of any checkpoint)
//! ```

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use sluice_core::{CheckpointId, StateError};
use serde::{Deserialize, Serialize};

/// A file in checkpoint storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRef {
    /// Path relative to the storage root.
    pub path: String,
    pub size: u64,
}

/// A RocksDB file restored under `local_name` in the database directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedFile {
    pub local_name: String,
    pub file: FileRef,
}

/// Where a stage's keyed state for one checkpoint lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StateHandle {
    /// Full snapshot of an in-memory backend.
    Memory { file: FileRef },
    /// Incremental RocksDB checkpoint: SST files (possibly shared with
    /// earlier checkpoints) plus this checkpoint's metadata files
    /// (MANIFEST, CURRENT, OPTIONS).
    #[serde(rename = "rocksdb")]
    RocksDb {
        sst_files: Vec<SharedFile>,
        private_files: Vec<SharedFile>,
    },
}

impl StateHandle {
    /// The backend that wrote this snapshot (and can read it back).
    pub fn backend_kind(&self) -> crate::StateBackendKind {
        match self {
            StateHandle::Memory { .. } => crate::StateBackendKind::Memory,
            StateHandle::RocksDb { .. } => crate::StateBackendKind::RocksDb,
        }
    }

    /// Total bytes this checkpoint needs to restore the state.
    pub fn size(&self) -> u64 {
        match self {
            StateHandle::Memory { file } => file.size,
            StateHandle::RocksDb {
                sst_files,
                private_files,
            } => sst_files.iter().chain(private_files).map(|f| f.file.size).sum(),
        }
    }

    /// Every storage path this handle references.
    pub fn paths(&self) -> Vec<&str> {
        match self {
            StateHandle::Memory { file } => vec![file.path.as_str()],
            StateHandle::RocksDb {
                sst_files,
                private_files,
            } => sst_files.iter().chain(private_files).map(|f| f.file.path.as_str()).collect(),
        }
    }
}

/// Outcome of uploading one stage's snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotResult {
    pub handle: StateHandle,
    /// Bytes written by this checkpoint (the rest were already stored).
    pub uploaded_bytes: u64,
}

/// A directory on the local filesystem holding a job's checkpoints.
#[derive(Debug, Clone)]
pub struct CheckpointStorage {
    root: PathBuf,
}

impl CheckpointStorage {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("shared"))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn resolve(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    pub fn checkpoint_dir(&self, id: CheckpointId) -> PathBuf {
        self.root.join(Self::checkpoint_dir_name(id))
    }

    pub fn checkpoint_dir_name(id: CheckpointId) -> String {
        format!("chk-{id}")
    }

    /// Parses `chk-<id>` back into an id.
    pub fn parse_checkpoint_dir(name: &str) -> Option<CheckpointId> {
        name.strip_prefix("chk-")?.parse().ok()
    }

    /// Path of a file private to `stage` in checkpoint `id`.
    pub fn private_path(id: CheckpointId, stage: &str, name: &str) -> String {
        format!("{}/{stage}/{name}", Self::checkpoint_dir_name(id))
    }

    /// Path of an SST file first uploaded by checkpoint `id`. The id prefix
    /// keeps names unique across restarts, where RocksDB may reuse file
    /// numbers, and lets garbage collection tell old files from in-flight ones.
    pub fn shared_path(id: CheckpointId, stage: &str, name: &str) -> String {
        format!("shared/{stage}/{id:010}-{name}")
    }

    /// The checkpoint that first uploaded a shared file.
    pub fn shared_file_checkpoint(path: &str) -> Option<CheckpointId> {
        let name = path.rsplit('/').next()?;
        name.split('-').next()?.parse().ok()
    }

    /// Scratch space for running attempts.
    pub fn work_dir(&self) -> PathBuf {
        self.root.join("_work")
    }

    /// Writes `bytes` to `relative`, creating parent directories.
    pub fn write(&self, relative: &str, bytes: &[u8]) -> Result<FileRef, StateError> {
        let path = self.resolve(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_error("create", parent, e))?;
        }
        fs::write(&path, bytes).map_err(|e| io_error("write", &path, e))?;
        Ok(FileRef {
            path: relative.to_owned(),
            size: bytes.len() as u64,
        })
    }

    pub fn read(&self, file: &FileRef) -> Result<Vec<u8>, StateError> {
        let path = self.resolve(&file.path);
        fs::read(&path).map_err(|e| io_error("read", &path, e))
    }
}

pub(crate) fn io_error(action: &str, path: &Path, error: io::Error) -> StateError {
    StateError::new(format_args!("{action} {}: {error}", path.display()))
}

/// Hard-links `src` to `dst` (instant, no extra space), copying if the two
/// are on different filesystems.
pub(crate) fn link_or_copy(src: &Path, dst: &Path) -> Result<(), StateError> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).map_err(|e| io_error("create", parent, e))?;
    }
    let _ = fs::remove_file(dst);
    fs::hard_link(src, dst)
        .or_else(|_| fs::copy(src, dst).map(|_| ()))
        .map_err(|e| io_error("copy", src, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_conventions_round_trip() {
        assert_eq!(CheckpointStorage::parse_checkpoint_dir("chk-42"), Some(42));
        assert_eq!(CheckpointStorage::parse_checkpoint_dir("shared"), None);
        let shared = CheckpointStorage::shared_path(7, "03-window", "000012.sst");
        assert_eq!(shared, "shared/03-window/0000000007-000012.sst");
        assert_eq!(CheckpointStorage::shared_file_checkpoint(&shared), Some(7));
    }
}
