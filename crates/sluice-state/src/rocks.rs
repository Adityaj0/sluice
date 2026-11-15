use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use sluice_core::{CheckpointId, Entries, Entry, KeyedState, StateError};
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{ColumnFamily, DB, Direction, IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions};

use crate::backend::{PendingSnapshot, StateBackend};
use crate::checkpoint::{CheckpointStorage, FileRef, SharedFile, SnapshotResult, StateHandle, io_error, link_or_copy};
use crate::memory::kind_name;

/// SST files already in checkpoint storage, by local file name.
type Uploaded = Arc<Mutex<HashMap<String, FileRef>>>;

/// Keyed state in an embedded RocksDB instance, one per stateful operator,
/// with a column family per named state.
///
/// Writes skip the write-ahead log: durability comes from checkpoints, not
/// from RocksDB, and a snapshot flushes memtables first.
pub struct RocksDbBackend {
    stage: String,
    db: DB,
    path: PathBuf,
    cf_options: Options,
    write_options: WriteOptions,
    uploaded: Uploaded,
}

fn db_error(error: rocksdb::Error) -> StateError {
    StateError::new(format_args!("rocksdb: {error}"))
}

impl RocksDbBackend {
    /// Opens a fresh database for `stage` under `work_dir`, or rebuilds it
    /// from a checkpoint.
    pub fn open(
        stage: &str,
        states: &[&str],
        work_dir: &Path,
        restore: Option<(&StateHandle, &CheckpointStorage)>,
    ) -> Result<Self, StateError> {
        let path = work_dir.join(stage).join("db");
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|e| io_error("clear", &path, e))?;
        }
        fs::create_dir_all(&path).map_err(|e| io_error("create", &path, e))?;

        let mut uploaded = HashMap::new();
        match restore {
            None => {}
            Some((
                StateHandle::RocksDb {
                    sst_files,
                    private_files,
                },
                storage,
            )) => {
                // SST files are immutable, so linking them is safe; RocksDB
                // rewrites metadata files, so those are copied.
                for sst in sst_files {
                    link_or_copy(&storage.resolve(&sst.file.path), &path.join(&sst.local_name))?;
                    uploaded.insert(sst.local_name.clone(), sst.file.clone());
                }
                for file in private_files {
                    let src = storage.resolve(&file.file.path);
                    let dst = path.join(&file.local_name);
                    fs::copy(&src, &dst).map_err(|e| io_error("copy", &src, e))?;
                }
            }
            Some((other, _)) => {
                return Err(StateError::new(format_args!(
                    "stage {stage}: cannot restore a {} snapshot into the rocksdb backend",
                    kind_name(other)
                )));
            }
        }

        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let mut families: Vec<String> = if restore.is_some() {
            DB::list_cf(&options, &path).map_err(db_error)?
        } else {
            vec!["default".to_owned()]
        };
        for state in states {
            if !families.iter().any(|f| f == state) {
                families.push((*state).to_owned());
            }
        }
        let db = DB::open_cf(&options, &path, &families).map_err(db_error)?;

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);

        Ok(Self {
            stage: stage.to_owned(),
            db,
            path,
            cf_options: Options::default(),
            write_options,
            uploaded: Arc::new(Mutex::new(uploaded)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn cf(&self, state: &str) -> Option<&ColumnFamily> {
        self.db.cf_handle(state)
    }

    fn ensure_cf(&mut self, state: &str) -> Result<(), StateError> {
        if self.db.cf_handle(state).is_none() {
            self.db.create_cf(state, &self.cf_options).map_err(db_error)?;
        }
        Ok(())
    }

    fn keys_in_range(&self, cf: &ColumnFamily, from: &[u8], to: Option<&[u8]>) -> Result<Vec<Box<[u8]>>, StateError> {
        let mut read_options = ReadOptions::default();
        if let Some(to) = to {
            read_options.set_iterate_upper_bound(to.to_vec());
        }
        self.db
            .iterator_cf_opt(cf, read_options, IteratorMode::From(from, Direction::Forward))
            .map(|item| item.map(|(k, _)| k).map_err(db_error))
            .collect()
    }
}

impl KeyedState for RocksDbBackend {
    fn get(&self, state: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        match self.cf(state) {
            Some(cf) => self.db.get_cf(cf, key).map_err(db_error),
            None => Ok(None),
        }
    }

    fn put(&mut self, state: &str, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        self.ensure_cf(state)?;
        let cf = self.cf(state).expect("ensured above");
        self.db.put_cf_opt(cf, key, value, &self.write_options).map_err(db_error)
    }

    fn delete(&mut self, state: &str, key: &[u8]) -> Result<(), StateError> {
        match self.cf(state) {
            Some(cf) => self.db.delete_cf_opt(cf, key, &self.write_options).map_err(db_error),
            None => Ok(()),
        }
    }

    fn scan(&self, state: &str, from: &[u8], to: Option<&[u8]>) -> Result<Entries, StateError> {
        if to.is_some_and(|to| to <= from) {
            return Ok(Vec::new());
        }
        let Some(cf) = self.cf(state) else {
            return Ok(Vec::new());
        };
        let mut read_options = ReadOptions::default();
        if let Some(to) = to {
            read_options.set_iterate_upper_bound(to.to_vec());
        }
        self.db
            .iterator_cf_opt(cf, read_options, IteratorMode::From(from, Direction::Forward))
            .map(|item| item.map(|(k, v)| (k.into_vec(), v.into_vec())).map_err(db_error))
            .collect()
    }

    fn first(&self, state: &str, from: &[u8], to: Option<&[u8]>) -> Result<Option<Entry>, StateError> {
        if to.is_some_and(|to| to <= from) {
            return Ok(None);
        }
        let Some(cf) = self.cf(state) else {
            return Ok(None);
        };
        let mut read_options = ReadOptions::default();
        if let Some(to) = to {
            read_options.set_iterate_upper_bound(to.to_vec());
        }
        self.db
            .iterator_cf_opt(cf, read_options, IteratorMode::From(from, Direction::Forward))
            .next()
            .transpose()
            .map(|entry| entry.map(|(k, v)| (k.into_vec(), v.into_vec())))
            .map_err(db_error)
    }

    fn delete_range(&mut self, state: &str, from: &[u8], to: Option<&[u8]>) -> Result<(), StateError> {
        if to.is_some_and(|to| to <= from) {
            return Ok(());
        }
        let Some(cf) = self.cf(state) else {
            return Ok(());
        };
        let mut batch = WriteBatch::default();
        match to {
            Some(to) => batch.delete_range_cf(cf, from, to),
            None => {
                for key in self.keys_in_range(cf, from, None)? {
                    batch.delete_cf(cf, key);
                }
            }
        }
        self.db.write_opt(batch, &self.write_options).map_err(db_error)
    }
}

impl StateBackend for RocksDbBackend {
    fn state_names(&self) -> Vec<String> {
        let mut names: Vec<_> = DB::list_cf(&Options::default(), &self.path)
            .unwrap_or_default()
            .into_iter()
            .filter(|name| name != "default")
            .collect();
        names.sort();
        names
    }

    fn snapshot(&mut self, checkpoint_id: CheckpointId) -> Result<Box<dyn PendingSnapshot>, StateError> {
        // Writes bypass the WAL, so memtables must reach SST files first.
        let families = DB::list_cf(&Options::default(), &self.path).map_err(db_error)?;
        for name in &families {
            if let Some(cf) = self.db.cf_handle(name) {
                self.db.flush_cf(cf).map_err(db_error)?;
            }
        }

        let staging = self.path.with_file_name(format!("snapshot-{checkpoint_id}"));
        if staging.exists() {
            fs::remove_dir_all(&staging).map_err(|e| io_error("clear", &staging, e))?;
        }
        Checkpoint::new(&self.db)
            .and_then(|c| c.create_checkpoint(&staging))
            .map_err(db_error)?;

        Ok(Box::new(RocksDbSnapshot {
            stage: self.stage.clone(),
            checkpoint_id,
            dir: staging,
            uploaded: Arc::clone(&self.uploaded),
        }))
    }
}

/// A RocksDB checkpoint (hard links of the live SST files plus copied
/// metadata) waiting to be uploaded.
struct RocksDbSnapshot {
    stage: String,
    checkpoint_id: CheckpointId,
    dir: PathBuf,
    uploaded: Uploaded,
}

impl PendingSnapshot for RocksDbSnapshot {
    fn upload(self: Box<Self>, storage: &CheckpointStorage) -> Result<SnapshotResult, StateError> {
        let result = self.upload_files(storage);
        let _ = fs::remove_dir_all(&self.dir);
        result
    }
}

impl RocksDbSnapshot {
    fn upload_files(&self, storage: &CheckpointStorage) -> Result<SnapshotResult, StateError> {
        let known = self.uploaded.lock().unwrap_or_else(PoisonError::into_inner).clone();

        let mut entries: Vec<_> = fs::read_dir(&self.dir)
            .map_err(|e| io_error("list", &self.dir, e))?
            .collect::<Result<_, _>>()
            .map_err(|e| io_error("list", &self.dir, e))?;
        entries.sort_by_key(|e| e.file_name());

        let mut sst_files = Vec::new();
        let mut private_files = Vec::new();
        let mut uploaded_bytes = 0;
        let mut current = HashMap::new();

        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            let src = entry.path();
            let size = entry.metadata().map_err(|e| io_error("stat", &src, e))?.len();

            if name.ends_with(".sst") {
                let file = match known.get(&name) {
                    Some(file) => file.clone(),
                    None => {
                        let path = CheckpointStorage::shared_path(self.checkpoint_id, &self.stage, &name);
                        link_or_copy(&src, &storage.resolve(&path))?;
                        uploaded_bytes += size;
                        FileRef { path, size }
                    }
                };
                current.insert(name.clone(), file.clone());
                sst_files.push(SharedFile { local_name: name, file });
            } else {
                let path = CheckpointStorage::private_path(self.checkpoint_id, &self.stage, &name);
                link_or_copy(&src, &storage.resolve(&path))?;
                uploaded_bytes += size;
                private_files.push(SharedFile {
                    local_name: name,
                    file: FileRef { path, size },
                });
            }
        }

        // Forget files compaction has removed; they can never be reused.
        *self.uploaded.lock().unwrap_or_else(PoisonError::into_inner) = current;

        Ok(SnapshotResult {
            handle: StateHandle::RocksDb {
                sst_files,
                private_files,
            },
            uploaded_bytes,
        })
    }
}
