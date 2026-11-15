use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use sluice_core::{CheckpointId, Entries, Entry, KeyedState, StateError, decode, encode};

use crate::backend::{PendingSnapshot, StateBackend};
use crate::checkpoint::{CheckpointStorage, SnapshotResult, StateHandle};

type Table = BTreeMap<Vec<u8>, Vec<u8>>;

/// Keyed state in ordered in-memory maps. Fast, but bounded by memory, and
/// every checkpoint copies and writes out the whole state.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    stage: String,
    states: HashMap<String, Table>,
}

impl MemoryBackend {
    pub fn new(stage: &str) -> Self {
        Self {
            stage: stage.to_owned(),
            states: HashMap::new(),
        }
    }

    pub fn open(stage: &str, restore: Option<(&StateHandle, &CheckpointStorage)>) -> Result<Self, StateError> {
        let mut backend = Self::new(stage);
        match restore {
            None => {}
            Some((StateHandle::Memory { file }, storage)) => {
                let bytes = storage.read(file)?;
                let states: BTreeMap<String, Table> =
                    decode(&bytes).map_err(|e| StateError::new(format_args!("decoding {}: {e}", file.path)))?;
                backend.states = states.into_iter().collect();
            }
            Some((other, _)) => {
                return Err(StateError::new(format_args!(
                    "stage {stage}: cannot restore a {} snapshot into the memory backend",
                    kind_name(other)
                )));
            }
        }
        Ok(backend)
    }

    /// Number of entries in `state`.
    pub fn len(&self, state: &str) -> usize {
        self.states.get(state).map_or(0, BTreeMap::len)
    }
}

pub(crate) fn kind_name(handle: &StateHandle) -> &'static str {
    match handle {
        StateHandle::Memory { .. } => "memory",
        StateHandle::RocksDb { .. } => "rocksdb",
    }
}

fn range_bounds<'a>(from: &'a [u8], to: Option<&'a [u8]>) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
    (Bound::Included(from), to.map_or(Bound::Unbounded, Bound::Excluded))
}

impl KeyedState for MemoryBackend {
    fn get(&self, state: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        Ok(self.states.get(state).and_then(|t| t.get(key)).cloned())
    }

    fn put(&mut self, state: &str, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        let table = match self.states.get_mut(state) {
            Some(table) => table,
            None => self.states.entry(state.to_owned()).or_default(),
        };
        table.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&mut self, state: &str, key: &[u8]) -> Result<(), StateError> {
        if let Some(table) = self.states.get_mut(state) {
            table.remove(key);
        }
        Ok(())
    }

    fn scan(&self, state: &str, from: &[u8], to: Option<&[u8]>) -> Result<Entries, StateError> {
        if to.is_some_and(|to| to <= from) {
            return Ok(Vec::new());
        }
        let Some(table) = self.states.get(state) else {
            return Ok(Vec::new());
        };
        Ok(table
            .range::<[u8], _>(range_bounds(from, to))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn first(&self, state: &str, from: &[u8], to: Option<&[u8]>) -> Result<Option<Entry>, StateError> {
        if to.is_some_and(|to| to <= from) {
            return Ok(None);
        }
        Ok(self
            .states
            .get(state)
            .and_then(|t| t.range::<[u8], _>(range_bounds(from, to)).next())
            .map(|(k, v)| (k.clone(), v.clone())))
    }

    fn delete_range(&mut self, state: &str, from: &[u8], to: Option<&[u8]>) -> Result<(), StateError> {
        if to.is_some_and(|to| to <= from) {
            return Ok(());
        }
        if let Some(table) = self.states.get_mut(state) {
            let mut tail = table.split_off(from);
            if let Some(to) = to {
                table.append(&mut tail.split_off(to));
            }
        }
        Ok(())
    }
}

struct MemorySnapshot {
    stage: String,
    checkpoint_id: CheckpointId,
    states: BTreeMap<String, Table>,
}

impl StateBackend for MemoryBackend {
    fn state_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.states.iter().filter(|(_, t)| !t.is_empty()).map(|(n, _)| n.clone()).collect();
        names.sort();
        names
    }

    fn snapshot(&mut self, checkpoint_id: CheckpointId) -> Result<Box<dyn PendingSnapshot>, StateError> {
        Ok(Box::new(MemorySnapshot {
            stage: self.stage.clone(),
            checkpoint_id,
            states: self.states.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        }))
    }
}

impl PendingSnapshot for MemorySnapshot {
    fn upload(self: Box<Self>, storage: &CheckpointStorage) -> Result<SnapshotResult, StateError> {
        let bytes = encode(&self.states).map_err(StateError::new)?;
        let path = CheckpointStorage::private_path(self.checkpoint_id, &self.stage, "keyed-state.bin");
        let file = storage.write(&path, &bytes)?;
        Ok(SnapshotResult {
            uploaded_bytes: file.size,
            handle: StateHandle::Memory { file },
        })
    }
}
