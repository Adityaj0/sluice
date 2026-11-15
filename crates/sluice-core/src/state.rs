//! The keyed-state interface operators program against. Implementations
//! (in-memory, RocksDB) live in `sluice-state`.

use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("state error: {0}")]
pub struct StateError(pub String);

impl StateError {
    pub fn new(message: impl Display) -> Self {
        Self(message.to_string())
    }
}

/// A key-value pair.
pub type Entry = (Vec<u8>, Vec<u8>);

/// Key-value pairs in key order, as returned by [`KeyedState::scan`].
pub type Entries = Vec<Entry>;

/// Ordered key-value state, partitioned into named states (one RocksDB
/// column family each). Keys within a state are ordered bytewise, so
/// operators can lay out keys for efficient range scans: windows prefix
/// their keys with the big-endian window end and fire with one scan.
pub trait KeyedState: Send {
    fn get(&self, state: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StateError>;

    fn put(&mut self, state: &str, key: &[u8], value: &[u8]) -> Result<(), StateError>;

    fn delete(&mut self, state: &str, key: &[u8]) -> Result<(), StateError>;

    /// Entries with `from <= key < to` in key order (`to = None`: no upper bound).
    fn scan(&self, state: &str, from: &[u8], to: Option<&[u8]>) -> Result<Entries, StateError>;

    /// Removes the entries [`scan`](Self::scan) would return.
    fn delete_range(&mut self, state: &str, from: &[u8], to: Option<&[u8]>) -> Result<(), StateError>;

    /// The first entry [`scan`](Self::scan) would return. Backends override
    /// this to avoid materialising the whole range.
    fn first(&self, state: &str, from: &[u8], to: Option<&[u8]>) -> Result<Option<Entry>, StateError> {
        Ok(self.scan(state, from, to)?.into_iter().next())
    }
}

/// The smallest key greater than every key starting with `prefix` (`None`
/// if there is none), for scanning a prefix as `[prefix, successor)`.
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// State for operators that declared none. Any access is a bug in the
/// operator, reported as an error rather than a panic.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoState;


impl NoState {
    fn err<T>(state: &str) -> Result<T, StateError> {
        Err(StateError(format!(
            "operator accessed state `{state}` without declaring it in `state_names`"
        )))
    }
}

impl KeyedState for NoState {
    fn get(&self, state: &str, _: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        Self::err(state)
    }
    fn put(&mut self, state: &str, _: &[u8], _: &[u8]) -> Result<(), StateError> {
        Self::err(state)
    }
    fn delete(&mut self, state: &str, _: &[u8]) -> Result<(), StateError> {
        Self::err(state)
    }
    fn scan(&self, state: &str, _: &[u8], _: Option<&[u8]>) -> Result<Entries, StateError> {
        Self::err(state)
    }
    fn delete_range(&mut self, state: &str, _: &[u8], _: Option<&[u8]>) -> Result<(), StateError> {
        Self::err(state)
    }
}

#[cfg(test)]
mod tests {
    use super::prefix_successor;

    #[test]
    fn prefix_successor_bounds_a_prefix() {
        assert_eq!(prefix_successor(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_successor(&[1, 0xFF]), Some(vec![2]));
        assert_eq!(prefix_successor(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_successor(&[]), None);
    }
}
