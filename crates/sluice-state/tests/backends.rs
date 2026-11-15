use sluice_state::{CheckpointStorage, StateBackend, StateBackendKind, StateHandle, create_backend};
use tempfile::TempDir;

const KINDS: [StateBackendKind; 2] = [StateBackendKind::Memory, StateBackendKind::RocksDb];

struct Env {
    _dir: TempDir,
    storage: CheckpointStorage,
}

impl Env {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let storage = CheckpointStorage::new(dir.path().join("checkpoints")).unwrap();
        Self { _dir: dir, storage }
    }

    fn backend(&self, kind: StateBackendKind, attempt: u32, restore: Option<&StateHandle>) -> Box<dyn StateBackend> {
        let work = self.storage.work_dir().join(format!("attempt-{attempt}"));
        create_backend(kind, "01-window", &["panes"], &work, Some(&self.storage), restore).unwrap()
    }
}

fn entries(state: &dyn StateBackend, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    state.scan(name, &[], None).unwrap()
}

#[test]
fn keyed_state_semantics() {
    for kind in KINDS {
        let env = Env::new();
        let mut s = env.backend(kind, 0, None);

        assert_eq!(s.get("panes", b"a").unwrap(), None, "{kind:?}");
        for key in [b"b", b"a", b"d", b"c"] {
            s.put("panes", key, key).unwrap();
        }
        s.put("panes", b"a", b"A").unwrap();
        s.put("other", b"a", b"x").unwrap(); // states are independent; created lazily
        assert_eq!(s.state_names(), ["other", "panes"], "{kind:?}");
        assert_eq!(s.get("panes", b"a").unwrap(), Some(b"A".to_vec()));
        assert_eq!(s.get("other", b"a").unwrap(), Some(b"x".to_vec()));
        assert_eq!(s.get("missing", b"a").unwrap(), None);

        let keys = |s: &dyn StateBackend, from: &[u8], to: Option<&[u8]>| -> Vec<Vec<u8>> {
            s.scan("panes", from, to).unwrap().into_iter().map(|(k, _)| k).collect()
        };
        assert_eq!(keys(&*s, b"", None), [b"a", b"b", b"c", b"d"].map(|k| k.to_vec()));
        assert_eq!(keys(&*s, b"b", Some(b"d")), [b"b", b"c"].map(|k| k.to_vec()));
        assert!(keys(&*s, b"c", Some(b"b")).is_empty());

        s.delete_range("panes", b"b", Some(b"d")).unwrap();
        assert_eq!(keys(&*s, b"", None), [b"a", b"d"].map(|k| k.to_vec()));
        s.delete("panes", b"a").unwrap();
        s.delete_range("panes", b"c", None).unwrap();
        assert!(keys(&*s, b"", None).is_empty(), "{kind:?}");
        assert_eq!(s.get("other", b"a").unwrap(), Some(b"x".to_vec()));
    }
}

#[test]
fn snapshot_restores_state_as_of_the_snapshot() {
    for kind in KINDS {
        let env = Env::new();
        let mut s = env.backend(kind, 0, None);
        for i in 0..1_000u32 {
            s.put("panes", &i.to_be_bytes(), &(i * 2).to_be_bytes()).unwrap();
        }
        let pending = s.snapshot(1).unwrap();

        // Changes after the synchronous phase must not leak into the snapshot.
        s.delete_range("panes", &[], None).unwrap();
        s.put("panes", b"late", b"!").unwrap();

        let result = pending.upload(&env.storage).unwrap();
        assert!(result.handle.size() > 0);
        drop(s);

        let restored = env.backend(kind, 1, Some(&result.handle));
        let data = entries(&*restored, "panes");
        assert_eq!(data.len(), 1_000, "{kind:?}");
        assert_eq!(data[7], (7u32.to_be_bytes().to_vec(), 14u32.to_be_bytes().to_vec()));
        assert_eq!(restored.get("panes", b"late").unwrap(), None);
    }
}

#[test]
fn rocksdb_checkpoints_are_incremental() {
    let env = Env::new();
    let mut s = env.backend(StateBackendKind::RocksDb, 0, None);
    for i in 0..5_000u32 {
        s.put("panes", &i.to_be_bytes(), &[7; 100]).unwrap();
    }
    let first = s.snapshot(1).unwrap().upload(&env.storage).unwrap();
    assert_eq!(first.uploaded_bytes, first.handle.size(), "first checkpoint uploads everything");

    s.put("panes", b"one-more", b"x").unwrap();
    let second = s.snapshot(2).unwrap().upload(&env.storage).unwrap();
    let StateHandle::RocksDb { sst_files, .. } = &second.handle else {
        panic!("expected a rocksdb handle")
    };
    assert!(sst_files.len() >= 2, "old SST plus the new flush");
    assert!(
        sst_files.iter().any(|f| f.file.path.contains("/0000000001-")),
        "the SST uploaded by checkpoint 1 is referenced, not re-uploaded"
    );
    assert!(
        second.uploaded_bytes < first.uploaded_bytes / 2,
        "second checkpoint uploaded {} bytes vs {} total",
        second.uploaded_bytes,
        second.handle.size()
    );

    drop(s);
    let restored = env.backend(StateBackendKind::RocksDb, 1, Some(&second.handle));
    assert_eq!(entries(&*restored, "panes").len(), 5_001);

    // A restored backend keeps sharing the files it was restored from.
    let mut restored = restored;
    restored.put("panes", b"after-restore", b"y").unwrap();
    let third = restored.snapshot(3).unwrap().upload(&env.storage).unwrap();
    assert!(third.uploaded_bytes < first.uploaded_bytes / 2);
}

#[test]
fn restoring_the_wrong_kind_fails_cleanly() {
    let env = Env::new();
    let mut s = env.backend(StateBackendKind::Memory, 0, None);
    s.put("panes", b"k", b"v").unwrap();
    let handle = s.snapshot(1).unwrap().upload(&env.storage).unwrap().handle;
    let work = env.storage.work_dir().join("x");
    let err = create_backend(StateBackendKind::RocksDb, "01-window", &[], &work, Some(&env.storage), Some(&handle))
        .err()
        .unwrap();
    assert!(err.0.contains("memory snapshot"), "{err}");
}

#[test]
fn first_returns_the_smallest_key_in_range() {
    for kind in KINDS {
        let env = Env::new();
        let mut s = env.backend(kind, 0, None);
        assert_eq!(s.first("panes", b"", None).unwrap(), None);
        for key in [b"c", b"a", b"e"] {
            s.put("panes", key, b"v").unwrap();
        }
        let first = |s: &dyn StateBackend, from: &[u8], to: Option<&[u8]>| s.first("panes", from, to).unwrap().map(|(k, _)| k);
        assert_eq!(first(&*s, b"", None), Some(b"a".to_vec()), "{kind:?}");
        assert_eq!(first(&*s, b"b", None), Some(b"c".to_vec()));
        assert_eq!(first(&*s, b"b", Some(b"c")), None);
        assert_eq!(first(&*s, b"f", None), None);
    }
}
