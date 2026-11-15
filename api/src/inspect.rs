//! Reads a job's state as of a retained checkpoint, for the time-travel
//! debugger: source offsets, operator watermarks, and every window's
//! accumulators by key.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use sluice_checkpoint::manifest::{completed_checkpoints, read_manifest};
use sluice_core::CheckpointId;
use sluice_runtime::window::{AggregateFunction, SESSION_STATE, decode_operator_state, decode_state_entry};
use sluice_runtime::{StageInfo, decode_source_checkpoint};
use sluice_sql::physical::SqlAcc;
use sluice_state::{CheckpointStorage, create_backend};
use serde_json::{Value, json};

use crate::job_def::{AggregateDef, JobDefinition, JsonAcc};

/// Completed checkpoints still on disk, ascending.
pub fn retained(dir: &Path) -> Vec<CheckpointId> {
    CheckpointStorage::new(dir)
        .ok()
        .and_then(|storage| completed_checkpoints(&storage).ok())
        .unwrap_or_default()
}

/// How a stage's accumulators are decoded.
enum Decoder {
    Json(AggregateDef),
    Sql,
}

impl Decoder {
    fn accumulator(&self, state: &str, key: &[u8], value: &[u8]) -> Result<Option<Value>, String> {
        let window = |w: sluice_runtime::window::TimeWindow| json!({ "start": w.start, "end": w.end });
        match self {
            Decoder::Json(aggregate) => {
                let Some(entry) = decode_state_entry::<JsonAcc>(state, key, value).map_err(|e| e.to_string())? else {
                    return Ok(None);
                };
                Ok(Some(json!({
                    "kind": entry.kind,
                    "window": window(entry.window),
                    "key": String::from_utf8_lossy(&entry.key),
                    "fired": entry.fired,
                    "value": aggregate.result(entry.accumulator.clone()),
                    "accumulator": entry.accumulator,
                })))
            }
            Decoder::Sql => {
                let Some(entry) = decode_state_entry::<SqlAcc>(state, key, value).map_err(|e| e.to_string())? else {
                    return Ok(None);
                };
                Ok(Some(json!({
                    "kind": entry.kind,
                    "window": window(entry.window),
                    "key": String::from_utf8_lossy(&entry.key),
                    "fired": entry.fired,
                    "value": entry.accumulator.describe(),
                })))
            }
        }
    }
}

static INSPECTIONS: AtomicU64 = AtomicU64::new(0);

/// The state of every stage as of `checkpoint`. Blocking (reads files and
/// may open RocksDB); call from a blocking thread.
pub fn inspect_checkpoint(
    definition: &JobDefinition,
    stages: &[StageInfo],
    stage_names: &[String],
    checkpoint_dir: &Path,
    scratch_dir: &Path,
    checkpoint: CheckpointId,
    limit: usize,
) -> Result<Value, String> {
    let storage = CheckpointStorage::new(checkpoint_dir).map_err(|e| e.to_string())?;
    let manifest = read_manifest(&storage, checkpoint).map_err(|_| {
        let retained = retained(checkpoint_dir);
        format!("checkpoint {checkpoint} is not on disk; retained checkpoints: {retained:?}")
    })?;
    let window_aggregates = definition.window_aggregates();

    let mut out = Vec::new();
    for (index, info) in stages.iter().enumerate() {
        let name = stage_names.get(index).cloned().unwrap_or_default();
        let Some(state) = manifest.stages.get(&info.uid) else { continue };
        let mut stage = json!({ "id": index, "uid": info.uid, "name": name, "size": state.size() });

        if let Some(file) = &state.operator_state {
            let bytes = storage.read(file).map_err(|e| e.to_string())?;
            let described = if info.is_source {
                decode_source_checkpoint(&bytes).map(|s| json!({ "source": s }))
            } else if name.starts_with("window") {
                let sessions = name.contains("session") || name.contains("SESSION");
                decode_operator_state(&bytes, sessions).map(|w| json!({ "window": w }))
            } else {
                None
            };
            stage["operator_state"] = described.unwrap_or_else(|| json!({ "bytes": bytes.len() }));
        }

        if let Some(handle) = &state.keyed_state {
            let decoder = if definition.query.is_some() {
                Decoder::Sql
            } else {
                match window_aggregates.iter().find(|(stage_name, _)| *stage_name == name) {
                    Some((_, aggregate)) => Decoder::Json(aggregate.clone()),
                    None => continue,
                }
            };
            let work = scratch_dir.join(format!("inspect-{}-{}", std::process::id(), INSPECTIONS.fetch_add(1, Ordering::Relaxed)));
            let result = (|| -> Result<Value, String> {
                let backend = create_backend(handle.backend_kind(), &info.uid, &[], &work, Some(&storage), Some(handle))
                    .map_err(|e| e.to_string())?;
                let mut states = Vec::new();
                for state_name in backend.state_names() {
                    let entries = backend.scan(&state_name, &[], None).map_err(|e| e.to_string())?;
                    let total = entries.len();
                    let mut decoded = Vec::new();
                    for (key, value) in entries.iter().take(limit) {
                        if let Some(entry) = decoder.accumulator(&state_name, key, value)? {
                            decoded.push(entry);
                        }
                    }
                    let internal = decoded.is_empty() && total > 0;
                    states.push(json!({
                        "name": state_name,
                        "kind": if state_name == SESSION_STATE { "sessions" } else { "windows" },
                        "total": total,
                        "truncated": total > limit,
                        "internal": internal,
                        "entries": decoded,
                    }));
                }
                Ok(json!({ "backend": handle.backend_kind(), "states": states }))
            })();
            let _ = std::fs::remove_dir_all(&work);
            stage["keyed_state"] = result?;
        }
        out.push(stage);
    }

    Ok(json!({
        "checkpoint": checkpoint,
        "triggered_at": manifest.triggered_at,
        "completed_at": manifest.completed_at,
        "size": manifest.size(),
        "stages": out,
    }))
}
