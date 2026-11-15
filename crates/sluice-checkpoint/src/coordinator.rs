use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sluice_core::{CheckpointBarrier, CheckpointId, Timestamp, now_millis};
use sluice_state::{CheckpointStorage, StateBackendKind};
use serde::Serialize;
use tokio::sync::{broadcast, mpsc};
use tokio::time::MissedTickBehavior;

use crate::manifest::{CheckpointManifest, StageState, garbage_collect, write_manifest};

/// Checkpointing settings for a job.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Where this job's checkpoints are stored.
    pub dir: PathBuf,
    /// Time between the end of one checkpoint and the trigger of the next.
    pub interval: Duration,
    /// A checkpoint not completed within this time is aborted.
    pub timeout: Duration,
    /// Number of completed checkpoints kept on disk.
    pub retained: usize,
    pub backend: StateBackendKind,
    /// Restore from the newest checkpoint already in `dir` when the job
    /// starts (resuming a previous run), instead of starting empty.
    pub resume: bool,
}

impl CheckpointConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(60),
            retained: 3,
            backend: StateBackendKind::RocksDb,
            resume: false,
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_retained(mut self, retained: usize) -> Self {
        self.retained = retained.max(1);
        self
    }

    pub fn with_backend(mut self, backend: StateBackendKind) -> Self {
        self.backend = backend;
        self
    }

    pub fn resuming(mut self) -> Self {
        self.resume = true;
        self
    }
}

/// Coordinator → stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlMessage {
    /// Sources: inject this barrier.
    Trigger(CheckpointBarrier),
    /// Every stage acknowledged and the manifest is durable. Sinks commit
    /// output up to this checkpoint; sources may discard replay data.
    Complete(CheckpointId),
}

/// Stages → coordinator.
#[derive(Debug)]
pub enum StageReport {
    Ack(StageAck),
    /// The stage could not snapshot; the checkpoint is aborted.
    Decline {
        checkpoint_id: CheckpointId,
        stage: usize,
        reason: String,
    },
    /// A source reached the end of its input. It can no longer inject
    /// barriers, so no further checkpoints can complete.
    SourceFinished { stage: usize },
}

#[derive(Debug)]
pub struct StageAck {
    pub checkpoint_id: CheckpointId,
    pub stage: usize,
    pub state: StageState,
    pub stats: StageCheckpointStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StageInfo {
    /// Stable identity across restarts: `<index>-<name>`.
    pub uid: String,
    pub is_source: bool,
    /// Indices of the stages feeding this one (the pipeline's edges).
    pub inputs: Vec<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct StageCheckpointStats {
    pub stage: String,
    /// Time between the first and last barrier arriving (inputs paused).
    pub alignment_ms: u64,
    /// Synchronous snapshot time, during which the stage processes nothing.
    pub sync_ms: u64,
    /// Upload time, overlapped with processing.
    pub async_ms: u64,
    pub state_size: u64,
    pub uploaded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CheckpointStatus {
    InProgress,
    Completed,
    Failed { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckpointRecord {
    pub id: CheckpointId,
    pub status: CheckpointStatus,
    pub triggered_at: Timestamp,
    pub duration_ms: Option<u64>,
    /// Total bytes needed to restore from this checkpoint.
    pub state_size: u64,
    /// Bytes this checkpoint actually wrote (incremental checkpoints reuse
    /// files written by earlier ones).
    pub uploaded_bytes: u64,
    pub stages: Vec<StageCheckpointStats>,
}

/// Recent checkpoints of a job, across restarts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CheckpointHistory {
    pub completed: u64,
    pub failed: u64,
    pub latest_completed: Option<CheckpointId>,
    /// The checkpoint the current attempt was restored from.
    pub restored_from: Option<CheckpointId>,
    /// Oldest first, at most [`CheckpointHistory::MAX_RECORDS`].
    pub records: VecDeque<CheckpointRecord>,
}

impl CheckpointHistory {
    pub const MAX_RECORDS: usize = 100;

    pub fn record(&self, id: CheckpointId) -> Option<&CheckpointRecord> {
        self.records.iter().rev().find(|r| r.id == id)
    }

    fn record_mut(&mut self, id: CheckpointId) -> Option<&mut CheckpointRecord> {
        self.records.iter_mut().rev().find(|r| r.id == id)
    }

    fn start(&mut self, id: CheckpointId, triggered_at: Timestamp) {
        if self.records.len() == Self::MAX_RECORDS {
            self.records.pop_front();
        }
        self.records.push_back(CheckpointRecord {
            id,
            status: CheckpointStatus::InProgress,
            triggered_at,
            duration_ms: None,
            state_size: 0,
            uploaded_bytes: 0,
            stages: Vec::new(),
        });
    }

    fn fail(&mut self, id: CheckpointId, reason: String, duration: Duration) {
        if let Some(record) = self.record_mut(id) {
            record.status = CheckpointStatus::Failed { reason };
            record.duration_ms = Some(duration.as_millis() as u64);
            self.failed += 1;
        }
    }

    /// Records that the job (re)started from `checkpoint`. A durable
    /// manifest is what makes a checkpoint complete, so if the attempt died
    /// between writing the manifest and recording completion, the record is
    /// corrected here.
    pub fn restored(&mut self, checkpoint: Option<CheckpointId>) {
        self.restored_from = checkpoint;
        let Some(id) = checkpoint else { return };
        if let Some(record) = self.record_mut(id) {
            if record.status != CheckpointStatus::Completed {
                let was_failed = matches!(record.status, CheckpointStatus::Failed { .. });
                record.status = CheckpointStatus::Completed;
                self.completed += 1;
                if was_failed {
                    self.failed = self.failed.saturating_sub(1);
                }
            }
        }
        self.latest_completed = self.latest_completed.max(Some(id));
    }

    /// Marks every in-progress checkpoint failed (the attempt running them
    /// is gone).
    pub fn abort_in_progress(&mut self, reason: &str) {
        let mut aborted = 0;
        for record in &mut self.records {
            if record.status == CheckpointStatus::InProgress {
                record.status = CheckpointStatus::Failed {
                    reason: reason.to_owned(),
                };
                aborted += 1;
            }
        }
        self.failed += aborted;
    }
}

struct Pending {
    barrier: CheckpointBarrier,
    started: Instant,
    deadline: tokio::time::Instant,
    acks: BTreeMap<usize, StageAck>,
}

/// Drives the checkpoints of one job attempt. At most one checkpoint is in
/// flight at a time.
pub struct CheckpointCoordinator {
    job: String,
    config: CheckpointConfig,
    storage: CheckpointStorage,
    stages: Vec<StageInfo>,
    control: broadcast::Sender<ControlMessage>,
    reports: mpsc::UnboundedReceiver<StageReport>,
    history: Arc<Mutex<CheckpointHistory>>,
    next_id: Arc<AtomicU64>,
}

impl CheckpointCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job: String,
        config: CheckpointConfig,
        storage: CheckpointStorage,
        stages: Vec<StageInfo>,
        control: broadcast::Sender<ControlMessage>,
        reports: mpsc::UnboundedReceiver<StageReport>,
        history: Arc<Mutex<CheckpointHistory>>,
        next_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            job,
            config,
            storage,
            stages,
            control,
            reports,
            history,
            next_id,
        }
    }

    fn history(&self) -> std::sync::MutexGuard<'_, CheckpointHistory> {
        self.history.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs until every stage has dropped its report channel (or the task
    /// is aborted when the attempt ends).
    pub async fn run(mut self) {
        let first = tokio::time::Instant::now() + self.config.interval;
        let mut ticker = tokio::time::interval_at(first, self.config.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut pending: Option<Pending> = None;
        let mut sources_finished = false;

        loop {
            let deadline = pending.as_ref().map(|p| p.deadline);
            tokio::select! {
                _ = ticker.tick() => {
                    if pending.is_none() && !sources_finished {
                        pending = Some(self.trigger());
                    }
                }
                report = self.reports.recv() => {
                    let Some(report) = report else { break };
                    match report {
                        StageReport::Ack(ack) => {
                            let Some(p) = pending.as_mut().filter(|p| p.barrier.checkpoint_id == ack.checkpoint_id) else {
                                continue; // late ack for an aborted checkpoint
                            };
                            p.acks.insert(ack.stage, ack);
                            if p.acks.len() == self.stages.len() {
                                let p = pending.take().expect("checked above");
                                self.complete(p).await;
                                ticker.reset();
                            }
                        }
                        StageReport::Decline { checkpoint_id, stage, reason } => {
                            if let Some(p) = pending.take_if(|p| p.barrier.checkpoint_id == checkpoint_id) {
                                let reason = format!("stage {} declined: {reason}", self.stage_name(stage));
                                self.fail(p, reason).await;
                            }
                        }
                        StageReport::SourceFinished { stage } => {
                            sources_finished = true;
                            if let Some(p) = pending.take_if(|p| !p.acks.contains_key(&stage)) {
                                let reason = format!("source {} finished before the barrier", self.stage_name(stage));
                                self.fail(p, reason).await;
                            }
                        }
                    }
                }
                _ = sleep_until(deadline) => {
                    if let Some(p) = pending.take() {
                        let waiting: Vec<_> = self
                            .stages
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| !p.acks.contains_key(i))
                            .map(|(_, s)| s.uid.as_str())
                            .collect();
                        let reason = format!(
                            "timed out after {:?} waiting for {}",
                            self.config.timeout,
                            waiting.join(", ")
                        );
                        self.fail(p, reason).await;
                    }
                }
            }
        }
    }

    fn stage_name(&self, stage: usize) -> &str {
        self.stages.get(stage).map_or("?", |s| s.uid.as_str())
    }

    fn trigger(&self) -> Pending {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let barrier = CheckpointBarrier {
            checkpoint_id: id,
            timestamp: now_millis(),
        };
        self.history().start(id, barrier.timestamp);
        tracing::debug!(job = %self.job, checkpoint = id, "checkpoint triggered");
        let _ = self.control.send(ControlMessage::Trigger(barrier));
        Pending {
            barrier,
            started: Instant::now(),
            deadline: tokio::time::Instant::now() + self.config.timeout,
            acks: BTreeMap::new(),
        }
    }

    async fn complete(&mut self, pending: Pending) {
        let id = pending.barrier.checkpoint_id;
        let mut stages = BTreeMap::new();
        let mut stats = Vec::with_capacity(pending.acks.len());
        let mut uploaded_bytes = 0;
        for (index, ack) in pending.acks {
            uploaded_bytes += ack.stats.uploaded_bytes;
            stats.push(ack.stats);
            stages.insert(self.stages[index].uid.clone(), ack.state);
        }
        let manifest = CheckpointManifest {
            job: self.job.clone(),
            checkpoint_id: id,
            triggered_at: pending.barrier.timestamp,
            completed_at: now_millis(),
            stages,
        };
        let state_size = manifest.size();

        let storage = self.storage.clone();
        let retained = self.config.retained;
        let persisted = tokio::task::spawn_blocking(move || {
            write_manifest(&storage, &manifest)?;
            garbage_collect(&storage, retained)
        })
        .await;

        let duration = pending.started.elapsed();
        match persisted {
            Ok(Ok(gc)) => {
                {
                    let mut history = self.history();
                    history.completed += 1;
                    history.latest_completed = Some(id);
                    if let Some(record) = history.record_mut(id) {
                        record.status = CheckpointStatus::Completed;
                        record.duration_ms = Some(duration.as_millis() as u64);
                        record.state_size = state_size;
                        record.uploaded_bytes = uploaded_bytes;
                        record.stages = stats;
                    }
                }
                tracing::info!(
                    job = %self.job,
                    checkpoint = id,
                    duration_ms = duration.as_millis() as u64,
                    state_size,
                    uploaded_bytes,
                    gc_checkpoints = gc.checkpoints_removed,
                    gc_files = gc.files_removed,
                    "checkpoint completed"
                );
                let _ = self.control.send(ControlMessage::Complete(id));
            }
            Ok(Err(error)) => {
                let reason = format!("writing manifest: {error}");
                self.history().fail(id, reason, duration);
            }
            Err(error) => self.history().fail(id, error.to_string(), duration),
        }
    }

    async fn fail(&mut self, pending: Pending, reason: String) {
        let id = pending.barrier.checkpoint_id;
        tracing::warn!(job = %self.job, checkpoint = id, %reason, "checkpoint failed");
        self.history().fail(id, reason, pending.started.elapsed());
        let dir = self.storage.checkpoint_dir(id);
        let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(dir)).await;
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{completed_checkpoints, read_manifest};

    struct Harness {
        _dir: tempfile::TempDir,
        storage: CheckpointStorage,
        control: broadcast::Receiver<ControlMessage>,
        reports: mpsc::UnboundedSender<StageReport>,
        history: Arc<Mutex<CheckpointHistory>>,
    }

    fn start(stages: usize, timeout: Duration) -> Harness {
        let dir = tempfile::TempDir::new().unwrap();
        let storage = CheckpointStorage::new(dir.path()).unwrap();
        let (control_tx, control) = broadcast::channel(16);
        let (reports, reports_rx) = mpsc::unbounded_channel();
        let history = Arc::new(Mutex::new(CheckpointHistory::default()));
        let config = CheckpointConfig::new(dir.path())
            .with_interval(Duration::from_millis(10))
            .with_timeout(timeout);
        let infos = (0..stages)
            .map(|i| StageInfo {
                uid: format!("{i:02}-stage"),
                is_source: i == 0,
                inputs: Vec::new(),
            })
            .collect();
        let coordinator = CheckpointCoordinator::new(
            "test".into(),
            config,
            storage.clone(),
            infos,
            control_tx,
            reports_rx,
            Arc::clone(&history),
            Arc::new(AtomicU64::new(1)),
        );
        tokio::spawn(coordinator.run());
        Harness {
            _dir: dir,
            storage,
            control,
            reports,
            history,
        }
    }

    fn ack(id: CheckpointId, stage: usize) -> StageReport {
        StageReport::Ack(StageAck {
            checkpoint_id: id,
            stage,
            state: StageState::default(),
            stats: StageCheckpointStats::default(),
        })
    }

    #[tokio::test]
    async fn completes_when_every_stage_acks() {
        let mut h = start(2, Duration::from_secs(5));
        let ControlMessage::Trigger(barrier) = h.control.recv().await.unwrap() else {
            panic!("expected trigger")
        };
        assert_eq!(barrier.checkpoint_id, 1);

        h.reports.send(ack(1, 0)).unwrap();
        h.reports.send(ack(1, 1)).unwrap();
        assert_eq!(h.control.recv().await.unwrap(), ControlMessage::Complete(1));
        assert_eq!(completed_checkpoints(&h.storage).unwrap(), vec![1]);
        let manifest = read_manifest(&h.storage, 1).unwrap();
        assert_eq!(manifest.stages.keys().collect::<Vec<_>>(), ["00-stage", "01-stage"]);

        let history = h.history.lock().unwrap().clone();
        assert_eq!(history.latest_completed, Some(1));
        assert_eq!(history.records[0].status, CheckpointStatus::Completed);

        // The next one follows.
        assert!(matches!(h.control.recv().await.unwrap(), ControlMessage::Trigger(b) if b.checkpoint_id == 2));
    }

    #[tokio::test]
    async fn times_out_and_moves_on() {
        let mut h = start(2, Duration::from_millis(30));
        assert!(matches!(h.control.recv().await.unwrap(), ControlMessage::Trigger(_)));
        h.reports.send(ack(1, 0)).unwrap(); // stage 1 never answers

        let ControlMessage::Trigger(next) = h.control.recv().await.unwrap() else {
            panic!("expected the next trigger")
        };
        assert_eq!(next.checkpoint_id, 2);
        let history = h.history.lock().unwrap().clone();
        match &history.records[0].status {
            CheckpointStatus::Failed { reason } => assert!(reason.contains("01-stage"), "{reason}"),
            other => panic!("{other:?}"),
        }
        h.reports.send(ack(1, 1)).unwrap(); // a late ack is ignored
        assert!(completed_checkpoints(&h.storage).unwrap().is_empty());
    }

    #[tokio::test]
    async fn decline_aborts_the_checkpoint() {
        let mut h = start(2, Duration::from_secs(5));
        h.control.recv().await.unwrap();
        h.reports
            .send(StageReport::Decline {
                checkpoint_id: 1,
                stage: 1,
                reason: "disk full".into(),
            })
            .unwrap();
        assert!(matches!(h.control.recv().await.unwrap(), ControlMessage::Trigger(b) if b.checkpoint_id == 2));
        let history = h.history.lock().unwrap().clone();
        assert_eq!(history.failed, 1);
    }
}
