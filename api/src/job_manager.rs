use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use sluice_core::{CheckpointId, Timestamp, now_millis};
use sluice_ingest::EventRouter;
use sluice_runtime::{
    CheckpointHistory, CollectSink, JobHandle, JobStatus, OperatorMetricsSnapshot, StageInfo, run_job,
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::job_def::JobDefinition;

pub type JobId = u64;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JobError {
    #[error("invalid job definition: {0}")]
    Invalid(String),
    #[error("job {0} not found")]
    NotFound(JobId),
    #[error("job {job} has no stage {stage}")]
    StageNotFound { job: JobId, stage: usize },
    #[error("{0}")]
    Internal(String),
    #[error("{0}")]
    CheckpointUnavailable(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckpointsView {
    #[serde(flatten)]
    pub history: CheckpointHistory,
    /// Completed checkpoints still on disk, ascending.
    pub retained: Vec<CheckpointId>,
}

/// Owns every submitted job: its definition, runtime handle and output.
pub struct JobManager {
    router: EventRouter,
    generators: crate::generators::GeneratorManager,
    data_dir: PathBuf,
    jobs: RwLock<BTreeMap<JobId, JobEntry>>,
    next_id: AtomicU64,
}

struct JobEntry {
    definition: JobDefinition,
    handle: JobHandle,
    output: CollectSink<Value>,
    submitted_at: Timestamp,
    checkpoint_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobSummary {
    pub id: JobId,
    pub name: String,
    pub status: JobStatus,
    pub submitted_at: Timestamp,
    pub restarts: u32,
    pub checkpoints_completed: u64,
    pub latest_checkpoint: Option<CheckpointId>,
    /// `sql` or `pipeline`.
    pub kind: &'static str,
    /// The stream the job reads.
    pub stream: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobDetails {
    #[serde(flatten)]
    pub summary: JobSummary,
    pub definition: JobDefinition,
    pub checkpoint_dir: Option<PathBuf>,
    pub stages: Vec<StageInfo>,
    pub operators: Vec<OperatorMetricsSnapshot>,
    /// Failures that caused restarts (and the final one, if any).
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputRecord {
    pub key: Option<String>,
    pub event_time: Timestamp,
    pub value: Value,
}

/// Metrics for one job, for the Prometheus exporter.
pub struct JobMetrics {
    pub id: JobId,
    pub name: String,
    pub restarts: u32,
    pub checkpoints: CheckpointHistory,
    pub operators: Vec<OperatorMetricsSnapshot>,
}

impl JobEntry {
    fn summary(&self, id: JobId) -> JobSummary {
        let checkpoints = self.handle.checkpoints();
        JobSummary {
            id,
            name: self.definition.name.clone(),
            status: self.handle.status(),
            submitted_at: self.submitted_at,
            restarts: self.handle.restarts(),
            checkpoints_completed: checkpoints.completed,
            latest_checkpoint: checkpoints.latest_completed,
            kind: if self.definition.query.is_some() { "sql" } else { "pipeline" },
            stream: self.definition.stream().to_owned(),
        }
    }
}

impl JobManager {
    /// Checkpoints are written under `data_dir/checkpoints`.
    pub fn new(router: EventRouter, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            generators: crate::generators::GeneratorManager::new(router.clone()),
            router,
            data_dir: data_dir.into(),
            jobs: RwLock::default(),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn generators(&self) -> &crate::generators::GeneratorManager {
        &self.generators
    }

    pub fn router(&self) -> &EventRouter {
        &self.router
    }

    /// Validates and starts a job. Must run inside a Tokio runtime.
    pub fn submit(&self, definition: JobDefinition) -> Result<JobSummary, JobError> {
        definition.validate().map_err(JobError::Invalid)?;

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let submitted_at = now_millis();
        let checkpoint_dir = definition
            .checkpointing
            .is_some()
            .then(|| self.data_dir.join("checkpoints").join(format!("job-{id}-{submitted_at}")));

        // The subscription and output outlive individual attempts: a
        // restarted pipeline rewinds the same subscription and commits to
        // the same output.
        let subscription = Arc::new(self.router.subscribe(definition.stream()));
        let output = definition.output_buffer();
        let config = definition.job_config(checkpoint_dir.as_deref().unwrap_or(&self.data_dir));
        let handle = {
            let definition = definition.clone();
            let output = output.clone();
            run_job(config, move |env| definition.build(env, &subscription, &output))
                .map_err(|e| JobError::Internal(e.to_string()))?
        };

        tracing::info!(job_id = id, name = %definition.name, checkpointing = checkpoint_dir.is_some(), "job started");
        let entry = JobEntry {
            definition,
            handle,
            output,
            submitted_at,
            checkpoint_dir,
        };
        let summary = entry.summary(id);
        self.jobs.write().unwrap_or_else(PoisonError::into_inner).insert(id, entry);
        Ok(summary)
    }

    fn with_job<R>(&self, id: JobId, f: impl FnOnce(&JobEntry) -> R) -> Result<R, JobError> {
        let jobs = self.jobs.read().unwrap_or_else(PoisonError::into_inner);
        jobs.get(&id).map(f).ok_or(JobError::NotFound(id))
    }

    pub fn list(&self) -> Vec<JobSummary> {
        let jobs = self.jobs.read().unwrap_or_else(PoisonError::into_inner);
        jobs.iter().map(|(id, job)| job.summary(*id)).collect()
    }

    pub fn details(&self, id: JobId) -> Result<JobDetails, JobError> {
        self.with_job(id, |job| JobDetails {
            summary: job.summary(id),
            definition: job.definition.clone(),
            checkpoint_dir: job.checkpoint_dir.clone(),
            stages: job.handle.stages(),
            operators: job.handle.metrics(),
            failures: job.handle.failures(),
        })
    }

    pub fn metrics(&self, id: JobId) -> Result<Vec<OperatorMetricsSnapshot>, JobError> {
        self.with_job(id, |job| job.handle.metrics())
    }

    /// Checkpoint history, plus which completed checkpoints are still on
    /// disk (and can be inspected).
    pub fn checkpoints(&self, id: JobId) -> Result<CheckpointsView, JobError> {
        self.with_job(id, |job| CheckpointsView {
            history: job.handle.checkpoints(),
            retained: job.checkpoint_dir.as_deref().map(crate::inspect::retained).unwrap_or_default(),
        })
    }

    /// The state of every stage as of a retained checkpoint.
    pub async fn inspect(&self, id: JobId, checkpoint: CheckpointId, limit: usize) -> Result<Value, JobError> {
        let (definition, stages, names, dir) = self.with_job(id, |job| {
            (
                job.definition.clone(),
                job.handle.stages(),
                job.handle.metrics().into_iter().map(|m| m.name).collect::<Vec<_>>(),
                job.checkpoint_dir.clone(),
            )
        })?;
        let dir = dir.ok_or_else(|| JobError::Invalid(format!("job {id} does not checkpoint")))?;
        let scratch = self.data_dir.join("inspect");
        tokio::task::spawn_blocking(move || {
            crate::inspect::inspect_checkpoint(&definition, &stages, &names, &dir, &scratch, checkpoint, limit)
        })
        .await
        .map_err(|e| JobError::Internal(e.to_string()))?
        .map_err(JobError::CheckpointUnavailable)
    }

    /// Everything the live view needs in one message: status, topology,
    /// operator metrics and recent checkpoints.
    pub fn live(&self, id: JobId) -> Result<Value, JobError> {
        self.with_job(id, |job| {
            let checkpoints = job.handle.checkpoints();
            let recent: Vec<_> = checkpoints.records.iter().rev().take(30).rev().collect();
            json!({
                "ts": now_millis(),
                "job": job.summary(id),
                "stages": job.handle.stages(),
                "operators": job.handle.metrics(),
                "checkpoints": {
                    "completed": checkpoints.completed,
                    "failed": checkpoints.failed,
                    "latest_completed": checkpoints.latest_completed,
                    "restored_from": checkpoints.restored_from,
                    "recent": recent,
                },
            })
        })
    }

    /// The `limit` most recent results, oldest first.
    pub fn output(&self, id: JobId, limit: usize) -> Result<Vec<OutputRecord>, JobError> {
        self.with_job(id, |job| {
            job.output
                .latest(limit)
                .into_iter()
                .map(|event| OutputRecord {
                    key: event.key.as_deref().map(|k| String::from_utf8_lossy(k).into_owned()),
                    event_time: event.event_time,
                    value: event.value,
                })
                .collect()
        })
    }

    /// Requests cancellation. The status turns `cancelled` once every stage
    /// has stopped.
    pub fn cancel(&self, id: JobId) -> Result<JobSummary, JobError> {
        self.with_job(id, |job| {
            job.handle.cancel();
            tracing::info!(job_id = id, "job cancellation requested");
            job.summary(id)
        })
    }

    /// Kills one stage to exercise recovery (chaos testing).
    pub fn kill_stage(&self, id: JobId, stage: usize) -> Result<JobSummary, JobError> {
        self.with_job(id, |job| {
            if job.handle.kill_stage(stage) {
                tracing::warn!(job_id = id, stage, "stage kill requested");
                Ok(job.summary(id))
            } else {
                Err(JobError::StageNotFound { job: id, stage })
            }
        })?
    }

    pub async fn wait(&self, id: JobId) -> Result<JobStatus, JobError> {
        let handle = self.with_job(id, |job| job.handle.clone())?;
        Ok(handle.wait().await)
    }

    pub fn all_metrics(&self) -> Vec<JobMetrics> {
        let jobs = self.jobs.read().unwrap_or_else(PoisonError::into_inner);
        jobs.iter()
            .map(|(id, job)| JobMetrics {
                id: *id,
                name: job.definition.name.clone(),
                restarts: job.handle.restarts(),
                checkpoints: job.handle.checkpoints(),
                operators: job.handle.metrics(),
            })
            .collect()
    }
}
