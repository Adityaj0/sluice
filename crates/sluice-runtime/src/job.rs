//! Running jobs: the supervisor that spawns stages, reacts to failures, and
//! restarts the job from its latest checkpoint.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use sluice_checkpoint::recovery::describe;
use sluice_checkpoint::{
    CheckpointConfig, CheckpointCoordinator, CheckpointHistory, CheckpointManifest, RestartStrategy, StageInfo,
    cleanup_incomplete, latest_manifest, next_checkpoint_id,
};
use sluice_state::CheckpointStorage;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::{AbortHandle, JoinError, JoinSet};

use crate::dag::{DEFAULT_CHANNEL_CAPACITY, JobStatus, RuntimeError, StageSlot, StreamEnv};
use crate::metrics::{OperatorMetrics, OperatorMetricsSnapshot};
use crate::stage::{AttemptContext, CheckpointRuntime};

/// Settings for a job started with [`run_job`].
#[derive(Debug, Clone)]
pub struct JobConfig {
    pub name: String,
    pub channel_capacity: usize,
    /// `None` runs without checkpoints; restarts then start from scratch.
    pub checkpointing: Option<CheckpointConfig>,
    pub restart: RestartStrategy,
}

impl JobConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            checkpointing: None,
            restart: RestartStrategy::none(),
        }
    }

    pub fn with_checkpointing(mut self, config: CheckpointConfig) -> Self {
        self.checkpointing = Some(config);
        self
    }

    pub fn with_restart(mut self, restart: RestartStrategy) -> Self {
        self.restart = restart;
        self
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }
}

enum Command {
    Cancel,
    Kill(usize),
}

#[derive(Default)]
struct JobShared {
    stages: RwLock<Vec<StageInfo>>,
    metrics: RwLock<Vec<Arc<OperatorMetrics>>>,
    history: Arc<Mutex<CheckpointHistory>>,
    restarts: AtomicU32,
    failures: Mutex<Vec<String>>,
}

/// A handle to a running job. Cloning is cheap; dropping it does not stop
/// the job.
#[derive(Clone)]
pub struct JobHandle {
    status: watch::Receiver<JobStatus>,
    commands: mpsc::UnboundedSender<Command>,
    shared: Arc<JobShared>,
}

impl JobHandle {
    fn new() -> (Self, watch::Sender<JobStatus>, mpsc::UnboundedReceiver<Command>) {
        let (status_tx, status) = watch::channel(JobStatus::Running);
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let handle = Self {
            status,
            commands,
            shared: Arc::default(),
        };
        (handle, status_tx, commands_rx)
    }

    pub fn status(&self) -> JobStatus {
        self.status.borrow().clone()
    }

    /// Waits until the job finishes, fails or is cancelled.
    pub async fn wait(&self) -> JobStatus {
        let mut status = self.status.clone();
        let result = status.wait_for(JobStatus::is_terminal).await.map(|s| s.clone());
        result.unwrap_or_else(|_| self.status())
    }

    /// Stops every stage. Output already emitted stays where it is.
    pub fn cancel(&self) {
        let _ = self.commands.send(Command::Cancel);
    }

    /// Kills one stage, simulating a crash: the job fails over and, if
    /// restarts remain, recovers from its latest checkpoint. Returns `false`
    /// for an unknown stage.
    pub fn kill_stage(&self, stage: usize) -> bool {
        stage < self.stages().len() && self.commands.send(Command::Kill(stage)).is_ok()
    }

    /// Per-stage metrics of the current attempt, in pipeline order.
    pub fn metrics(&self) -> Vec<OperatorMetricsSnapshot> {
        let metrics = self.shared.metrics.read().unwrap_or_else(PoisonError::into_inner);
        metrics.iter().map(|m| m.snapshot()).collect()
    }

    pub fn stages(&self) -> Vec<StageInfo> {
        self.shared.stages.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub fn checkpoints(&self) -> CheckpointHistory {
        self.shared.history.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub fn restarts(&self) -> u32 {
        self.shared.restarts.load(Ordering::Relaxed)
    }

    /// The failures that caused restarts (and the final one, if any).
    pub fn failures(&self) -> Vec<String> {
        self.shared.failures.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

impl JobShared {
    fn install(&self, stages: &[StageSlot]) {
        *self.stages.write().unwrap_or_else(PoisonError::into_inner) = stages.iter().map(|s| s.info.clone()).collect();
        *self.metrics.write().unwrap_or_else(PoisonError::into_inner) =
            stages.iter().map(|s| Arc::clone(&s.metrics)).collect();
    }

    fn history(&self) -> std::sync::MutexGuard<'_, CheckpointHistory> {
        self.history.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Runs prebuilt stages once (see [`StreamEnv::execute`]).
pub(crate) fn execute_once(stages: Vec<StageSlot>) -> JobHandle {
    let (handle, status, mut commands) = JobHandle::new();
    handle.shared.install(&stages);
    tokio::spawn(async move {
        let outcome = run_attempt(stages, &mut commands).await;
        status.send_replace(outcome.into_status());
    });
    handle
}

/// Starts a fault-tolerant job. `build` constructs the pipeline and is
/// called again for every restart, so it must build the same topology each
/// time (sources are rewound to their checkpointed positions).
pub fn run_job<F>(config: JobConfig, build: F) -> Result<JobHandle, RuntimeError>
where
    F: Fn(&StreamEnv) + Send + Sync + 'static,
{
    let storage = config
        .checkpointing
        .as_ref()
        .map(|c| CheckpointStorage::new(&c.dir).map_err(|e| RuntimeError::Storage(format!("{}: {e}", c.dir.display()))))
        .transpose()?;
    let (handle, status, commands) = JobHandle::new();
    let shared = Arc::clone(&handle.shared);
    tokio::spawn(supervise(config, storage, build, status, commands, shared));
    Ok(handle)
}

async fn supervise<F>(
    config: JobConfig,
    storage: Option<CheckpointStorage>,
    build: F,
    status: watch::Sender<JobStatus>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    shared: Arc<JobShared>,
) where
    F: Fn(&StreamEnv) + Send + Sync + 'static,
{
    let next_id = Arc::new(AtomicU64::new(1));
    let mut restore: Option<Arc<CheckpointManifest>> = None;
    if let (Some(storage), Some(cp)) = (&storage, &config.checkpointing) {
        let s = storage.clone();
        let resume = cp.resume;
        let (next, latest) = tokio::task::spawn_blocking(move || {
            let latest = if resume { latest_manifest(&s) } else { None };
            (next_checkpoint_id(&s), latest)
        })
        .await
        .unwrap_or((1, None));
        next_id.store(next, Ordering::SeqCst);
        if let Some(manifest) = latest {
            tracing::info!(job = %config.name, "resuming from {}", describe(&manifest));
            restore = Some(Arc::new(manifest));
        }
    }

    let mut attempt = 0;
    loop {
        // Fresh channels per attempt: nothing from a dead attempt can leak
        // into the next one.
        let (control, _) = broadcast::channel(64);
        let (reports, reports_rx) = mpsc::unbounded_channel();
        let (uploads, mut uploads_done) = mpsc::channel::<()>(1);
        let work_dir = storage.as_ref().map(|s| s.work_dir().join(format!("attempt-{attempt}")));
        let checkpointing = match (&storage, &config.checkpointing, &work_dir) {
            (Some(storage), Some(cp), Some(work_dir)) => Some(CheckpointRuntime {
                storage: storage.clone(),
                backend: cp.backend,
                work_dir: work_dir.clone(),
                control: control.clone(),
                reports,
                uploads,
            }),
            _ => {
                drop(uploads);
                None
            }
        };
        let ctx = Arc::new(AttemptContext {
            attempt,
            checkpointing,
            restore: restore.clone(),
        });

        let env = StreamEnv::for_attempt(ctx, config.channel_capacity);
        if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(|| build(&env))) {
            let error = RuntimeError::Build(panic_message(panic));
            shared.failures.lock().unwrap_or_else(PoisonError::into_inner).push(error.to_string());
            status.send_replace(JobStatus::Failed {
                error: error.to_string(),
            });
            return;
        }
        let stages = env.take_stages();
        drop(env);
        shared.install(&stages);
        shared.history().restored(restore.as_ref().map(|m| m.checkpoint_id));

        let coordinator = match (&storage, &config.checkpointing) {
            (Some(storage), Some(cp)) => {
                let infos = stages.iter().map(|s| s.info.clone()).collect();
                let coordinator = CheckpointCoordinator::new(
                    config.name.clone(),
                    cp.clone(),
                    storage.clone(),
                    infos,
                    control,
                    reports_rx,
                    Arc::clone(&shared.history),
                    Arc::clone(&next_id),
                );
                match spawn_coordinator(&config.name, coordinator) {
                    Ok(handle) => Some(handle),
                    Err(error) => {
                        status.send_replace(JobStatus::Failed {
                            error: error.to_string(),
                        });
                        return;
                    }
                }
            }
            _ => None,
        };

        let outcome = run_attempt(stages, &mut commands).await;

        if let Some(coordinator) = coordinator {
            coordinator.stop().await;
        }
        shared.history().abort_in_progress("job attempt ended");
        // Let in-flight snapshot uploads finish, then drop what the
        // attempt left half-done: its work dir and incomplete checkpoints.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(30), uploads_done.recv()).await;
        if let (Some(storage), Some(cp)) = (&storage, &config.checkpointing) {
            let (storage, retained) = (storage.clone(), cp.retained);
            let _ = tokio::task::spawn_blocking(move || cleanup_incomplete(&storage, retained)).await;
        }
        if let Some(dir) = work_dir {
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(dir)).await;
        }

        let error = match outcome {
            AttemptOutcome::Failed(error) => error,
            other => {
                status.send_replace(other.into_status());
                return;
            }
        };
        shared.failures.lock().unwrap_or_else(PoisonError::into_inner).push(error.to_string());
        if shared.restarts.load(Ordering::Relaxed) >= config.restart.max_restarts {
            tracing::error!(job = %config.name, %error, "job failed; no restarts left");
            status.send_replace(JobStatus::Failed {
                error: error.to_string(),
            });
            return;
        }
        shared.restarts.fetch_add(1, Ordering::Relaxed);

        // Wait out the restart delay unless cancelled meanwhile.
        let delay = tokio::time::sleep(config.restart.delay);
        tokio::pin!(delay);
        loop {
            tokio::select! {
                _ = &mut delay => break,
                command = commands.recv() => {
                    if matches!(command, Some(Command::Cancel)) {
                        status.send_replace(JobStatus::Cancelled);
                        return;
                    }
                }
            }
        }

        restore = match &storage {
            Some(storage) => {
                let s = storage.clone();
                tokio::task::spawn_blocking(move || latest_manifest(&s)).await.ok().flatten().map(Arc::new)
            }
            None => None,
        };
        attempt += 1;
        match &restore {
            Some(manifest) => tracing::warn!(job = %config.name, %error, attempt, "restarting from {}", describe(manifest)),
            None => tracing::warn!(job = %config.name, %error, attempt, "restarting from scratch (no checkpoint)"),
        }
    }
}

/// The coordinator of one attempt, running on its own thread.
struct CoordinatorThread {
    stop: tokio::sync::oneshot::Sender<()>,
    thread: std::thread::JoinHandle<()>,
}

/// Runs the coordinator on a dedicated thread with its own runtime, like
/// Flink's JobManager: operators are synchronous user code and may block
/// the worker threads they run on, which must not stall checkpoint
/// triggering and timeouts.
fn spawn_coordinator(job: &str, coordinator: CheckpointCoordinator) -> Result<CoordinatorThread, RuntimeError> {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| RuntimeError::Storage(format!("starting checkpoint coordinator: {e}")))?;
    let thread = std::thread::Builder::new()
        .name(format!("coordinator-{job}"))
        .spawn(move || {
            runtime.block_on(async move {
                tokio::select! {
                    _ = coordinator.run() => {}
                    _ = stopped => {}
                }
            });
        })
        .map_err(|e| RuntimeError::Storage(format!("starting checkpoint coordinator: {e}")))?;
    Ok(CoordinatorThread { stop, thread })
}

impl CoordinatorThread {
    async fn stop(self) {
        let _ = self.stop.send(());
        let thread = self.thread;
        let _ = tokio::task::spawn_blocking(move || thread.join()).await;
    }
}

enum AttemptOutcome {
    Finished,
    Cancelled,
    Failed(RuntimeError),
}

impl AttemptOutcome {
    fn into_status(self) -> JobStatus {
        match self {
            AttemptOutcome::Finished => JobStatus::Finished,
            AttemptOutcome::Cancelled => JobStatus::Cancelled,
            AttemptOutcome::Failed(error) => JobStatus::Failed {
                error: error.to_string(),
            },
        }
    }
}

/// Runs one attempt's stages to completion. The first failure aborts every
/// other stage; a cancel command stops them all.
async fn run_attempt(stages: Vec<StageSlot>, commands: &mut mpsc::UnboundedReceiver<Command>) -> AttemptOutcome {
    let mut tasks = JoinSet::new();
    let mut by_task = HashMap::new();
    let mut aborts: Vec<AbortHandle> = Vec::new();
    let mut names = Vec::new();
    for (index, stage) in stages.into_iter().enumerate() {
        let future = stage.future.unwrap_or_else(|| {
            let name = stage.name.clone();
            Box::pin(async move { Err(RuntimeError::Stage { stage: name, message: "stage was never wired up".into() }) })
        });
        let handle = tasks.spawn(future);
        by_task.insert(handle.id(), index);
        aborts.push(handle);
        names.push(stage.name);
    }

    let mut killed = HashSet::new();
    let mut failure: Option<RuntimeError> = None;
    let mut commands_open = true;
    let mut record = |error: RuntimeError, tasks: &mut JoinSet<_>| {
        // A crash upstream shows up downstream as InputClosed; report the
        // root cause.
        let replace = match &failure {
            None => true,
            Some(RuntimeError::InputClosed(_)) => !matches!(error, RuntimeError::InputClosed(_)),
            Some(_) => false,
        };
        if replace {
            tracing::error!(%error, "pipeline stage failed");
            failure = Some(error);
        }
        tasks.abort_all();
    };

    loop {
        tokio::select! {
            joined = tasks.join_next_with_id() => match joined {
                None => break,
                Some(Ok((_, Ok(())))) => {}
                Some(Ok((_, Err(error)))) => record(error, &mut tasks),
                Some(Err(join_error)) => {
                    let index = by_task.get(&join_error.id()).copied().unwrap_or_default();
                    let stage = names.get(index).cloned().unwrap_or_default();
                    if join_error.is_panic() {
                        let message = panic_message_of(join_error);
                        record(RuntimeError::Panicked { stage, message }, &mut tasks);
                    } else if killed.contains(&index) {
                        record(RuntimeError::Killed(stage), &mut tasks);
                    }
                }
            },
            command = commands.recv(), if commands_open => match command {
                Some(Command::Cancel) => {
                    tasks.shutdown().await;
                    return AttemptOutcome::Cancelled;
                }
                Some(Command::Kill(index)) => {
                    if let Some(handle) = aborts.get(index) {
                        tracing::warn!(stage = %names[index], "killing stage on request");
                        killed.insert(index);
                        handle.abort();
                    }
                }
                None => commands_open = false,
            }
        }
    }

    match failure {
        None => AttemptOutcome::Finished,
        Some(error) => AttemptOutcome::Failed(error),
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

fn panic_message_of(error: JoinError) -> String {
    match error.try_into_panic() {
        Ok(payload) => panic_message(payload),
        Err(error) => error.to_string(),
    }
}
