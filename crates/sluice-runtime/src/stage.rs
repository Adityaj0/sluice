//! The task bodies that drive sources, operators, splits and sinks,
//! including their part in checkpointing.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::future::poll_fn;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};

use sluice_checkpoint::{
    CheckpointManifest, ControlMessage, StageAck, StageCheckpointStats, StageReport, StageState,
};
use sluice_core::{
    CheckpointBarrier, CheckpointId, Context, Either, Event, KeyedState, NoState, Operator, StateError, StreamElement,
    StreamStatus, Timestamp, WatermarkTracker, decode, encode,
};
use sluice_state::{CheckpointStorage, PendingSnapshot, StateBackend, StateBackendKind, create_backend};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tokio::time::MissedTickBehavior;

use crate::channel::{Receiver, Sender};
use crate::dag::RuntimeError;
use crate::metrics::OperatorMetrics;
use crate::sink::Sink;
use crate::source::{AlignmentMember, Emitter, SourceFunction, SourceItem, WatermarkStrategy};

/// Everything the stages of one job attempt share.
pub(crate) struct AttemptContext {
    pub attempt: u32,
    pub checkpointing: Option<CheckpointRuntime>,
    pub restore: Option<Arc<CheckpointManifest>>,
}

pub(crate) struct CheckpointRuntime {
    pub storage: CheckpointStorage,
    pub backend: StateBackendKind,
    /// Live state of this attempt (RocksDB directories).
    pub work_dir: PathBuf,
    pub control: broadcast::Sender<ControlMessage>,
    pub reports: mpsc::UnboundedSender<StageReport>,
    /// Held by every in-flight snapshot upload; the supervisor waits for
    /// all clones to drop before cleaning up after the attempt.
    pub uploads: mpsc::Sender<()>,
}

impl AttemptContext {
    /// A single run without checkpointing.
    pub fn standalone() -> Self {
        Self {
            attempt: 0,
            checkpointing: None,
            restore: None,
        }
    }
}

/// Per-stage handle on the attempt: identity, metrics, restore data and the
/// checkpoint protocol.
pub(crate) struct StageRuntime {
    pub index: usize,
    pub uid: String,
    pub name: String,
    pub metrics: Arc<OperatorMetrics>,
    pub ctx: Arc<AttemptContext>,
    /// Coordinator notifications, subscribed when the stage is built so no
    /// trigger can be missed. Only sources and sinks listen.
    pub control: Option<broadcast::Receiver<ControlMessage>>,
}

impl StageRuntime {
    fn error(&self, message: impl Display) -> RuntimeError {
        RuntimeError::Stage {
            stage: self.name.clone(),
            message: message.to_string(),
        }
    }

    fn restored(&self) -> Option<&StageState> {
        self.ctx.restore.as_ref()?.stages.get(&self.uid)
    }

    fn restored_checkpoint(&self) -> Option<CheckpointId> {
        self.ctx.restore.as_ref().map(|m| m.checkpoint_id)
    }

    fn checkpointing(&self) -> bool {
        self.ctx.checkpointing.is_some()
    }

    async fn restored_operator_state(&self) -> Result<Option<Vec<u8>>, RuntimeError> {
        let (Some(cp), Some(file)) = (&self.ctx.checkpointing, self.restored().and_then(|s| s.operator_state.clone()))
        else {
            return Ok(None);
        };
        let storage = cp.storage.clone();
        let bytes = tokio::task::spawn_blocking(move || storage.read(&file))
            .await
            .map_err(|e| self.error(e))?
            .map_err(|e| self.error(e))?;
        Ok(Some(bytes))
    }

    async fn open_backend(&self, states: &'static [&'static str]) -> Result<Box<dyn StateBackend>, RuntimeError> {
        let uid = self.uid.clone();
        let restore = self.restored().and_then(|s| s.keyed_state.clone());
        let (kind, work_dir, storage) = match &self.ctx.checkpointing {
            Some(cp) => (cp.backend, cp.work_dir.clone(), Some(cp.storage.clone())),
            None => (StateBackendKind::Memory, PathBuf::new(), None),
        };
        tokio::task::spawn_blocking(move || {
            create_backend(kind, &uid, states, &work_dir, storage.as_ref(), restore.as_ref())
        })
        .await
        .map_err(|e| self.error(e))?
        .map_err(|e| self.error(e))
    }

    /// Finishes this stage's part of a checkpoint off the async runtime:
    /// uploads operator and keyed state, then reports to the coordinator.
    fn acknowledge(
        &self,
        barrier: CheckpointBarrier,
        operator_state: Option<Vec<u8>>,
        keyed_state: Option<Box<dyn PendingSnapshot>>,
        alignment: Duration,
        sync: Duration,
    ) {
        let Some(cp) = &self.ctx.checkpointing else { return };
        let storage = cp.storage.clone();
        let reports = cp.reports.clone();
        let in_flight = cp.uploads.clone();
        let uid = self.uid.clone();
        let stage = self.index;
        let checkpoint_id = barrier.checkpoint_id;

        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let upload = || -> Result<(StageState, u64), StateError> {
                let mut state = StageState::default();
                let mut uploaded = 0;
                if let Some(bytes) = operator_state.filter(|b| !b.is_empty()) {
                    let path = CheckpointStorage::private_path(checkpoint_id, &uid, "operator-state.bin");
                    let file = storage.write(&path, &bytes)?;
                    uploaded += file.size;
                    state.operator_state = Some(file);
                }
                if let Some(pending) = keyed_state {
                    let result = pending.upload(&storage)?;
                    uploaded += result.uploaded_bytes;
                    state.keyed_state = Some(result.handle);
                }
                Ok((state, uploaded))
            };
            let report = match upload() {
                Ok((state, uploaded_bytes)) => StageReport::Ack(StageAck {
                    checkpoint_id,
                    stage,
                    stats: StageCheckpointStats {
                        stage: uid,
                        alignment_ms: alignment.as_millis() as u64,
                        sync_ms: sync.as_millis() as u64,
                        async_ms: started.elapsed().as_millis() as u64,
                        state_size: state.size(),
                        uploaded_bytes,
                    },
                    state,
                }),
                Err(error) => StageReport::Decline {
                    checkpoint_id,
                    stage,
                    reason: error.to_string(),
                },
            };
            let _ = reports.send(report);
            drop(in_flight);
        });
    }

    fn decline(&self, checkpoint_id: CheckpointId, reason: impl Display) {
        if let Some(cp) = &self.ctx.checkpointing {
            let _ = cp.reports.send(StageReport::Decline {
                checkpoint_id,
                stage: self.index,
                reason: reason.to_string(),
            });
        }
    }

    fn source_finished(&self) {
        if let Some(cp) = &self.ctx.checkpointing {
            let _ = cp.reports.send(StageReport::SourceFinished { stage: self.index });
        }
    }
}

/// Waits for the next coordinator message; never resolves without a
/// channel. Lagging (missed messages) is harmless: triggers are retried by
/// timeout and completions are cumulative.
async fn next_control(control: &mut Option<broadcast::Receiver<ControlMessage>>) -> Option<ControlMessage> {
    let Some(rx) = control else {
        return std::future::pending().await;
    };
    loop {
        match rx.recv().await {
            Ok(message) => return Some(message),
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}

/// Converts the payload type of a stream element.
fn map_element<U, T>(element: StreamElement<U>, f: impl FnOnce(U) -> T) -> StreamElement<T> {
    match element {
        StreamElement::Record(event) => StreamElement::Record(event.map_value(f)),
        StreamElement::Watermark(wm) => StreamElement::Watermark(wm),
        StreamElement::Barrier(barrier) => StreamElement::Barrier(barrier),
        StreamElement::Status(status) => StreamElement::Status(status),
        StreamElement::End => StreamElement::End,
    }
}

/// One input of a (possibly multi-input) operator.
pub(crate) trait InputPort<T>: Send {
    fn poll_next(&mut self, cx: &mut TaskContext<'_>) -> Poll<Option<StreamElement<T>>>;
}

pub(crate) type BoxedInput<T> = Box<dyn InputPort<T>>;

impl<T: Send> InputPort<T> for Receiver<StreamElement<T>> {
    fn poll_next(&mut self, cx: &mut TaskContext<'_>) -> Poll<Option<StreamElement<T>>> {
        self.poll_recv(cx)
    }
}

/// An input whose records are converted on the way in, so streams of
/// different types can feed one operator (`Either::Left` / `Either::Right`
/// for a join).
pub(crate) struct MapInput<U, T, F> {
    rx: Receiver<StreamElement<U>>,
    f: F,
    _output: PhantomData<fn() -> T>,
}

impl<U, T, F: FnMut(U) -> T> MapInput<U, T, F> {
    pub(crate) fn new(rx: Receiver<StreamElement<U>>, f: F) -> Self {
        Self {
            rx,
            f,
            _output: PhantomData,
        }
    }
}

impl<U: Send, T, F: FnMut(U) -> T + Send> InputPort<T> for MapInput<U, T, F> {
    fn poll_next(&mut self, cx: &mut TaskContext<'_>) -> Poll<Option<StreamElement<T>>> {
        self.rx
            .poll_recv(cx)
            .map(|element| element.map(|e| map_element(e, &mut self.f)))
    }
}

/// What a source checkpoints: where to resume reading and its watermark
/// generator's state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCheckpoint {
    pub position: u64,
    pub max_seen: Option<Timestamp>,
    pub last_watermark: Option<Timestamp>,
}

/// Decodes a source's operator state from a checkpoint.
pub fn decode_source_checkpoint(bytes: &[u8]) -> Option<SourceCheckpoint> {
    decode(bytes).ok()
}

/// Without checkpointing, sources release read data this often.
const COMMIT_EVERY: u64 = 1_024;

pub(crate) async fn run_source<T, S>(
    mut rt: StageRuntime,
    mut source: S,
    strategy: WatermarkStrategy,
    mut alignment: Option<AlignmentMember>,
    output: Sender<StreamElement<T>>,
) -> Result<(), RuntimeError>
where
    T: Send + 'static,
    S: SourceFunction<T>,
{
    let mut emitter = Emitter::new(output, &strategy, Arc::clone(&rt.metrics));
    if let Some(bytes) = rt.restored_operator_state().await? {
        let snapshot: SourceCheckpoint = decode(&bytes).map_err(|e| rt.error(e))?;
        source.seek(snapshot.position);
        emitter.restore(snapshot.max_seen, snapshot.last_watermark);
    }

    let checkpointing = rt.checkpointing();
    let mut control = rt.control.take();
    // Read position at each in-flight checkpoint, released on completion.
    let mut positions: BTreeMap<CheckpointId, u64> = BTreeMap::new();
    let mut ticker = tokio::time::interval(strategy.interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_record = Instant::now();

    loop {
        // Watermark alignment: register for wake-ups before checking, so
        // an update between the check and the wait is not missed.
        let group = alignment.as_ref().map(|a| Arc::clone(&a.group));
        let changed = group.as_ref().map(|g| g.changed.notified());
        tokio::pin!(changed);
        if let Some(changed) = changed.as_mut().as_pin_mut() {
            changed.enable();
        }
        let paused = alignment.as_ref().is_some_and(AlignmentMember::should_pause);
        if paused {
            last_record = Instant::now(); // held back, not idle
        }

        tokio::select! {
            biased;
            message = next_control(&mut control) => match message {
                Some(ControlMessage::Trigger(barrier)) => {
                    let started = Instant::now();
                    let position = source.position();
                    let (max_seen, last_watermark) = emitter.snapshot();
                    let snapshot = SourceCheckpoint { position, max_seen, last_watermark };
                    match encode(&snapshot) {
                        Ok(bytes) => {
                            positions.insert(barrier.checkpoint_id, position);
                            rt.acknowledge(barrier, Some(bytes), None, Duration::ZERO, started.elapsed());
                        }
                        Err(error) => rt.decline(barrier.checkpoint_id, error),
                    }
                    if !emitter.barrier(barrier).await {
                        return Ok(());
                    }
                }
                Some(ControlMessage::Complete(id)) => {
                    let later = positions.split_off(&(id + 1));
                    if let Some((_, position)) = std::mem::replace(&mut positions, later).pop_last() {
                        source.commit(position);
                    }
                }
                None => control = None,
            },
            _ = async { changed.as_mut().as_pin_mut().expect("paused implies alignment").await }, if paused => {}
            item = source.next(), if !paused => match item {
                Some(SourceItem::Record(event)) => {
                    last_record = Instant::now();
                    if !emitter.record(event).await {
                        return Ok(());
                    }
                    if !checkpointing && source.position() % COMMIT_EVERY == 0 {
                        source.commit(source.position());
                    }
                }
                Some(SourceItem::Watermark(wm)) => {
                    if !emitter.explicit_watermark(wm).await {
                        return Ok(());
                    }
                }
                None => break,
            },
            _ = ticker.tick() => {
                if !emitter.watermark().await {
                    return Ok(());
                }
                if let Some(timeout) = strategy.idle_timeout {
                    if !emitter.is_idle() && last_record.elapsed() >= timeout && !emitter.set_idle(true).await {
                        return Ok(());
                    }
                }
                if !checkpointing {
                    source.commit(source.position());
                }
            }
        }

        if let Some(member) = alignment.as_mut() {
            member.report(if emitter.is_idle() { None } else { emitter.last_emitted() });
        }
    }

    drop(alignment); // leave the group: finished sources hold nobody back
    rt.source_finished();
    emitter.finish().await;
    if !checkpointing {
        source.commit(source.position());
    }
    Ok(())
}

/// Polls the inputs that are neither finished nor paused for barrier
/// alignment, round-robin from `cursor` for fairness. Returns `None` once
/// every input has finished.
async fn next_input<T>(
    inputs: &mut [Option<BoxedInput<T>>],
    blocked: &[bool],
    cursor: &mut usize,
) -> Option<(usize, Option<StreamElement<T>>)> {
    let n = inputs.len();
    poll_fn(|cx| {
        let mut live = false;
        for k in 0..n {
            let i = (*cursor + k) % n;
            let Some(input) = inputs[i].as_mut() else { continue };
            live = true;
            if blocked[i] {
                continue;
            }
            if let Poll::Ready(element) = input.poll_next(cx) {
                *cursor = (i + 1) % n;
                return Poll::Ready(Some((i, element)));
            }
        }
        if live { Poll::Pending } else { Poll::Ready(None) }
    })
    .await
}

struct Alignment {
    barrier: CheckpointBarrier,
    started: Instant,
}

pub(crate) async fn run_operator<O: Operator>(
    rt: StageRuntime,
    mut operator: O,
    inputs: Vec<BoxedInput<O::In>>,
    output: Sender<StreamElement<O::Out>>,
) -> Result<(), RuntimeError> {
    if let Some(bytes) = rt.restored_operator_state().await? {
        operator.restore_state(&bytes).map_err(|e| rt.error(e))?;
    }
    let states = operator.state_names();
    let mut backend = if states.is_empty() {
        None
    } else {
        Some(rt.open_backend(states).await?)
    };
    let mut no_state = NoState;

    let n = inputs.len();
    let mut inputs: Vec<_> = inputs.into_iter().map(Some).collect();
    let mut blocked = vec![false; n];
    let mut tracker = WatermarkTracker::new(n);
    let mut output_idle = false;
    let mut alignment: Option<Alignment> = None;
    let mut buffer = Vec::new();
    let mut cursor = 0;

    macro_rules! state {
        () => {
            match backend.as_mut() {
                Some(backend) => backend.as_mut() as &mut dyn KeyedState,
                None => &mut no_state as &mut dyn KeyedState,
            }
        };
    }

    macro_rules! advance {
        ($wm:expr) => {{
            let wm = $wm;
            operator
                .on_watermark(wm, &mut Context::new(&mut buffer, state!()))
                .map_err(|e| rt.error(e))?;
            rt.metrics.set_late(operator.late_records());
            forward(&output, &mut buffer, &rt.metrics).await && emit_watermark(&output, wm, &rt.metrics).await
        }};
    }

    while let Some((input, element)) = next_input(&mut inputs, &blocked, &mut cursor).await {
        let alive = match element {
            None => return Err(RuntimeError::InputClosed(rt.name.clone())),
            Some(StreamElement::Record(event)) => {
                rt.metrics.add_in(1);
                operator
                    .process(event, &mut Context::new(&mut buffer, state!()))
                    .map_err(|e| rt.error(e))?;
                rt.metrics.set_late(operator.late_records());
                forward(&output, &mut buffer, &rt.metrics).await
            }
            Some(StreamElement::Watermark(wm)) => match tracker.update(input, wm) {
                Some(wm) => advance!(wm),
                None => true,
            },
            Some(StreamElement::Status(status)) => match tracker.set_idle(input, status == StreamStatus::Idle) {
                Some(wm) => advance!(wm),
                None => true,
            },
            Some(StreamElement::Barrier(barrier)) => {
                match &alignment {
                    Some(current) if current.barrier.checkpoint_id > barrier.checkpoint_id => {
                        continue; // stale barrier of an abandoned checkpoint
                    }
                    Some(current) if current.barrier.checkpoint_id < barrier.checkpoint_id => {
                        // An input skipped the checkpoint being aligned (its
                        // source missed the trigger): abandon it.
                        blocked.fill(false);
                        alignment = None;
                    }
                    _ => {}
                }
                alignment.get_or_insert_with(|| Alignment {
                    barrier,
                    started: Instant::now(),
                });
                blocked[input] = true;
                true
            }
            Some(StreamElement::End) => {
                inputs[input] = None;
                blocked[input] = false;
                match tracker.finish_input(input) {
                    Some(wm) => advance!(wm),
                    None => true,
                }
            }
        };
        if !alive {
            return Ok(()); // downstream gone: the job is being torn down
        }

        // The operator is idle once all its inputs are; tell downstream.
        let idle = tracker.all_idle();
        if idle != output_idle {
            output_idle = idle;
            rt.metrics.set_idle(idle);
            let status = if idle { StreamStatus::Idle } else { StreamStatus::Active };
            if output.send(StreamElement::Status(status)).await.is_err() {
                return Ok(());
            }
        }

        // Aligned once every live input has delivered the barrier.
        let aligned = alignment.is_some() && inputs.iter().zip(&blocked).all(|(rx, blocked)| rx.is_none() || *blocked);
        if aligned && inputs.iter().any(Option::is_some) {
            let Alignment { barrier, started } = alignment.take().expect("aligned");
            let alignment_time = started.elapsed();
            let sync_started = Instant::now();
            let snapshot = operator
                .snapshot_state()
                .map_err(|e| e.to_string())
                .and_then(|op_state| match backend.as_mut() {
                    Some(backend) => backend
                        .snapshot(barrier.checkpoint_id)
                        .map(|keyed| (op_state, Some(keyed)))
                        .map_err(|e| e.to_string()),
                    None => Ok((op_state, None)),
                });
            match snapshot {
                Ok((op_state, keyed)) => {
                    rt.acknowledge(barrier, Some(op_state), keyed, alignment_time, sync_started.elapsed())
                }
                Err(error) => rt.decline(barrier.checkpoint_id, error),
            }
            if output.send(StreamElement::Barrier(barrier)).await.is_err() {
                return Ok(());
            }
            blocked.fill(false);
        }
    }

    let _ = output.send(StreamElement::End).await;
    Ok(())
}

async fn forward<T>(output: &Sender<StreamElement<T>>, buffer: &mut Vec<Event<T>>, metrics: &OperatorMetrics) -> bool {
    for event in buffer.drain(..) {
        if output.send(StreamElement::Record(event)).await.is_err() {
            return false;
        }
        metrics.add_out(1);
    }
    true
}

async fn emit_watermark<T>(output: &Sender<StreamElement<T>>, wm: Timestamp, metrics: &OperatorMetrics) -> bool {
    metrics.set_watermark(wm);
    output.send(StreamElement::Watermark(wm)).await.is_ok()
}

/// Routes `Either::Left` records to one output and `Either::Right` to the
/// other (side outputs); control elements go to both. A side nobody
/// consumes is skipped.
pub(crate) async fn run_split<A, B>(
    rt: StageRuntime,
    mut input: Receiver<StreamElement<Either<A, B>>>,
    left: Sender<StreamElement<A>>,
    right: Sender<StreamElement<B>>,
) -> Result<(), RuntimeError>
where
    A: Send + 'static,
    B: Send + 'static,
{
    let mut left = Some(left);
    let mut right = Some(right);

    async fn send<T>(slot: &mut Option<Sender<StreamElement<T>>>, element: StreamElement<T>) {
        if let Some(tx) = slot {
            if tx.send(element).await.is_err() {
                *slot = None;
            }
        }
    }

    loop {
        let Some(element) = input.recv().await else {
            return Err(RuntimeError::InputClosed(rt.name.clone()));
        };
        match element {
            StreamElement::Record(event) => {
                rt.metrics.add_in(1);
                let Event {
                    key,
                    value,
                    event_time,
                    ingestion_time,
                } = event;
                match value {
                    Either::Left(value) => {
                        send(&mut left, StreamElement::Record(Event { key, value, event_time, ingestion_time })).await
                    }
                    Either::Right(value) => {
                        send(&mut right, StreamElement::Record(Event { key, value, event_time, ingestion_time })).await
                    }
                }
                rt.metrics.add_out(1);
            }
            StreamElement::Barrier(barrier) => {
                rt.acknowledge(barrier, None, None, Duration::ZERO, Duration::ZERO);
                send(&mut left, StreamElement::Barrier(barrier)).await;
                send(&mut right, StreamElement::Barrier(barrier)).await;
            }
            StreamElement::End => {
                send(&mut left, StreamElement::End).await;
                send(&mut right, StreamElement::End).await;
                return Ok(());
            }
            StreamElement::Watermark(wm) => {
                rt.metrics.set_watermark(wm);
                send(&mut left, StreamElement::Watermark(wm)).await;
                send(&mut right, StreamElement::Watermark(wm)).await;
            }
            StreamElement::Status(status) => {
                rt.metrics.set_idle(status == StreamStatus::Idle);
                send(&mut left, StreamElement::Status(status)).await;
                send(&mut right, StreamElement::Status(status)).await;
            }
        }
        if left.is_none() && right.is_none() {
            return Ok(());
        }
    }
}

pub(crate) async fn run_sink<T, S: Sink<T>>(
    mut rt: StageRuntime,
    mut sink: S,
    mut input: Receiver<StreamElement<T>>,
) -> Result<(), RuntimeError> {
    sink.restore(rt.restored_checkpoint());
    let mut control = rt.control.take();

    loop {
        tokio::select! {
            biased;
            message = next_control(&mut control) => match message {
                Some(ControlMessage::Complete(id)) => sink.commit(id),
                Some(ControlMessage::Trigger(_)) => {}
                None => control = None,
            },
            element = input.recv() => match element {
                Some(StreamElement::Record(event)) => {
                    rt.metrics.add_in(1);
                    rt.metrics.record_latency(sluice_core::now_millis().saturating_sub(event.ingestion_time));
                    sink.write(event);
                }
                Some(StreamElement::Watermark(wm)) => {
                    rt.metrics.set_watermark(wm);
                    sink.on_watermark(wm);
                }
                Some(StreamElement::Status(status)) => rt.metrics.set_idle(status == StreamStatus::Idle),
                Some(StreamElement::Barrier(barrier)) => {
                    let started = Instant::now();
                    sink.pre_commit(barrier.checkpoint_id);
                    rt.acknowledge(barrier, None, None, Duration::ZERO, started.elapsed());
                }
                Some(StreamElement::End) => {
                    sink.finish();
                    return Ok(());
                }
                None => return Err(RuntimeError::InputClosed(rt.name.clone())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sluice_core::MapOperator;

    use super::*;
    use crate::channel::channel;

    fn runtime() -> StageRuntime {
        StageRuntime {
            index: 0,
            uid: "00-test".into(),
            name: "test".into(),
            metrics: Arc::new(OperatorMetrics::new(0, "test")),
            ctx: Arc::new(AttemptContext::standalone()),
            control: None,
        }
    }

    fn barrier(id: CheckpointId) -> StreamElement<u32> {
        StreamElement::Barrier(CheckpointBarrier {
            checkpoint_id: id,
            timestamp: 0,
        })
    }

    fn record(v: u32) -> StreamElement<u32> {
        StreamElement::Record(Event::new(v, v as u64))
    }

    type Ports = (Sender<StreamElement<u32>>, Sender<StreamElement<u32>>, Receiver<StreamElement<u32>>);

    fn two_input_identity() -> Ports {
        let (a_tx, a_rx) = channel(16);
        let (b_tx, b_rx) = channel(16);
        let (out_tx, out_rx) = channel(64);
        let inputs: Vec<BoxedInput<u32>> = vec![Box::new(a_rx), Box::new(b_rx)];
        tokio::spawn(run_operator(runtime(), MapOperator::new(|x: u32| x), inputs, out_tx));
        (a_tx, b_tx, out_rx)
    }

    /// The core exactly-once invariant: after input A delivers barrier 1,
    /// nothing more is read from A until B delivers it too, so the snapshot
    /// (and the forwarded barrier) splits both inputs at the same logical
    /// point.
    #[tokio::test]
    async fn aligns_barriers_across_inputs() {
        let (a_tx, b_tx, mut out_rx) = two_input_identity();

        a_tx.send(record(1)).await.unwrap();
        a_tx.send(barrier(1)).await.unwrap();
        a_tx.send(record(2)).await.unwrap(); // post-barrier: must wait
        b_tx.send(record(10)).await.unwrap();

        let mut seen = Vec::new();
        for _ in 0..2 {
            match out_rx.recv().await.unwrap() {
                StreamElement::Record(e) => seen.push(e.value),
                other => panic!("unexpected {other:?}"),
            }
        }
        seen.sort();
        assert_eq!(seen, vec![1, 10]);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(out_rx.try_recv().is_none(), "record 2 is held back until alignment");

        b_tx.send(barrier(1)).await.unwrap();
        assert!(matches!(out_rx.recv().await.unwrap(), StreamElement::Barrier(b) if b.checkpoint_id == 1));
        assert!(matches!(out_rx.recv().await.unwrap(), StreamElement::Record(e) if e.value == 2));

        a_tx.send(StreamElement::End).await.unwrap();
        b_tx.send(StreamElement::End).await.unwrap();
        // Both inputs ended: MAX watermark, then End.
        assert!(matches!(out_rx.recv().await.unwrap(), StreamElement::Watermark(u64::MAX)));
        assert!(matches!(out_rx.recv().await.unwrap(), StreamElement::End));
    }

    #[tokio::test]
    async fn finished_input_counts_as_aligned() {
        let (a_tx, b_tx, mut out_rx) = two_input_identity();
        b_tx.send(StreamElement::End).await.unwrap();
        a_tx.send(barrier(3)).await.unwrap();
        assert!(matches!(out_rx.recv().await.unwrap(), StreamElement::Barrier(b) if b.checkpoint_id == 3));
    }

    #[tokio::test]
    async fn newer_barrier_abandons_stale_alignment() {
        let (a_tx, b_tx, mut out_rx) = two_input_identity();
        a_tx.send(barrier(1)).await.unwrap();
        b_tx.send(barrier(2)).await.unwrap(); // B's source never saw trigger 1
        a_tx.send(barrier(2)).await.unwrap();
        assert!(matches!(out_rx.recv().await.unwrap(), StreamElement::Barrier(b) if b.checkpoint_id == 2));
    }

    #[tokio::test]
    async fn idle_input_lets_the_watermark_advance_and_idleness_propagates() {
        let (a_tx, b_tx, mut out_rx) = two_input_identity();
        a_tx.send(StreamElement::Watermark(100)).await.unwrap();
        b_tx.send(StreamElement::Status(StreamStatus::Idle)).await.unwrap();
        assert_eq!(out_rx.recv().await.unwrap(), StreamElement::Watermark(100));

        a_tx.send(StreamElement::Status(StreamStatus::Idle)).await.unwrap();
        assert_eq!(out_rx.recv().await.unwrap(), StreamElement::Status(StreamStatus::Idle));
        a_tx.send(StreamElement::Watermark(200)).await.unwrap(); // a is active again
        assert_eq!(out_rx.recv().await.unwrap(), StreamElement::Watermark(200));
        assert_eq!(out_rx.recv().await.unwrap(), StreamElement::Status(StreamStatus::Active));
    }

    #[tokio::test]
    async fn split_routes_records_and_broadcasts_control() {
        let (tx, rx) = channel(16);
        let (left_tx, mut left) = channel(16);
        let (right_tx, mut right) = channel::<StreamElement<String>>(16);
        let task = tokio::spawn(run_split(runtime(), rx, left_tx, right_tx));

        tx.send(StreamElement::Record(Event::new(Either::Left(1u32), 5))).await.unwrap();
        tx.send(StreamElement::Record(Event::new(Either::Right("late".to_string()), 1))).await.unwrap();
        tx.send(StreamElement::Watermark(7)).await.unwrap();
        tx.send(StreamElement::End).await.unwrap();

        assert!(matches!(left.recv().await.unwrap(), StreamElement::Record(e) if e.value == 1));
        assert_eq!(left.recv().await.unwrap(), StreamElement::Watermark(7));
        assert!(matches!(right.recv().await.unwrap(), StreamElement::Record(e) if e.value == "late"));
        assert_eq!(right.recv().await.unwrap(), StreamElement::Watermark(7));
        assert_eq!(left.recv().await.unwrap(), StreamElement::End);
        task.await.unwrap().unwrap();
    }
}
