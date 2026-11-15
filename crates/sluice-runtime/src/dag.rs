//! Pipeline construction.
//!
//! Every source, operator and sink becomes its own Tokio task. Adjacent
//! stages are connected by credit-based channels (see [`crate::channel`]),
//! so a slow stage stalls its upstream once its input buffer is full:
//! backpressure propagates all the way to the source.

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use sluice_checkpoint::StageInfo;
use sluice_core::{
    Context, Either, Event, FilterMapOperator, FilterOperator, KeyByOperator, MapOperator, Operator, OperatorError,
    StreamElement, Timestamp,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

use crate::channel::{self, Receiver, Sender};
use crate::job::JobHandle;
use crate::join::WindowJoinOperator;
use crate::metrics::OperatorMetrics;
use crate::sink::{CollectSink, Sink, StdoutSink};
use crate::source::{AlignmentGroup, ChannelSource, ElementsSource, IterSource, SourceFunction, WatermarkStrategy};
use crate::stage::{self, AttemptContext, BoxedInput, MapInput, StageRuntime};
use crate::window::{AggregateFunction, SlidingWindows, WindowResult, WindowSpec, WithLateOutput};

pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

pub(crate) type StageFuture = Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeError {
    #[error("stage `{0}` lost its input before end of stream")]
    InputClosed(String),
    #[error("stage `{stage}` panicked: {message}")]
    Panicked { stage: String, message: String },
    #[error("stage `{stage}` failed: {message}")]
    Stage { stage: String, message: String },
    #[error("stage `{0}` was killed")]
    Killed(String),
    #[error("building the pipeline panicked: {0}")]
    Build(String),
    #[error("checkpoint storage: {0}")]
    Storage(String),
}

/// Lifecycle state of a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum JobStatus {
    /// Running (possibly after restarts).
    Running,
    /// Every source ended and every stage drained.
    Finished,
    Failed { error: String },
    Cancelled,
}

impl JobStatus {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, JobStatus::Running)
    }
}

/// One built stage, ready to spawn.
pub(crate) struct StageSlot {
    pub info: StageInfo,
    pub name: String,
    pub metrics: Arc<OperatorMetrics>,
    pub future: Option<StageFuture>,
}

/// Collects the stages of a pipeline while it is being built.
///
/// Cloning is cheap; all clones share the same pipeline. With
/// [`run_job`](crate::run_job) the build closure receives a fresh
/// environment for every (re)start of the job.
#[derive(Clone)]
pub struct StreamEnv {
    inner: Arc<Mutex<EnvInner>>,
    ctx: Arc<AttemptContext>,
}

struct EnvInner {
    capacity: usize,
    stages: Vec<StageSlot>,
    alignment: HashMap<String, Arc<AlignmentGroup>>,
}

impl Default for StreamEnv {
    fn default() -> Self {
        Self::new()
    }
}

/// `03-window` style ids: stable across restarts as long as the pipeline is
/// built the same way, and safe as directory names.
fn stage_uid(index: usize, name: &str) -> String {
    let mut slug = String::with_capacity(name.len());
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug: String = slug.trim_matches('-').chars().take(40).collect();
    format!("{index:02}-{slug}")
}

impl StreamEnv {
    /// A standalone pipeline without checkpointing, started with
    /// [`execute`](Self::execute).
    pub fn new() -> Self {
        Self::with_channel_capacity(DEFAULT_CHANNEL_CAPACITY)
    }

    /// `capacity` is the number of credits (in-flight elements) between two
    /// adjacent stages.
    pub fn with_channel_capacity(capacity: usize) -> Self {
        Self::for_attempt(Arc::new(AttemptContext::standalone()), capacity)
    }

    pub(crate) fn for_attempt(ctx: Arc<AttemptContext>, capacity: usize) -> Self {
        assert!(capacity > 0, "channel capacity must be positive");
        Self {
            inner: Arc::new(Mutex::new(EnvInner {
                capacity,
                stages: Vec::new(),
                alignment: HashMap::new(),
            })),
            ctx,
        }
    }

    /// Which (re)start of the job this environment builds, from 0.
    pub fn attempt(&self) -> u32 {
        self.ctx.attempt
    }

    fn lock(&self) -> MutexGuard<'_, EnvInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn channel<T>(&self) -> (Sender<StreamElement<T>>, Receiver<StreamElement<T>>) {
        channel::channel(self.lock().capacity)
    }

    fn register(&self, name: &str, is_source: bool, listens: bool, inputs: Vec<usize>) -> StageRuntime {
        let mut inner = self.lock();
        let index = inner.stages.len();
        let uid = stage_uid(index, name);
        let metrics = Arc::new(OperatorMetrics::new(index, name));
        inner.stages.push(StageSlot {
            info: StageInfo {
                uid: uid.clone(),
                is_source,
                inputs,
            },
            name: name.to_owned(),
            metrics: Arc::clone(&metrics),
            future: None,
        });
        let control = listens
            .then(|| self.ctx.checkpointing.as_ref().map(|cp| cp.control.subscribe()))
            .flatten();
        StageRuntime {
            index,
            uid,
            name: name.to_owned(),
            metrics,
            ctx: Arc::clone(&self.ctx),
            control,
        }
    }

    fn set_future<F>(&self, index: usize, future: F)
    where
        F: Future<Output = Result<(), RuntimeError>> + Send + 'static,
    {
        self.lock().stages[index].future = Some(Box::pin(future));
    }

    pub(crate) fn take_stages(&self) -> Vec<StageSlot> {
        std::mem::take(&mut self.lock().stages)
    }

    /// A source reading from any [`SourceFunction`].
    pub fn from_source<T, S>(&self, name: &str, source: S, strategy: WatermarkStrategy) -> DataStream<T>
    where
        T: Send + 'static,
        S: SourceFunction<T>,
    {
        let rt = self.register(name, true, true, Vec::new());
        let index = rt.index;
        let (tx, rx) = self.channel();
        rt.metrics.add_output(tx.stats());
        let member = strategy.alignment.as_ref().map(|(group, max_drift)| {
            let group = Arc::clone(
                self.lock()
                    .alignment
                    .entry(group.clone())
                    .or_insert_with(|| Arc::new(AlignmentGroup::new(*max_drift))),
            );
            group.join()
        });
        self.set_future(index, stage::run_source(rt, source, strategy, member, tx));
        DataStream {
            env: self.clone(),
            rx,
            producer: index,
        }
    }

    /// A finite source over an iterator of events.
    pub fn from_iter<T, I>(&self, name: &str, events: I, strategy: WatermarkStrategy) -> DataStream<T>
    where
        T: Send + 'static,
        I: IntoIterator<Item = Event<T>>,
        I::IntoIter: Send + 'static,
    {
        self.from_source(name, IterSource::new(events.into_iter()), strategy)
    }

    /// A source that replays records and watermarks exactly as given.
    pub fn from_elements<T>(&self, name: &str, elements: Vec<StreamElement<T>>) -> DataStream<T>
    where
        T: Send + 'static,
    {
        self.from_source(name, ElementsSource::new(elements), WatermarkStrategy::none())
    }

    /// A source fed by another task. The stream ends when every sender for
    /// `events` is dropped. Not replayable.
    pub fn from_channel<T>(&self, name: &str, events: mpsc::Receiver<Event<T>>, strategy: WatermarkStrategy) -> DataStream<T>
    where
        T: Send + 'static,
    {
        self.from_source(name, ChannelSource::new(events), strategy)
    }

    /// Spawns the pipeline once, without checkpointing or restarts. Must be
    /// called from within a Tokio runtime. For fault-tolerant jobs use
    /// [`run_job`](crate::run_job).
    pub fn execute(&self) -> JobHandle {
        crate::job::execute_once(self.take_stages())
    }

    fn add_operator<O: Operator>(&self, name: &str, operator: O, inputs: Vec<Input<O::In>>) -> DataStream<O::Out> {
        let producers = inputs.iter().map(|i| i.producer).collect();
        let rt = self.register(name, false, false, producers);
        let index = rt.index;
        let (tx, rx) = self.channel();
        rt.metrics.add_output(tx.stats());
        let inputs = inputs
            .into_iter()
            .map(|input| {
                rt.metrics.add_input(input.stats);
                input.port
            })
            .collect();
        self.set_future(index, stage::run_operator(rt, operator, inputs, tx));
        DataStream {
            env: self.clone(),
            rx,
            producer: index,
        }
    }
}

/// The output of a pipeline stage, ready to be transformed further or
/// written to a sink.
#[must_use = "a stream does nothing unless it ends in a sink"]
pub struct DataStream<T> {
    env: StreamEnv,
    rx: Receiver<StreamElement<T>>,
    /// Index of the stage producing this stream.
    producer: usize,
}

/// A stream being wired into an operator as one of its inputs.
struct Input<T> {
    port: BoxedInput<T>,
    stats: Arc<channel::ChannelStats>,
    producer: usize,
}

/// Forwards its input unchanged; the operator behind [`DataStream::union`].
struct PassThrough<T>(PhantomData<fn(T)>);

impl<T: Send + 'static> Operator for PassThrough<T> {
    type In = T;
    type Out = T;

    fn process(&mut self, event: Event<T>, ctx: &mut Context<'_, T>) -> Result<(), OperatorError> {
        ctx.emit(event);
        Ok(())
    }
}

impl<T: Send + 'static> DataStream<T> {
    pub fn env(&self) -> &StreamEnv {
        &self.env
    }

    fn into_input(self) -> Input<T> {
        Input {
            stats: self.rx.stats(),
            producer: self.producer,
            port: Box::new(self.rx),
        }
    }

    fn mapped_input<U: Send + 'static>(self, f: fn(T) -> U) -> Input<U> {
        Input {
            stats: self.rx.stats(),
            producer: self.producer,
            port: Box::new(MapInput::new(self.rx, f)),
        }
    }

    /// Appends an arbitrary operator as a new stage.
    pub fn process<O>(self, name: &str, operator: O) -> DataStream<O::Out>
    where
        O: Operator<In = T>,
    {
        let env = self.env.clone();
        env.add_operator(name, operator, vec![self.into_input()])
    }

    /// Merges streams of the same type. The union stage tracks the minimum
    /// watermark across its (active) inputs and aligns checkpoint barriers.
    pub fn union(self, others: impl IntoIterator<Item = DataStream<T>>) -> DataStream<T> {
        let env = self.env.clone();
        let mut inputs = vec![self.into_input()];
        inputs.extend(others.into_iter().map(DataStream::into_input));
        env.add_operator("union", PassThrough(PhantomData), inputs)
    }

    pub fn map<U, F>(self, f: F) -> DataStream<U>
    where
        U: Send + 'static,
        F: FnMut(T) -> U + Send + 'static,
    {
        self.process("map", MapOperator::new(f))
    }

    pub fn filter<P>(self, predicate: P) -> DataStream<T>
    where
        P: FnMut(&T) -> bool + Send + 'static,
    {
        self.process("filter", FilterOperator::new(predicate))
    }

    pub fn filter_map<U, F>(self, f: F) -> DataStream<U>
    where
        U: Send + 'static,
        F: FnMut(T) -> Option<U> + Send + 'static,
    {
        self.process("filter_map", FilterMapOperator::new(f))
    }

    pub fn key_by<K, F>(self, mut key_fn: F) -> DataStream<T>
    where
        K: Into<Vec<u8>>,
        F: FnMut(&T) -> K + Send + 'static,
    {
        self.process("key_by", KeyByOperator::new(move |value: &T| key_fn(value).into()))
    }

    /// Keyed event-time window aggregation that drops late events. Put a
    /// [`key_by`](Self::key_by) in front unless one global window per time
    /// range is intended. See [`windowed`](Self::windowed) for allowed
    /// lateness and late side outputs.
    pub fn window<W, Agg>(self, windows: W, aggregate: Agg) -> DataStream<WindowResult<Agg::Out>>
    where
        W: WindowSpec,
        Agg: AggregateFunction<T>,
    {
        self.windowed(windows).aggregate(aggregate)
    }

    /// Starts a windowed aggregation with options.
    pub fn windowed<W: WindowSpec>(self, windows: W) -> WindowedStream<T, W> {
        WindowedStream {
            stream: self,
            windows,
            allowed_lateness: 0,
        }
    }

    /// Starts a windowed inner join with `right`.
    pub fn join<R: Send + 'static>(self, right: DataStream<R>) -> JoinedStreams<T, R> {
        JoinedStreams { left: self, right }
    }

    pub fn sink<S: Sink<T>>(self, name: &str, sink: S) {
        let rt = self.env.register(name, false, true, vec![self.producer]);
        rt.metrics.add_input(self.rx.stats());
        let index = rt.index;
        self.env.set_future(index, stage::run_sink(rt, sink, self.rx));
    }

    /// Ends the stream in an unbounded in-memory buffer (at-least-once).
    pub fn collect(self) -> CollectSink<T> {
        let sink = CollectSink::new();
        self.sink("collect", sink.clone());
        sink
    }

    pub fn print(self)
    where
        T: Debug,
    {
        self.sink("print", StdoutSink::new());
    }
}

impl<A: Send + 'static, B: Send + 'static> DataStream<Either<A, B>> {
    /// Routes `Left` records to the first stream and `Right` records to the
    /// second (side outputs). Watermarks, barriers and the end of stream go
    /// to both.
    pub fn split(self, name: &str) -> (DataStream<A>, DataStream<B>) {
        let env = self.env.clone();
        let rt = env.register(name, false, false, vec![self.producer]);
        rt.metrics.add_input(self.rx.stats());
        let (left_tx, left_rx) = env.channel();
        let (right_tx, right_rx) = env.channel();
        rt.metrics.add_output(left_tx.stats());
        rt.metrics.add_output(right_tx.stats());
        let index = rt.index;
        env.set_future(index, stage::run_split(rt, self.rx, left_tx, right_tx));
        (
            DataStream {
                env: env.clone(),
                rx: left_rx,
                producer: index,
            },
            DataStream {
                env,
                rx: right_rx,
                producer: index,
            },
        )
    }
}

/// A windowed aggregation being configured; see [`DataStream::windowed`].
pub struct WindowedStream<T, W> {
    stream: DataStream<T>,
    windows: W,
    allowed_lateness: Timestamp,
}

impl<T: Send + 'static, W: WindowSpec> WindowedStream<T, W> {
    /// Keep window state this long after a window fires: late events within
    /// it update the window and re-emit its result (`late_firing`).
    pub fn allowed_lateness(mut self, lateness: Duration) -> Self {
        self.allowed_lateness = lateness.as_millis() as Timestamp;
        self
    }

    pub fn aggregate<Agg: AggregateFunction<T>>(self, aggregate: Agg) -> DataStream<WindowResult<Agg::Out>> {
        let operator = self.windows.operator(aggregate, self.allowed_lateness);
        self.stream.process("window", operator)
    }

    /// Like [`aggregate`](Self::aggregate), but events too late even for
    /// the allowed lateness come out of the second stream instead of being
    /// dropped.
    pub fn aggregate_with_late_output<Agg: AggregateFunction<T>>(
        self,
        aggregate: Agg,
    ) -> (DataStream<WindowResult<Agg::Out>>, DataStream<T>) {
        let operator = WithLateOutput::new(self.windows.operator(aggregate, self.allowed_lateness));
        self.stream.process("window", operator).split("late-split")
    }
}

/// A join being configured; see [`DataStream::join`].
pub struct JoinedStreams<L, R> {
    left: DataStream<L>,
    right: DataStream<R>,
}

/// Extracts the join key from an element.
pub type KeyFn<T> = Box<dyn Fn(&T) -> Vec<u8> + Send>;

impl<L: Send + 'static, R: Send + 'static> JoinedStreams<L, R> {
    /// Joins elements whose keys are equal.
    pub fn on<K1, K2>(
        self,
        left_key: impl Fn(&L) -> K1 + Send + 'static,
        right_key: impl Fn(&R) -> K2 + Send + 'static,
    ) -> KeyedJoin<L, R>
    where
        K1: Into<Vec<u8>>,
        K2: Into<Vec<u8>>,
    {
        KeyedJoin {
            left: self.left,
            right: self.right,
            left_key: Box::new(move |l: &L| left_key(l).into()),
            right_key: Box::new(move |r: &R| right_key(r).into()),
        }
    }
}

pub struct KeyedJoin<L, R> {
    left: DataStream<L>,
    right: DataStream<R>,
    left_key: KeyFn<L>,
    right_key: KeyFn<R>,
}

impl<L, R> KeyedJoin<L, R>
where
    L: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
{
    /// Pairs elements of the same key falling in the same tumbling or
    /// sliding window.
    pub fn window(self, windows: impl Into<SlidingWindows>) -> WindowedJoin<L, R> {
        WindowedJoin {
            join: self,
            windows: windows.into(),
        }
    }
}

pub struct WindowedJoin<L, R> {
    join: KeyedJoin<L, R>,
    windows: SlidingWindows,
}

impl<L, R> WindowedJoin<L, R>
where
    L: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
{
    /// Emits `f(left, right)` for every matching pair when the window
    /// closes. The output is keyed by the join key and timestamped at the
    /// window's end.
    pub fn apply<O, F>(self, f: F) -> DataStream<O>
    where
        O: Send + 'static,
        F: Fn(&L, &R) -> O + Send + 'static,
    {
        let KeyedJoin {
            left,
            right,
            left_key,
            right_key,
        } = self.join;
        let env = left.env.clone();
        let inputs = vec![
            left.mapped_input(Either::Left as fn(L) -> Either<L, R>),
            right.mapped_input(Either::Right as fn(R) -> Either<L, R>),
        ];
        env.add_operator(
            "join",
            WindowJoinOperator::new(self.windows, left_key, right_key, f),
            inputs,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::stage_uid;

    #[test]
    fn stage_uids_are_path_safe() {
        assert_eq!(stage_uid(3, "window(tumbling 60000ms, sum(amount))"), "03-window-tumbling-60000ms-sum-amount");
        assert_eq!(stage_uid(0, "source(grpc:purchases)"), "00-source-grpc-purchases");
        assert_eq!(stage_uid(12, "Ünïcode!"), "12-n-code");
    }
}
