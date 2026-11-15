//! JSON job definitions and their compilation to runtime pipelines.
//!
//! Records flowing through a submitted job are JSON objects. Example:
//!
//! ```json
//! {
//!   "name": "spend-per-user",
//!   "source": { "type": "grpc", "stream": "purchases", "max_out_of_orderness_ms": 2000 },
//!   "operators": [
//!     { "type": "filter", "field": "amount", "op": "gt", "value": 0 },
//!     { "type": "key_by", "field": "user_id" },
//!     { "type": "window", "window": { "kind": "tumbling", "size_ms": 60000 },
//!       "aggregate": { "fn": "sum", "field": "amount" } }
//!   ],
//!   "sink": { "type": "buffer", "capacity": 1000 }
//! }
//! ```

use std::cmp::Ordering;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use sluice_core::{
    CheckpointId, Context, Either, Event, FilterMapOperator, FilterOperator, KeyByOperator, MapOperator, Operator,
    OperatorError, Timestamp,
};
use sluice_ingest::{LogSource, Subscription};
use sluice_runtime::window::{
    AggregateFunction, SessionWindows, SlidingWindows, TumblingWindows, WindowResult, WindowSpec, WithLateOutput,
};
use sluice_runtime::{
    CheckpointConfig, CollectSink, DataStream, JobConfig, RestartStrategy, Sink, StateBackendKind, StreamEnv,
    WatermarkStrategy,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobDefinition {
    pub name: String,
    pub source: SourceDef,
    #[serde(default)]
    pub operators: Vec<OperatorDef>,
    /// A streaming SQL query to run instead of `operators`; its FROM names
    /// the source stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default)]
    pub sink: SinkDef,
    /// Enables checkpoints, recovery from them, and exactly-once output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpointing: Option<CheckpointingDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart: Option<RestartDef>,
}

/// With checkpointing, sink output becomes visible when the checkpoint
/// covering it completes, so it lags by up to one interval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointingDef {
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_retained")]
    pub retained: usize,
    #[serde(default = "default_backend")]
    pub backend: StateBackendKind,
}

fn default_interval_ms() -> u64 {
    5_000
}
fn default_timeout_ms() -> u64 {
    60_000
}
fn default_retained() -> usize {
    3
}
fn default_backend() -> StateBackendKind {
    StateBackendKind::RocksDb
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartDef {
    #[serde(default = "default_max_restarts")]
    pub max_restarts: u32,
    #[serde(default = "default_delay_ms")]
    pub delay_ms: u64,
}

fn default_max_restarts() -> u32 {
    3
}
fn default_delay_ms() -> u64 {
    1_000
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceDef {
    /// Events sent to `stream` through the gRPC ingest service. Payloads
    /// must be JSON; anything else is dropped by the decode stage.
    Grpc {
        stream: String,
        #[serde(default)]
        max_out_of_orderness_ms: Timestamp,
        /// Declare the stream idle after this long without events, so a
        /// quiet stream does not hold back event time.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idle_timeout_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OperatorDef {
    /// Keeps records where `field <op> value`. `field` may be a dotted path.
    Filter { field: String, op: CompareOp, value: Value },
    /// Keeps only the listed fields.
    Map { select: Vec<String> },
    /// Partitions the stream by a field's value.
    KeyBy { field: String },
    /// Keyed event-time window aggregation.
    Window {
        window: WindowDef,
        aggregate: AggregateDef,
        /// Keep window state this long after firing; late events within it
        /// re-emit the window with `"late_firing": true`.
        #[serde(default)]
        allowed_lateness_ms: Timestamp,
        /// What to do with events later than that: drop them (counted in
        /// `late_records`) or emit them to the job output as
        /// `{"late_event": <record>}`.
        #[serde(default)]
        late_events: LateEventsDef,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LateEventsDef {
    #[default]
    Drop,
    Emit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WindowDef {
    Tumbling { size_ms: Timestamp },
    Sliding { size_ms: Timestamp, slide_ms: Timestamp },
    Session { gap_ms: Timestamp },
}

/// Window aggregates. Numeric aggregates skip records where the field is
/// missing or not a number; `avg`, `min` and `max` yield `null` for a window
/// with no numeric values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "fn", rename_all = "snake_case")]
pub enum AggregateDef {
    Count,
    Sum { field: String },
    Avg { field: String },
    Min { field: String },
    Max { field: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkDef {
    /// Keeps the most recent `capacity` results for `GET /jobs/:id/output`
    /// and, if `log` is set, logs each one.
    Buffer {
        #[serde(default = "default_buffer_capacity")]
        capacity: usize,
        #[serde(default)]
        log: bool,
    },
}

fn default_buffer_capacity() -> usize {
    1_000
}

impl Default for SinkDef {
    fn default() -> Self {
        SinkDef::Buffer {
            capacity: default_buffer_capacity(),
            log: false,
        }
    }
}

impl JobDefinition {
    /// Checks the definition without building anything.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("`name` must not be empty".into());
        }
        match &self.source {
            SourceDef::Grpc { stream, .. } if stream.trim().is_empty() => {
                return Err("`source.stream` must not be empty".into());
            }
            SourceDef::Grpc { .. } => {}
        }
        if let Some(query) = &self.query {
            if !self.operators.is_empty() {
                return Err("use either `query` or `operators`, not both".into());
            }
            let compiled = sluice_sql::compile(query).map_err(|e| format!("query: {e}"))?;
            if compiled.physical.stream != self.stream() {
                return Err(format!(
                    "query reads from `{}` but the source stream is `{}`",
                    compiled.physical.stream,
                    self.stream()
                ));
            }
        }

        let mut keyed = false;
        for (i, op) in self.operators.iter().enumerate() {
            let fail = |msg: &str| Err(format!("operators[{i}] ({}): {msg}", op.kind()));
            match op {
                OperatorDef::Filter { field, .. } if field.is_empty() => return fail("`field` must not be empty"),
                OperatorDef::Filter { .. } => {}
                OperatorDef::Map { select } if select.is_empty() || select.iter().any(String::is_empty) => {
                    return fail("`select` must list at least one non-empty field");
                }
                OperatorDef::Map { .. } => {}
                OperatorDef::KeyBy { field } if field.is_empty() => return fail("`field` must not be empty"),
                OperatorDef::KeyBy { .. } => keyed = true,
                OperatorDef::Window { window, aggregate, .. } => {
                    if !keyed {
                        return fail("a window must be preceded by a key_by");
                    }
                    match *window {
                        WindowDef::Tumbling { size_ms: 0 } => return fail("`size_ms` must be positive"),
                        WindowDef::Sliding { size_ms, slide_ms } if slide_ms == 0 || slide_ms > size_ms => {
                            return fail("`slide_ms` must be positive and at most `size_ms`");
                        }
                        WindowDef::Session { gap_ms: 0 } => return fail("`gap_ms` must be positive"),
                        _ => {}
                    }
                    if aggregate.field().is_some_and(str::is_empty) {
                        return fail("aggregate `field` must not be empty");
                    }
                }
            }
        }

        let SinkDef::Buffer { capacity, .. } = &self.sink;
        if *capacity == 0 {
            return Err("`sink.capacity` must be positive".into());
        }
        if let Some(cp) = &self.checkpointing {
            if cp.interval_ms == 0 || cp.timeout_ms == 0 {
                return Err("`checkpointing.interval_ms` and `timeout_ms` must be positive".into());
            }
            if cp.retained == 0 {
                return Err("`checkpointing.retained` must be at least 1".into());
            }
        }
        Ok(())
    }

    /// Window stages by name, with their aggregate (for decoding state).
    pub fn window_aggregates(&self) -> Vec<(String, AggregateDef)> {
        self.operators
            .iter()
            .filter_map(|op| match op {
                OperatorDef::Window { aggregate, .. } => Some((op.stage_name()?, aggregate.clone())),
                _ => None,
            })
            .collect()
    }

    /// The stream the job's source reads.
    pub fn stream(&self) -> &str {
        let SourceDef::Grpc { stream, .. } = &self.source;
        stream
    }

    /// The buffer the job's sink commits to: exactly-once when checkpointing.
    pub fn output_buffer(&self) -> CollectSink<Value> {
        let SinkDef::Buffer { capacity, .. } = self.sink;
        let buffer = CollectSink::bounded(capacity);
        if self.checkpointing.is_some() { buffer.exactly_once() } else { buffer }
    }

    /// Runtime settings. Checkpoints go to `checkpoint_dir`.
    pub fn job_config(&self, checkpoint_dir: &Path) -> JobConfig {
        let mut config = JobConfig::new(self.name.clone());
        if let Some(cp) = &self.checkpointing {
            config = config.with_checkpointing(
                CheckpointConfig::new(checkpoint_dir)
                    .with_interval(Duration::from_millis(cp.interval_ms))
                    .with_timeout(Duration::from_millis(cp.timeout_ms))
                    .with_retained(cp.retained)
                    .with_backend(cp.backend),
            );
        }
        let restart = match (&self.restart, &self.checkpointing) {
            (Some(r), _) => RestartStrategy::fixed_delay(r.max_restarts, Duration::from_millis(r.delay_ms)),
            (None, Some(_)) => RestartStrategy::default(),
            (None, None) => RestartStrategy::none(),
        };
        config.with_restart(restart)
    }

    /// Builds the pipeline into `env`. Called again on every restart, so it
    /// only reads its inputs. Call [`validate`](Self::validate) first.
    pub fn build(&self, env: &StreamEnv, subscription: &Arc<Subscription>, output: &CollectSink<Value>) {
        let SourceDef::Grpc {
            stream,
            max_out_of_orderness_ms,
            idle_timeout_ms,
        } = &self.source;
        let mut strategy = WatermarkStrategy::bounded_out_of_orderness(*max_out_of_orderness_ms);
        if let Some(idle) = idle_timeout_ms {
            strategy = strategy.with_idleness(Duration::from_millis(*idle));
        }

        let raw = env.from_source(
            &format!("source(grpc:{stream})"),
            LogSource::new(Arc::clone(subscription)),
            strategy,
        );
        let mut data: DataStream<Value> = match &self.query {
            // The query's scan stage decodes, filters and prunes records.
            Some(query) => sluice_sql::compile(query)
                .expect("validated before building")
                .physical
                .build(raw),
            None => raw.process(
                "decode(json)",
                FilterMapOperator::new(|bytes: Vec<u8>| serde_json::from_slice::<Value>(&bytes).ok()),
            ),
        };

        // Late-event side outputs bypass later operators and join the main
        // stream just before the sink.
        let mut late_streams = Vec::new();
        for op in &self.operators {
            let (main, late) = op.apply(data);
            data = main;
            late_streams.extend(late);
        }
        if !late_streams.is_empty() {
            data = data.union(late_streams);
        }

        let SinkDef::Buffer { log, .. } = self.sink;
        data.sink(
            "sink(buffer)",
            BufferSink {
                inner: output.clone(),
                log,
                job: self.name.clone(),
            },
        );
    }
}

/// The REST job sink: a [`CollectSink`] that can also log each record it
/// publishes.
struct BufferSink {
    inner: CollectSink<Value>,
    log: bool,
    job: String,
}

impl Sink<Value> for BufferSink {
    fn write(&mut self, event: Event<Value>) {
        if self.log {
            tracing::info!(job = %self.job, event_time = event.event_time, value = %event.value, "job output");
        }
        self.inner.write(event);
    }
    fn pre_commit(&mut self, checkpoint_id: CheckpointId) {
        self.inner.pre_commit(checkpoint_id);
    }
    fn commit(&mut self, checkpoint_id: CheckpointId) {
        self.inner.commit(checkpoint_id);
    }
    fn restore(&mut self, checkpoint: Option<CheckpointId>) {
        self.inner.restore(checkpoint);
    }
    fn finish(&mut self) {
        self.inner.finish();
    }
}

impl OperatorDef {
    /// Name of the window stage this operator builds (other operators'
    /// names are built inline).
    fn stage_name(&self) -> Option<String> {
        let OperatorDef::Window {
            window,
            aggregate,
            allowed_lateness_ms,
            ..
        } = self
        else {
            return None;
        };
        let mut name = match window {
            WindowDef::Tumbling { size_ms } => format!("window(tumbling {size_ms}ms, {aggregate}"),
            WindowDef::Sliding { size_ms, slide_ms } => format!("window(sliding {size_ms}ms every {slide_ms}ms, {aggregate}"),
            WindowDef::Session { gap_ms } => format!("window(session gap {gap_ms}ms, {aggregate}"),
        };
        if *allowed_lateness_ms > 0 {
            name.push_str(&format!(", lateness {allowed_lateness_ms}ms"));
        }
        name.push(')');
        Some(name)
    }

    fn kind(&self) -> &'static str {
        match self {
            OperatorDef::Filter { .. } => "filter",
            OperatorDef::Map { .. } => "map",
            OperatorDef::KeyBy { .. } => "key_by",
            OperatorDef::Window { .. } => "window",
        }
    }

    /// Appends this operator; windows that emit late events also return
    /// that side output.
    fn apply(&self, data: DataStream<Value>) -> (DataStream<Value>, Option<DataStream<Value>>) {
        let main = match self.clone() {
            OperatorDef::Filter { field, op, value } => data.process(
                &format!("filter({field} {op} {value})"),
                FilterOperator::new(move |record: &Value| op.matches(lookup(record, &field), &value)),
            ),
            OperatorDef::Map { select } => data.process(
                &format!("map(select {})", select.join(", ")),
                MapOperator::new(move |record: Value| project(&record, &select)),
            ),
            OperatorDef::KeyBy { field } => data.process(
                &format!("key_by({field})"),
                KeyByOperator::new(move |record: &Value| key_bytes(lookup(record, &field))),
            ),
            OperatorDef::Window {
                window,
                aggregate,
                allowed_lateness_ms,
                late_events,
            } => {
                let name = self.stage_name().expect("windows have a stage name");
                let stage = WindowStage {
                    name,
                    aggregate,
                    allowed_lateness: allowed_lateness_ms,
                    late_events,
                };
                return match window {
                    WindowDef::Tumbling { size_ms } => stage.build(data, TumblingWindows::of_millis(size_ms)),
                    WindowDef::Sliding { size_ms, slide_ms } => {
                        stage.build(data, SlidingWindows::of_millis(size_ms, slide_ms))
                    }
                    WindowDef::Session { gap_ms } => stage.build(data, SessionWindows::with_gap_millis(gap_ms)),
                };
            }
        };
        (main, None)
    }
}

struct WindowStage {
    name: String,
    aggregate: AggregateDef,
    allowed_lateness: Timestamp,
    late_events: LateEventsDef,
}

impl WindowStage {
    fn build<W: WindowSpec>(self, data: DataStream<Value>, windows: W) -> (DataStream<Value>, Option<DataStream<Value>>) {
        let label = self.aggregate.name();
        let operator = windows.operator::<Value, AggregateDef>(self.aggregate, self.allowed_lateness);
        match self.late_events {
            LateEventsDef::Drop => (data.process(&self.name, JsonWindowOperator::new(operator, label)), None),
            LateEventsDef::Emit => {
                let (results, late) = data
                    .process(&self.name, JsonWindowOperator::new(WithLateOutput::new(operator), label))
                    .split("late-events");
                let late = late.process(
                    "late-events(json)",
                    MapOperator::new(|record: Value| json!({ "late_event": record })),
                );
                (results, Some(late))
            }
        }
    }
}

/// Resolves a dotted path such as `order.items.0.price`.
fn lookup<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(record, |current, segment| match current {
        Value::Array(items) => segment.parse::<usize>().ok().and_then(|i| items.get(i)),
        _ => current.get(segment),
    })
}

fn project(record: &Value, fields: &[String]) -> Value {
    let mut out = Map::with_capacity(fields.len());
    for field in fields {
        if let Some(value) = lookup(record, field) {
            out.insert(field.clone(), value.clone());
        }
    }
    Value::Object(out)
}

fn key_bytes(value: Option<&Value>) -> Vec<u8> {
    match value {
        Some(Value::String(s)) => s.as_bytes().to_vec(),
        Some(other) => other.to_string().into_bytes(),
        None => b"null".to_vec(),
    }
}

impl CompareOp {
    fn matches(self, lhs: Option<&Value>, rhs: &Value) -> bool {
        let Some(lhs) = lhs else {
            return self == CompareOp::Ne;
        };
        let ordering = compare(lhs, rhs);
        match self {
            CompareOp::Eq => ordering == Some(Ordering::Equal),
            CompareOp::Ne => ordering != Some(Ordering::Equal),
            CompareOp::Gt => ordering == Some(Ordering::Greater),
            CompareOp::Gte => matches!(ordering, Some(Ordering::Greater | Ordering::Equal)),
            CompareOp::Lt => ordering == Some(Ordering::Less),
            CompareOp::Lte => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
        }
    }
}

fn compare(lhs: &Value, rhs: &Value) -> Option<Ordering> {
    match (lhs, rhs) {
        (Value::Number(a), Value::Number(b)) => a.as_f64()?.partial_cmp(&b.as_f64()?),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (a, b) => (a == b).then_some(Ordering::Equal),
    }
}

impl fmt::Display for CompareOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CompareOp::Eq => "==",
            CompareOp::Ne => "!=",
            CompareOp::Gt => ">",
            CompareOp::Gte => ">=",
            CompareOp::Lt => "<",
            CompareOp::Lte => "<=",
        })
    }
}

impl AggregateDef {
    fn name(&self) -> &'static str {
        match self {
            AggregateDef::Count => "count",
            AggregateDef::Sum { .. } => "sum",
            AggregateDef::Avg { .. } => "avg",
            AggregateDef::Min { .. } => "min",
            AggregateDef::Max { .. } => "max",
        }
    }

    fn field(&self) -> Option<&str> {
        match self {
            AggregateDef::Count => None,
            AggregateDef::Sum { field }
            | AggregateDef::Avg { field }
            | AggregateDef::Min { field }
            | AggregateDef::Max { field } => Some(field),
        }
    }
}

impl fmt::Display for AggregateDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.field() {
            Some(field) => write!(f, "{}({field})", self.name()),
            None => write!(f, "{}(*)", self.name()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonAcc {
    count: u64,
    numeric: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl AggregateFunction<Value> for AggregateDef {
    type Acc = JsonAcc;
    type Out = Value;

    fn create_accumulator(&self) -> JsonAcc {
        JsonAcc {
            count: 0,
            numeric: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    fn add(&self, acc: &mut JsonAcc, record: &Value) {
        acc.count += 1;
        if let Some(x) = self.field().and_then(|f| lookup(record, f)).and_then(Value::as_f64) {
            acc.numeric += 1;
            acc.sum += x;
            acc.min = acc.min.min(x);
            acc.max = acc.max.max(x);
        }
    }

    fn merge(&self, a: JsonAcc, b: JsonAcc) -> JsonAcc {
        JsonAcc {
            count: a.count + b.count,
            numeric: a.numeric + b.numeric,
            sum: a.sum + b.sum,
            min: a.min.min(b.min),
            max: a.max.max(b.max),
        }
    }

    fn result(&self, acc: JsonAcc) -> Value {
        let any = acc.numeric > 0;
        match self {
            AggregateDef::Count => json!(acc.count),
            AggregateDef::Sum { .. } => json!(acc.sum),
            AggregateDef::Avg { .. } if any => json!(acc.sum / acc.numeric as f64),
            AggregateDef::Min { .. } if any => json!(acc.min),
            AggregateDef::Max { .. } if any => json!(acc.max),
            _ => Value::Null,
        }
    }
}

/// A window result as a JSON record:
/// `{"key", "window_start", "window_end", "<fn>": value}`, plus
/// `"late_firing": true` for an update caused by a late event.
fn result_json(result: WindowResult<Value>, label: &str) -> Value {
    let mut record = Map::new();
    record.insert("key".into(), json!(result.key_str()));
    record.insert("window_start".into(), json!(result.window.start));
    record.insert("window_end".into(), json!(result.window.end));
    record.insert(label.into(), result.value);
    if result.late_firing {
        record.insert("late_firing".into(), json!(true));
    }
    Value::Object(record)
}

/// Window outputs that can be turned into JSON records.
trait IntoJson {
    type Json: Send + 'static;
    fn into_json(self, label: &str) -> Self::Json;
}

impl IntoJson for WindowResult<Value> {
    type Json = Value;
    fn into_json(self, label: &str) -> Value {
        result_json(self, label)
    }
}

/// Results (left) and late events (right) of a window with a side output.
impl IntoJson for Either<WindowResult<Value>, Value> {
    type Json = Either<Value, Value>;
    fn into_json(self, label: &str) -> Either<Value, Value> {
        match self {
            Either::Left(result) => Either::Left(result_json(result, label)),
            Either::Right(late) => Either::Right(late),
        }
    }
}

/// Wraps any window operator so its results come out as JSON records.
struct JsonWindowOperator<W: Operator> {
    inner: W,
    label: &'static str,
    results: Vec<Event<W::Out>>,
}

impl<W: Operator> JsonWindowOperator<W> {
    fn new(inner: W, label: &'static str) -> Self {
        Self {
            inner,
            label,
            results: Vec::new(),
        }
    }
}

impl<W> Operator for JsonWindowOperator<W>
where
    W: Operator<In = Value>,
    W::Out: IntoJson,
{
    type In = Value;
    type Out = <W::Out as IntoJson>::Json;

    fn state_names(&self) -> &'static [&'static str] {
        self.inner.state_names()
    }

    fn process(&mut self, event: Event<Value>, ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError> {
        self.inner.process(event, &mut Context::new(&mut self.results, ctx.state()))?;
        for event in self.results.drain(..) {
            ctx.emit(event.map_value(|r| r.into_json(self.label)));
        }
        Ok(())
    }

    fn on_watermark(&mut self, watermark: Timestamp, ctx: &mut Context<'_, Self::Out>) -> Result<(), OperatorError> {
        self.inner.on_watermark(watermark, &mut Context::new(&mut self.results, ctx.state()))?;
        for event in self.results.drain(..) {
            ctx.emit(event.map_value(|r| r.into_json(self.label)));
        }
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>, OperatorError> {
        self.inner.snapshot_state()
    }

    fn restore_state(&mut self, state: &[u8]) -> Result<(), OperatorError> {
        self.inner.restore_state(state)
    }

    fn late_records(&self) -> u64 {
        self.inner.late_records()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(v: Value) -> Result<JobDefinition, String> {
        let def: JobDefinition = serde_json::from_value(v).map_err(|e| e.to_string())?;
        def.validate()?;
        Ok(def)
    }

    fn base(operators: Value) -> Value {
        json!({ "name": "j", "source": { "type": "grpc", "stream": "s" }, "operators": operators })
    }

    #[test]
    fn parses_full_definition_with_defaults() {
        let def = parse(base(json!([
            { "type": "filter", "field": "amount", "op": "gte", "value": 10 },
            { "type": "map", "select": ["user_id", "amount"] },
            { "type": "key_by", "field": "user_id" },
            { "type": "window", "window": { "kind": "tumbling", "size_ms": 1000 }, "aggregate": { "fn": "count" } }
        ])))
        .unwrap();
        assert_eq!(def.operators.len(), 4);
        assert_eq!(def.sink, SinkDef::default());
        let SourceDef::Grpc { max_out_of_orderness_ms, .. } = def.source.clone();
        assert_eq!(max_out_of_orderness_ms, 0);
    }

    #[test]
    fn window_kinds_and_late_handling_parse_and_validate() {
        let window = |w: Value| json!([{ "type": "key_by", "field": "u" }, { "type": "window", "window": w, "aggregate": { "fn": "count" } }]);
        let def = parse(base(window(json!({ "kind": "sliding", "size_ms": 1000, "slide_ms": 250 })))).unwrap();
        assert!(matches!(&def.operators[1], OperatorDef::Window { window: WindowDef::Sliding { slide_ms: 250, .. }, late_events: LateEventsDef::Drop, allowed_lateness_ms: 0, .. }));
        assert!(parse(base(window(json!({ "kind": "session", "gap_ms": 30000 })))).is_ok());

        let err = parse(base(window(json!({ "kind": "sliding", "size_ms": 1000, "slide_ms": 2000 })))).unwrap_err();
        assert!(err.contains("slide_ms"), "{err}");
        let err = parse(base(window(json!({ "kind": "session", "gap_ms": 0 })))).unwrap_err();
        assert!(err.contains("gap_ms"), "{err}");

        let mut ops = window(json!({ "kind": "tumbling", "size_ms": 1000 }));
        ops[1]["allowed_lateness_ms"] = json!(500);
        ops[1]["late_events"] = json!("emit");
        let def = parse(base(ops)).unwrap();
        assert!(matches!(&def.operators[1], OperatorDef::Window { allowed_lateness_ms: 500, late_events: LateEventsDef::Emit, .. }));
    }

    #[test]
    fn result_json_marks_late_firings() {
        let result = |late_firing| WindowResult {
            key: b"k".to_vec(),
            window: sluice_runtime::window::TimeWindow { start: 0, end: 10 },
            value: json!(3),
            late_firing,
        };
        assert_eq!(result_json(result(false), "count"), json!({ "key": "k", "window_start": 0, "window_end": 10, "count": 3 }));
        assert_eq!(result_json(result(true), "count")["late_firing"], json!(true));
    }

    #[test]
    fn checkpointing_and_restart_defaults() {
        let mut v = base(json!([]));
        v["checkpointing"] = json!({});
        let def = parse(v).unwrap();
        let cp = def.checkpointing.clone().unwrap();
        assert_eq!((cp.interval_ms, cp.timeout_ms, cp.retained), (5_000, 60_000, 3));
        assert_eq!(cp.backend, StateBackendKind::RocksDb);
        let config = def.job_config(Path::new("/tmp/x"));
        assert_eq!(config.restart, RestartStrategy::default(), "checkpointed jobs restart by default");
        assert!(def.output_buffer().is_empty());

        let mut v = base(json!([]));
        v["checkpointing"] = json!({ "backend": "memory", "interval_ms": 100 });
        v["restart"] = json!({ "max_restarts": 0 });
        let def = parse(v).unwrap();
        assert_eq!(def.checkpointing.as_ref().unwrap().backend, StateBackendKind::Memory);
        assert_eq!(def.job_config(Path::new("/tmp/x")).restart.max_restarts, 0);

        let plain = parse(base(json!([]))).unwrap();
        assert!(plain.job_config(Path::new("/tmp/x")).checkpointing.is_none());
    }

    #[test]
    fn rejects_invalid_definitions() {
        let window = json!({ "type": "window", "window": { "kind": "tumbling", "size_ms": 1000 }, "aggregate": { "fn": "count" } });
        let err = parse(base(json!([window]))).unwrap_err();
        assert!(err.contains("preceded by a key_by"), "{err}");

        let err = parse(base(json!([{ "type": "key_by", "field": "" }]))).unwrap_err();
        assert!(err.contains("operators[0]"), "{err}");

        let err = parse(base(json!([{ "type": "explode" }]))).unwrap_err();
        assert!(err.contains("explode"), "{err}");

        let err = parse(json!({ "name": "j", "source": { "type": "grpc", "stream": "s" }, "bogus": 1 })).unwrap_err();
        assert!(err.contains("bogus"), "{err}");
    }

    #[test]
    fn lookup_handles_nested_paths() {
        let record = json!({ "a": { "b": [ { "c": 7 } ] } });
        assert_eq!(lookup(&record, "a.b.0.c"), Some(&json!(7)));
        assert_eq!(lookup(&record, "a.x"), None);
    }

    #[test]
    fn comparisons() {
        let n = json!(5);
        assert!(CompareOp::Gt.matches(Some(&n), &json!(4.5)));
        assert!(CompareOp::Lte.matches(Some(&n), &json!(5)));
        assert!(CompareOp::Eq.matches(Some(&json!("a")), &json!("a")));
        assert!(!CompareOp::Gt.matches(Some(&json!("5")), &json!(4)), "no string/number coercion");
        assert!(CompareOp::Ne.matches(None, &json!(1)));
        assert!(!CompareOp::Eq.matches(None, &json!(1)));
    }

    #[test]
    fn json_aggregates_skip_non_numeric_values() {
        let records = [json!({"v": 2}), json!({"v": "x"}), json!({}), json!({"v": 4})];
        let run = |agg: AggregateDef| {
            let mut acc = agg.create_accumulator();
            for r in &records {
                agg.add(&mut acc, r);
            }
            agg.result(acc)
        };
        let field = || "v".to_string();
        assert_eq!(run(AggregateDef::Count), json!(4));
        assert_eq!(run(AggregateDef::Sum { field: field() }), json!(6.0));
        assert_eq!(run(AggregateDef::Avg { field: field() }), json!(3.0));
        assert_eq!(run(AggregateDef::Min { field: field() }), json!(2.0));
        assert_eq!(run(AggregateDef::Max { field: field() }), json!(4.0));
        assert_eq!(run(AggregateDef::Max { field: "missing".into() }), Value::Null);
    }
}
