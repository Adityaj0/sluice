//! Logical plan → runtime pipeline.
//!
//! The (linear) logical plan becomes a chain of stages over JSON records:
//!
//! - `Scan` → one stage that decodes JSON, applies the pushed-down
//!   predicate and prunes fields, so filtered records never leave it;
//! - `WindowAggregate` → `key_by` on the GROUP BY values, then a window
//!   operator whose accumulator holds every aggregate of the query plus the
//!   group values; its output rows carry `$groups`, `$aggregates` and the
//!   window bounds for the expressions above;
//! - `Filter` / `Project` → filter and map stages evaluating expressions.

use std::fmt::Write;

use sluice_core::{Context, Event, FilterMapOperator, FilterOperator, KeyByOperator, MapOperator, Operator, OperatorError, Timestamp};
use sluice_runtime::DataStream;
use sluice_runtime::window::{
    AggregateFunction, SessionWindows, SlidingWindows, TumblingWindows, WindowResult, WindowSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::ast::{AggFunc, Expr, WindowClause, WindowKind};
use crate::expr::{AGGREGATES, GROUPS, RowScope, WINDOW_END, WINDOW_START, eval, is_true, prune};
use crate::logical::{AggCall, LogicalPlan, resolve};

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalStage {
    Scan {
        stream: String,
        predicate: Option<Expr>,
        fields: Option<Vec<Vec<String>>>,
    },
    Filter {
        predicate: Expr,
        /// The predicate as SQL, references resolved.
        display: String,
    },
    KeyBy {
        keys: Vec<Expr>,
    },
    WindowAggregate {
        window: WindowClause,
        group_by: Vec<Expr>,
        aggregates: Vec<AggCall>,
    },
    Project {
        columns: Vec<(Expr, String)>,
        display: String,
    },
}

/// The stages of a compiled query, source first.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalPlan {
    pub stream: String,
    pub stages: Vec<PhysicalStage>,
}

impl PhysicalPlan {
    pub fn from_logical(plan: &LogicalPlan) -> Self {
        let mut stages = Vec::new();
        let mut node = Some(plan);
        let mut stream = String::new();
        while let Some(current) = node {
            let ctx = current.input().and_then(LogicalPlan::agg_context);
            match current {
                LogicalPlan::Scan {
                    stream: name,
                    predicate,
                    fields,
                } => {
                    stream = name.clone();
                    stages.push(PhysicalStage::Scan {
                        stream: name.clone(),
                        predicate: predicate.clone(),
                        fields: fields.clone(),
                    });
                }
                LogicalPlan::Filter { predicate, .. } => stages.push(PhysicalStage::Filter {
                    predicate: predicate.clone(),
                    display: resolve(predicate, ctx).to_string(),
                }),
                LogicalPlan::Project { columns, .. } => stages.push(PhysicalStage::Project {
                    columns: columns.clone(),
                    display: current.detail(),
                }),
                LogicalPlan::WindowAggregate {
                    window,
                    group_by,
                    aggregates,
                    ..
                } => {
                    stages.push(PhysicalStage::WindowAggregate {
                        window: *window,
                        group_by: group_by.clone(),
                        aggregates: aggregates.clone(),
                    });
                    stages.push(PhysicalStage::KeyBy { keys: group_by.clone() });
                }
            }
            node = current.input();
        }
        stages.reverse();
        Self { stream, stages }
    }

    /// Stage names, as they appear in job metrics.
    pub fn stage_names(&self) -> Vec<String> {
        self.stages.iter().map(PhysicalStage::name).collect()
    }

    pub fn explain(&self) -> String {
        let mut out = String::new();
        for (i, name) in self.stage_names().into_iter().enumerate() {
            writeln!(out, "{}. {name}", i + 1).unwrap();
        }
        out
    }

    /// Appends the query's stages to a stream of raw (JSON-encoded) records.
    pub fn build(&self, input: DataStream<Vec<u8>>) -> DataStream<Value> {
        let mut stages = self.stages.iter();
        let Some(PhysicalStage::Scan { predicate, fields, .. }) = stages.next() else {
            unreachable!("a physical plan starts with its scan");
        };
        let (predicate, fields) = (predicate.clone(), fields.clone());
        let mut data = input.process(
            &self.stages[0].name(),
            FilterMapOperator::new(move |bytes: Vec<u8>| {
                let record: Value = serde_json::from_slice(&bytes).ok()?;
                if let Some(predicate) = &predicate {
                    if !is_true(&eval(predicate, &RowScope(&record))) {
                        return None;
                    }
                }
                Some(match &fields {
                    Some(fields) => prune(&record, fields),
                    None => record,
                })
            }),
        );

        for stage in stages {
            let name = stage.name();
            data = match stage.clone() {
                PhysicalStage::Scan { .. } => unreachable!("only one scan"),
                PhysicalStage::Filter { predicate, .. } => data.process(
                    &name,
                    FilterOperator::new(move |row: &Value| is_true(&eval(&predicate, &RowScope(row)))),
                ),
                PhysicalStage::KeyBy { keys } => data.process(
                    &name,
                    KeyByOperator::new(move |row: &Value| group_key(&keys.iter().map(|k| eval(k, &RowScope(row))).collect::<Vec<_>>())),
                ),
                PhysicalStage::WindowAggregate {
                    window,
                    group_by,
                    aggregates,
                } => {
                    let aggregate = SqlAggregate { group_by, aggregates };
                    let lateness = window.allowed_lateness_ms;
                    match window.kind {
                        WindowKind::Tumbling { size_ms } => {
                            window_stage(data, &name, TumblingWindows::of_millis(size_ms), aggregate, lateness)
                        }
                        WindowKind::Sliding { size_ms, slide_ms } => {
                            window_stage(data, &name, SlidingWindows::of_millis(size_ms, slide_ms), aggregate, lateness)
                        }
                        WindowKind::Session { gap_ms } => {
                            window_stage(data, &name, SessionWindows::with_gap_millis(gap_ms), aggregate, lateness)
                        }
                    }
                }
                PhysicalStage::Project { columns, .. } => data.process(
                    &name,
                    MapOperator::new(move |row: Value| {
                        let scope = RowScope(&row);
                        let mut out = Map::with_capacity(columns.len());
                        for (expr, name) in &columns {
                            out.insert(name.clone(), eval(expr, &scope));
                        }
                        Value::Object(out)
                    }),
                ),
            };
        }
        data
    }
}

fn join<T>(items: &[T], f: impl Fn(&T) -> String) -> String {
    items.iter().map(f).collect::<Vec<_>>().join(", ")
}

impl PhysicalStage {
    pub fn kind(&self) -> &'static str {
        match self {
            PhysicalStage::Scan { .. } => "scan",
            PhysicalStage::Filter { .. } => "filter",
            PhysicalStage::KeyBy { .. } => "key_by",
            PhysicalStage::WindowAggregate { .. } => "window",
            PhysicalStage::Project { .. } => "project",
        }
    }

    pub fn name(&self) -> String {
        match self {
            PhysicalStage::Scan {
                stream,
                predicate,
                fields,
            } => {
                let mut out = format!("scan({stream}");
                if let Some(predicate) = predicate {
                    write!(out, " where {predicate}").unwrap();
                }
                if let Some(fields) = fields {
                    write!(out, " fields [{}]", join(fields, |f| f.join("."))).unwrap();
                }
                out + ")"
            }
            PhysicalStage::Filter { display, .. } => format!("filter({display})"),
            PhysicalStage::KeyBy { keys } if keys.is_empty() => "key_by(global)".into(),
            PhysicalStage::KeyBy { keys } => format!("key_by({})", join(keys, Expr::to_string)),
            PhysicalStage::WindowAggregate { window, aggregates, .. } => {
                format!("window({window}: {})", join(aggregates, |a| a.as_expr().to_string()))
            }
            PhysicalStage::Project { display, .. } => format!("project({display})"),
        }
    }
}

/// Partition key for a group: a plain string key stays readable.
fn group_key(values: &[Value]) -> Vec<u8> {
    match values {
        [] => Vec::new(),
        [Value::String(s)] => s.as_bytes().to_vec(),
        [single] => single.to_string().into_bytes(),
        many => Value::Array(many.to_vec()).to_string().into_bytes(),
    }
}

fn window_stage<W: WindowSpec>(
    data: DataStream<Value>,
    name: &str,
    windows: W,
    aggregate: SqlAggregate,
    lateness: Timestamp,
) -> DataStream<Value> {
    data.process(name, RowOperator(windows.operator::<Value, SqlAggregate>(aggregate, lateness)))
}

/// A number kept as `f64` that remembers whether every input was an
/// integer, so `SUM` of integers stays an integer.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct Num {
    value: f64,
    fractional: bool,
}

impl Num {
    fn of(v: &Value) -> Option<Num> {
        Some(Num {
            value: v.as_f64()?,
            fractional: v.as_i64().is_none() && v.as_u64().is_none(),
        })
    }

    fn to_json(self) -> Value {
        if !self.fractional && self.value.abs() < 9e15 {
            json!(self.value as i64)
        } else {
            json!(self.value)
        }
    }
}

/// Accumulator state of one aggregate call.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum AggState {
    Count(u64),
    Sum(Option<Num>),
    Avg { sum: f64, n: u64 },
    Min(Option<Num>),
    Max(Option<Num>),
}

/// Accumulator of a whole SELECT: the group's values (kept as JSON text:
/// the binary state format cannot hold untyped JSON) and one state per
/// aggregate call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlAcc {
    groups: Option<String>,
    states: Vec<AggState>,
}

impl AggState {
    fn value(&self) -> Value {
        match *self {
            AggState::Count(n) => json!(n),
            AggState::Sum(sum) => sum.map_or(Value::Null, Num::to_json),
            AggState::Avg { n: 0, .. } => Value::Null,
            AggState::Avg { sum, n } => json!(sum / n as f64),
            AggState::Min(v) | AggState::Max(v) => v.map_or(Value::Null, Num::to_json),
        }
    }
}

impl SqlAcc {
    /// Group values and the current value of each aggregate.
    fn values(&self) -> (Value, Vec<Value>) {
        let groups = self
            .groups
            .as_deref()
            .and_then(|g| serde_json::from_str(g).ok())
            .unwrap_or_else(|| Value::Array(Vec::new()));
        (groups, self.states.iter().map(AggState::value).collect())
    }

    /// The accumulator as JSON, for state inspection:
    /// `{"groups": [...], "aggregates": [...]}`.
    pub fn describe(&self) -> Value {
        let (groups, aggregates) = self.values();
        json!({ "groups": groups, "aggregates": aggregates })
    }
}

/// Computes all aggregates of a query in one accumulator.
pub struct SqlAggregate {
    group_by: Vec<Expr>,
    aggregates: Vec<AggCall>,
}

fn combine(a: Option<Num>, b: Option<Num>, f: fn(f64, f64) -> f64) -> Option<Num> {
    match (a, b) {
        (Some(a), Some(b)) => Some(Num {
            value: f(a.value, b.value),
            fractional: a.fractional || b.fractional,
        }),
        (a, b) => a.or(b),
    }
}

fn pick(a: Option<Num>, b: Option<Num>, take_b: fn(f64, f64) -> bool) -> Option<Num> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if take_b(a.value, b.value) { b } else { a }),
        (a, b) => a.or(b),
    }
}

impl AggregateFunction<Value> for SqlAggregate {
    type Acc = SqlAcc;
    type Out = Value;

    fn create_accumulator(&self) -> SqlAcc {
        SqlAcc {
            groups: None,
            states: self
                .aggregates
                .iter()
                .map(|a| match a.func {
                    AggFunc::Count => AggState::Count(0),
                    AggFunc::Sum => AggState::Sum(None),
                    AggFunc::Avg => AggState::Avg { sum: 0.0, n: 0 },
                    AggFunc::Min => AggState::Min(None),
                    AggFunc::Max => AggState::Max(None),
                })
                .collect(),
        }
    }

    fn add(&self, acc: &mut SqlAcc, row: &Value) {
        let scope = RowScope(row);
        if acc.groups.is_none() {
            let groups: Vec<Value> = self.group_by.iter().map(|g| eval(g, &scope)).collect();
            acc.groups = Some(Value::Array(groups).to_string());
        }
        for (state, call) in acc.states.iter_mut().zip(&self.aggregates) {
            let value = call.arg.as_ref().map(|arg| eval(arg, &scope));
            match state {
                AggState::Count(n) => {
                    if value.as_ref().is_none_or(|v| !v.is_null()) {
                        *n += 1;
                    }
                }
                AggState::Sum(sum) => *sum = combine(*sum, value.as_ref().and_then(Num::of), |a, b| a + b),
                AggState::Avg { sum, n } => {
                    if let Some(x) = value.as_ref().and_then(Value::as_f64) {
                        *sum += x;
                        *n += 1;
                    }
                }
                AggState::Min(min) => *min = pick(*min, value.as_ref().and_then(Num::of), |a, b| b < a),
                AggState::Max(max) => *max = pick(*max, value.as_ref().and_then(Num::of), |a, b| b > a),
            }
        }
    }

    fn merge(&self, a: SqlAcc, b: SqlAcc) -> SqlAcc {
        SqlAcc {
            groups: a.groups.or(b.groups),
            states: a
                .states
                .into_iter()
                .zip(b.states)
                .map(|pair| match pair {
                    (AggState::Count(x), AggState::Count(y)) => AggState::Count(x + y),
                    (AggState::Sum(x), AggState::Sum(y)) => AggState::Sum(combine(x, y, |a, b| a + b)),
                    (AggState::Avg { sum: s1, n: n1 }, AggState::Avg { sum: s2, n: n2 }) => AggState::Avg {
                        sum: s1 + s2,
                        n: n1 + n2,
                    },
                    (AggState::Min(x), AggState::Min(y)) => AggState::Min(pick(x, y, |a, b| b < a)),
                    (AggState::Max(x), AggState::Max(y)) => AggState::Max(pick(x, y, |a, b| b > a)),
                    (x, _) => x,
                })
                .collect(),
        }
    }

    fn result(&self, acc: SqlAcc) -> Value {
        let (groups, aggregates) = acc.values();
        json!({ GROUPS: groups, AGGREGATES: aggregates })
    }
}

/// Turns window results into aggregated rows: the aggregate's
/// `$groups`/`$aggregates` plus the window bounds.
struct RowOperator<W>(W);

impl<W> Operator for RowOperator<W>
where
    W: Operator<In = Value, Out = WindowResult<Value>>,
{
    type In = Value;
    type Out = Value;

    fn state_names(&self) -> &'static [&'static str] {
        self.0.state_names()
    }

    fn process(&mut self, event: Event<Value>, ctx: &mut Context<'_, Value>) -> Result<(), OperatorError> {
        let mut results = Vec::new();
        self.0.process(event, &mut Context::new(&mut results, ctx.state()))?;
        emit_rows(results, ctx);
        Ok(())
    }

    fn on_watermark(&mut self, watermark: Timestamp, ctx: &mut Context<'_, Value>) -> Result<(), OperatorError> {
        let mut results = Vec::new();
        self.0.on_watermark(watermark, &mut Context::new(&mut results, ctx.state()))?;
        emit_rows(results, ctx);
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>, OperatorError> {
        self.0.snapshot_state()
    }

    fn restore_state(&mut self, state: &[u8]) -> Result<(), OperatorError> {
        self.0.restore_state(state)
    }

    fn late_records(&self) -> u64 {
        self.0.late_records()
    }
}

fn emit_rows(results: Vec<Event<WindowResult<Value>>>, ctx: &mut Context<'_, Value>) {
    for event in results {
        ctx.emit(event.map_value(|result| {
            let mut row = match result.value {
                Value::Object(map) => map,
                _ => Map::new(),
            };
            row.insert(WINDOW_START.into(), json!(result.window.start));
            row.insert(WINDOW_END.into(), json!(result.window.end));
            Value::Object(row)
        }));
    }
}
