//! Expression evaluation over JSON records.
//!
//! SQL semantics: NULL propagates through operators and comparisons, AND/OR
//! use three-valued logic, and a filter keeps a row only when its predicate
//! is TRUE. Integer arithmetic stays integral (falling back to floating
//! point on overflow); division is always floating point.

use std::cmp::Ordering;

use serde_json::{Map, Number, Value};

use crate::ast::{BinaryOp, Expr, UnaryOp};

/// What an expression can refer to.
pub trait Scope {
    fn column(&self, path: &[String]) -> Value;
    fn group(&self, index: usize) -> Value;
    fn aggregate(&self, index: usize) -> Value;
    fn window_start(&self) -> Value;
    fn window_end(&self) -> Value;
}

/// Field names of an aggregated row (see [`RowScope`]).
pub const GROUPS: &str = "$groups";
pub const AGGREGATES: &str = "$aggregates";
pub const WINDOW_START: &str = "$window_start";
pub const WINDOW_END: &str = "$window_end";

/// A record, or an aggregated row carrying its group values, aggregate
/// values and window bounds under the reserved `$` fields.
pub struct RowScope<'a>(pub &'a Value);

impl Scope for RowScope<'_> {
    fn column(&self, path: &[String]) -> Value {
        lookup(self.0, path).cloned().unwrap_or(Value::Null)
    }
    fn group(&self, index: usize) -> Value {
        self.0.get(GROUPS).and_then(|g| g.get(index)).cloned().unwrap_or(Value::Null)
    }
    fn aggregate(&self, index: usize) -> Value {
        self.0.get(AGGREGATES).and_then(|a| a.get(index)).cloned().unwrap_or(Value::Null)
    }
    fn window_start(&self) -> Value {
        self.0.get(WINDOW_START).cloned().unwrap_or(Value::Null)
    }
    fn window_end(&self) -> Value {
        self.0.get(WINDOW_END).cloned().unwrap_or(Value::Null)
    }
}

/// Nothing in scope: for constant folding.
pub struct EmptyScope;

impl Scope for EmptyScope {
    fn column(&self, _: &[String]) -> Value {
        Value::Null
    }
    fn group(&self, _: usize) -> Value {
        Value::Null
    }
    fn aggregate(&self, _: usize) -> Value {
        Value::Null
    }
    fn window_start(&self) -> Value {
        Value::Null
    }
    fn window_end(&self) -> Value {
        Value::Null
    }
}

/// Follows a field path; array elements by index.
pub fn lookup<'a>(record: &'a Value, path: &[String]) -> Option<&'a Value> {
    path.iter().try_fold(record, |current, segment| match current {
        Value::Array(items) => segment.parse::<usize>().ok().and_then(|i| items.get(i)),
        _ => current.get(segment),
    })
}

/// A copy of `record` holding only the given field paths (nested objects
/// are rebuilt as needed).
pub fn prune(record: &Value, paths: &[Vec<String>]) -> Value {
    let mut out = Value::Object(Map::new());
    for path in paths {
        let Some(value) = lookup(record, path) else { continue };
        let mut target = &mut out;
        for (i, segment) in path.iter().enumerate() {
            let Value::Object(map) = target else { break };
            if i + 1 == path.len() {
                map.insert(segment.clone(), value.clone());
                break;
            }
            target = map.entry(segment.clone()).or_insert_with(|| Value::Object(Map::new()));
        }
    }
    out
}

/// `Some(bool)` for booleans, `None` (unknown) for NULL and anything else.
pub fn truth(value: &Value) -> Option<bool> {
    value.as_bool()
}

/// Whether a filter keeps the row: only TRUE passes.
pub fn is_true(value: &Value) -> bool {
    value == &Value::Bool(true)
}

pub fn eval(expr: &Expr, scope: &dyn Scope) -> Value {
    match expr {
        Expr::Literal(value) => value.clone(),
        Expr::Column(path) => scope.column(path),
        Expr::WindowStart => scope.window_start(),
        Expr::WindowEnd => scope.window_end(),
        Expr::GroupRef(i) => scope.group(*i),
        Expr::AggRef(i) => scope.aggregate(*i),
        // The planner replaces aggregates with AggRefs before evaluation.
        Expr::Aggregate { .. } => Value::Null,
        Expr::Unary { op, expr } => {
            let value = eval(expr, scope);
            match op {
                UnaryOp::Not => truth(&value).map_or(Value::Null, |b| Value::Bool(!b)),
                UnaryOp::Neg => negate(&value),
                UnaryOp::IsNull => Value::Bool(value.is_null()),
                UnaryOp::IsNotNull => Value::Bool(!value.is_null()),
            }
        }
        Expr::Binary { op, left, right } => match op {
            BinaryOp::And => {
                let l = truth(&eval(left, scope));
                if l == Some(false) {
                    return Value::Bool(false);
                }
                match (l, truth(&eval(right, scope))) {
                    (_, Some(false)) => Value::Bool(false),
                    (Some(true), Some(true)) => Value::Bool(true),
                    _ => Value::Null,
                }
            }
            BinaryOp::Or => {
                let l = truth(&eval(left, scope));
                if l == Some(true) {
                    return Value::Bool(true);
                }
                match (l, truth(&eval(right, scope))) {
                    (_, Some(true)) => Value::Bool(true),
                    (Some(false), Some(false)) => Value::Bool(false),
                    _ => Value::Null,
                }
            }
            _ => binary(*op, &eval(left, scope), &eval(right, scope)),
        },
        Expr::Function { name, args } => {
            let args: Vec<Value> = args.iter().map(|a| eval(a, scope)).collect();
            function(name, &args)
        }
    }
}

fn negate(value: &Value) -> Value {
    if let Some(i) = value.as_i64() {
        if let Some(n) = i.checked_neg() {
            return Value::from(n);
        }
    }
    value.as_f64().map_or(Value::Null, |f| float(-f))
}

fn float(f: f64) -> Value {
    Number::from_f64(f).map_or(Value::Null, Value::Number)
}

fn compare(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => match (a.as_i64(), b.as_i64()) {
            (Some(a), Some(b)) => Some(a.cmp(&b)),
            _ => a.as_f64()?.partial_cmp(&b.as_f64()?),
        },
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn binary(op: BinaryOp, left: &Value, right: &Value) -> Value {
    if left.is_null() || right.is_null() {
        return Value::Null;
    }
    match op {
        BinaryOp::Eq | BinaryOp::NotEq => {
            let equal = match compare(left, right) {
                Some(ordering) => ordering == Ordering::Equal,
                None => left == right,
            };
            Value::Bool(equal == (op == BinaryOp::Eq))
        }
        BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => match compare(left, right) {
            Some(ordering) => Value::Bool(match op {
                BinaryOp::Lt => ordering == Ordering::Less,
                BinaryOp::LtEq => ordering != Ordering::Greater,
                BinaryOp::Gt => ordering == Ordering::Greater,
                _ => ordering != Ordering::Less,
            }),
            None => Value::Null,
        },
        _ => arithmetic(op, left, right),
    }
}

fn arithmetic(op: BinaryOp, left: &Value, right: &Value) -> Value {
    if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
        let exact = match op {
            BinaryOp::Plus => a.checked_add(b),
            BinaryOp::Minus => a.checked_sub(b),
            BinaryOp::Multiply => a.checked_mul(b),
            BinaryOp::Modulo if b == 0 => return Value::Null,
            BinaryOp::Modulo => a.checked_rem(b),
            _ => None,
        };
        if let Some(n) = exact {
            return Value::from(n);
        }
    }
    let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) else {
        return Value::Null;
    };
    match op {
        BinaryOp::Plus => float(a + b),
        BinaryOp::Minus => float(a - b),
        BinaryOp::Multiply => float(a * b),
        BinaryOp::Divide if b == 0.0 => Value::Null,
        BinaryOp::Divide => float(a / b),
        BinaryOp::Modulo if b == 0.0 => Value::Null,
        BinaryOp::Modulo => float(a % b),
        _ => Value::Null,
    }
}

fn function(name: &str, args: &[Value]) -> Value {
    let first = args.first().unwrap_or(&Value::Null);
    match name {
        "LOWER" => first.as_str().map_or(Value::Null, |s| Value::String(s.to_lowercase())),
        "UPPER" => first.as_str().map_or(Value::Null, |s| Value::String(s.to_uppercase())),
        "LENGTH" => first.as_str().map_or(Value::Null, |s| Value::from(s.chars().count())),
        "ABS" => match first.as_i64() {
            Some(i) => i.checked_abs().map_or(Value::Null, Value::from),
            None => first.as_f64().map_or(Value::Null, |f| float(f.abs())),
        },
        "ROUND" => {
            let Some(x) = first.as_f64() else { return Value::Null };
            let digits = args.get(1).and_then(Value::as_i64).unwrap_or(0).clamp(-15, 15) as i32;
            let scale = 10f64.powi(digits);
            let rounded = (x * scale).round() / scale;
            if digits <= 0 && rounded.abs() < 9e15 {
                Value::from(rounded as i64)
            } else {
                float(rounded)
            }
        }
        "COALESCE" => args.iter().find(|v| !v.is_null()).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::parser::parse;
    use crate::ast::SelectItem;

    fn eval_sql(expr: &str, record: Value) -> Value {
        let query = parse(&format!("SELECT {expr} FROM t")).unwrap();
        let SelectItem::Expr { expr, .. } = &query.select[0] else { panic!() };
        eval(expr, &RowScope(&record))
    }

    #[test]
    fn arithmetic_keeps_integers_integral() {
        let r = json!({ "a": 7, "b": 2, "f": 1.5 });
        assert_eq!(eval_sql("a + b * 3", r.clone()), json!(13));
        assert_eq!(eval_sql("a / b", r.clone()), json!(3.5));
        assert_eq!(eval_sql("a % b", r.clone()), json!(1));
        assert_eq!(eval_sql("a + f", r.clone()), json!(8.5));
        assert_eq!(eval_sql("-a", r.clone()), json!(-7));
        assert_eq!(eval_sql("a / 0", r.clone()), Value::Null);
        assert_eq!(eval_sql("a + missing", r.clone()), Value::Null);
        assert_eq!(eval_sql("9223372036854775807 + 1", r), json!(9223372036854775808.0));
    }

    #[test]
    fn three_valued_logic() {
        let r = json!({ "t": true, "f": false });
        assert_eq!(eval_sql("t AND NULL", r.clone()), Value::Null);
        assert_eq!(eval_sql("f AND NULL", r.clone()), json!(false));
        assert_eq!(eval_sql("t OR NULL", r.clone()), json!(true));
        assert_eq!(eval_sql("f OR NULL", r.clone()), Value::Null);
        assert_eq!(eval_sql("NOT NULL", r.clone()), Value::Null);
        assert_eq!(eval_sql("missing = 1", r.clone()), Value::Null);
        assert_eq!(eval_sql("missing IS NULL", r.clone()), json!(true));
        assert!(!is_true(&eval_sql("missing > 1", r)));
    }

    #[test]
    fn comparisons_and_functions() {
        let r = json!({ "n": 10, "x": 2.5, "s": "Ann", "o": { "p": [1, { "q": "deep" }] } });
        assert_eq!(eval_sql("n > x", r.clone()), json!(true));
        assert_eq!(eval_sql("n = 10.0", r.clone()), json!(true));
        assert_eq!(eval_sql("s < 'Bob'", r.clone()), json!(true));
        assert_eq!(eval_sql("s = 10", r.clone()), json!(false));
        assert_eq!(eval_sql("o.p.1.q", r.clone()), json!("deep"));
        assert_eq!(eval_sql("LOWER(s)", r.clone()), json!("ann"));
        assert_eq!(eval_sql("LENGTH(s)", r.clone()), json!(3));
        assert_eq!(eval_sql("ROUND(x)", r.clone()), json!(3));
        assert_eq!(eval_sql("ROUND(x * 1.111, 2)", r.clone()), json!(2.78));
        assert_eq!(eval_sql("ABS(0 - n)", r.clone()), json!(10));
        assert_eq!(eval_sql("COALESCE(missing, s)", r), json!("Ann"));
    }

    #[test]
    fn prune_keeps_only_requested_paths() {
        let r = json!({ "a": 1, "b": 2, "o": { "x": 1, "y": 2 } });
        let paths = vec![vec!["a".to_string()], vec!["o".to_string(), "y".to_string()], vec!["zz".to_string()]];
        assert_eq!(prune(&r, &paths), json!({ "a": 1, "o": { "y": 2 } }));
    }
}
