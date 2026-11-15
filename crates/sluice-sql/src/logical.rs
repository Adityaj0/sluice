//! AST → logical plan.
//!
//! ```text
//! Project            SELECT list (over aggregated rows if windowed)
//!   Filter           HAVING
//!     WindowAggregate  WINDOW + GROUP BY + the query's aggregate calls
//!       Filter       WHERE
//!         Scan       FROM
//! ```
//!
//! Above a `WindowAggregate`, expressions no longer see input columns: the
//! planner rewrites GROUP BY expressions to [`Expr::GroupRef`] and aggregate
//! calls to [`Expr::AggRef`], and rejects any column left over (it is
//! neither grouped nor aggregated).

use std::collections::BTreeSet;
use std::fmt::Write;

use serde_json::{Value, json};

use crate::SqlError;
use crate::ast::{AggFunc, Expr, Query, SelectItem, WindowClause};

/// One aggregate computed per window and group: `COUNT(*)` has no argument.
#[derive(Debug, Clone, PartialEq)]
pub struct AggCall {
    pub func: AggFunc,
    pub arg: Option<Expr>,
}

impl AggCall {
    pub fn as_expr(&self) -> Expr {
        Expr::Aggregate {
            func: self.func,
            arg: self.arg.clone().map(Box::new),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Reads a stream. The optimizer may push a predicate into the scan and
    /// prune it to the fields the rest of the plan reads.
    Scan {
        stream: String,
        predicate: Option<Expr>,
        fields: Option<Vec<Vec<String>>>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<(Expr, String)>,
    },
    WindowAggregate {
        input: Box<LogicalPlan>,
        window: WindowClause,
        group_by: Vec<Expr>,
        aggregates: Vec<AggCall>,
    },
}

fn plan_error(message: impl Into<String>) -> SqlError {
    SqlError::Plan {
        message: message.into(),
    }
}

/// Default output column name for a SELECT expression.
fn output_name(expr: &Expr, index: usize) -> String {
    match expr {
        Expr::Column(path) => path.last().cloned().unwrap_or_default(),
        Expr::WindowStart => "window_start".into(),
        Expr::WindowEnd => "window_end".into(),
        Expr::Aggregate { func, arg } => {
            let func = func.name().to_ascii_lowercase();
            match arg.as_deref() {
                None => func,
                Some(Expr::Column(path)) => format!("{func}_{}", path.join("_")),
                Some(_) => format!("{func}_{index}"),
            }
        }
        Expr::Function { name, args } => match args.first() {
            Some(Expr::Column(path)) => format!("{}_{}", name.to_ascii_lowercase(), path.join("_")),
            _ => format!("{}_{index}", name.to_ascii_lowercase()),
        },
        _ => format!("expr_{index}"),
    }
}

/// Replaces aggregate calls with references into `aggregates` (adding new
/// ones), checking they are not nested.
fn extract_aggregates(expr: Expr, aggregates: &mut Vec<AggCall>) -> Result<Expr, SqlError> {
    let mut error = None;
    let rewritten = expr.rewrite(&mut |e| {
        let Expr::Aggregate { func, arg } = e else { return None };
        let arg = arg.as_deref().cloned();
        if let Some(arg) = &arg {
            if arg.contains_aggregate() {
                error.get_or_insert_with(|| plan_error(format!("aggregate functions cannot be nested: {e}")));
            }
            if arg.contains_window_bound() {
                error.get_or_insert_with(|| plan_error(format!("WINDOW_START/WINDOW_END cannot be aggregated: {e}")));
            }
        }
        let call = AggCall { func: *func, arg };
        let index = match aggregates.iter().position(|a| *a == call) {
            Some(i) => i,
            None => {
                aggregates.push(call);
                aggregates.len() - 1
            }
        };
        Some(Expr::AggRef(index))
    });
    match error {
        Some(error) => Err(error),
        None => Ok(rewritten),
    }
}

/// Replaces GROUP BY expressions with group references.
fn substitute_groups(expr: Expr, group_by: &[Expr]) -> Expr {
    expr.rewrite(&mut |e| group_by.iter().position(|g| g == e).map(Expr::GroupRef))
}

/// Rewrites a SELECT or HAVING expression of an aggregating query.
fn post_aggregate(expr: Expr, group_by: &[Expr], aggregates: &mut Vec<AggCall>, clause: &str) -> Result<Expr, SqlError> {
    let expr = substitute_groups(extract_aggregates(expr, aggregates)?, group_by);
    let mut leftover = BTreeSet::new();
    expr.columns(&mut leftover);
    if let Some(column) = leftover.into_iter().next() {
        return Err(plan_error(format!(
            "column `{}` in {clause} must appear in GROUP BY or be used in an aggregate function",
            column.join(".")
        )));
    }
    Ok(expr)
}

pub fn plan(query: &Query) -> Result<LogicalPlan, SqlError> {
    let mut plan = LogicalPlan::Scan {
        stream: query.from.clone(),
        predicate: None,
        fields: None,
    };

    if let Some(selection) = &query.selection {
        if selection.contains_aggregate() {
            return Err(plan_error("aggregate functions are not allowed in WHERE; use HAVING"));
        }
        if selection.contains_window_bound() {
            return Err(plan_error("WINDOW_START/WINDOW_END are only available after aggregation"));
        }
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: selection.clone(),
        };
    }

    let wildcard = query.select.iter().any(|item| matches!(item, SelectItem::Wildcard));
    if wildcard && query.select.len() > 1 {
        return Err(plan_error("SELECT * cannot be combined with other columns"));
    }

    let aggregating = query.window.is_some()
        || !query.group_by.is_empty()
        || query.having.is_some()
        || query.select.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => expr.contains_aggregate(),
            SelectItem::Wildcard => false,
        });

    let mut columns = Vec::new();
    if aggregating {
        let Some(window) = query.window else {
            return Err(plan_error(
                "aggregations over a stream need a WINDOW clause (TUMBLING, SLIDING or SESSION)",
            ));
        };
        if wildcard {
            return Err(plan_error("SELECT * is not allowed in an aggregating query"));
        }
        for expr in &query.group_by {
            if expr.contains_aggregate() || expr.contains_window_bound() {
                return Err(plan_error(format!("GROUP BY cannot contain `{expr}`")));
            }
        }

        let mut aggregates = Vec::new();
        for (i, item) in query.select.iter().enumerate() {
            let SelectItem::Expr { expr, alias } = item else { unreachable!("wildcard rejected") };
            let name = alias.clone().unwrap_or_else(|| output_name(expr, i));
            columns.push((post_aggregate(expr.clone(), &query.group_by, &mut aggregates, "SELECT")?, name));
        }
        let having = query
            .having
            .clone()
            .map(|h| post_aggregate(h, &query.group_by, &mut aggregates, "HAVING"))
            .transpose()?;
        if query.group_by.is_empty() && aggregates.is_empty() {
            return Err(plan_error("a WINDOW clause needs GROUP BY or aggregate functions"));
        }

        plan = LogicalPlan::WindowAggregate {
            input: Box::new(plan),
            window,
            group_by: query.group_by.clone(),
            aggregates,
        };
        if let Some(predicate) = having {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }
    } else if !wildcard {
        for (i, item) in query.select.iter().enumerate() {
            let SelectItem::Expr { expr, alias } = item else { unreachable!() };
            if expr.contains_window_bound() {
                return Err(plan_error("WINDOW_START/WINDOW_END are only available in windowed queries"));
            }
            let name = alias.clone().unwrap_or_else(|| output_name(expr, i));
            columns.push((expr.clone(), name));
        }
    }

    if !columns.is_empty() {
        let mut seen = BTreeSet::new();
        for (_, name) in &columns {
            if !seen.insert(name.as_str()) {
                return Err(plan_error(format!("duplicate output column `{name}`; add an alias with AS")));
            }
        }
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            columns,
        };
    }
    Ok(plan)
}

/// GROUP BY expressions and aggregates visible to expressions above a
/// window aggregate, for showing references as the SQL they stand for.
pub(crate) type AggContext<'a> = Option<(&'a [Expr], &'a [AggCall])>;

pub(crate) fn resolve(expr: &Expr, ctx: AggContext<'_>) -> Expr {
    let Some((groups, aggregates)) = ctx else { return expr.clone() };
    expr.clone().rewrite(&mut |e| match e {
        Expr::GroupRef(i) => groups.get(*i).cloned(),
        Expr::AggRef(i) => aggregates.get(*i).map(AggCall::as_expr),
        _ => None,
    })
}

fn join<T>(items: &[T], f: impl Fn(&T) -> String) -> String {
    items.iter().map(f).collect::<Vec<_>>().join(", ")
}

impl LogicalPlan {
    pub fn input(&self) -> Option<&LogicalPlan> {
        match self {
            LogicalPlan::Scan { .. } => None,
            LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } | LogicalPlan::WindowAggregate { input, .. } => {
                Some(input)
            }
        }
    }

    /// The nearest aggregation at or below this node.
    pub(crate) fn agg_context(&self) -> AggContext<'_> {
        match self {
            LogicalPlan::WindowAggregate {
                group_by, aggregates, ..
            } => Some((group_by, aggregates)),
            other => other.input().and_then(LogicalPlan::agg_context),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            LogicalPlan::Scan { .. } => "Scan",
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Project { .. } => "Project",
            LogicalPlan::WindowAggregate { .. } => "WindowAggregate",
        }
    }

    /// One-line description of this node.
    pub fn detail(&self) -> String {
        let ctx = self.input().and_then(LogicalPlan::agg_context);
        match self {
            LogicalPlan::Scan {
                stream,
                predicate,
                fields,
            } => {
                let mut out = stream.clone();
                if let Some(predicate) = predicate {
                    write!(out, " where {predicate}").unwrap();
                }
                if let Some(fields) = fields {
                    write!(out, " fields [{}]", join(fields, |f| f.join("."))).unwrap();
                }
                out
            }
            LogicalPlan::Filter { predicate, .. } => resolve(predicate, ctx).to_string(),
            LogicalPlan::Project { columns, .. } => join(columns, |(expr, name)| {
                let expr = resolve(expr, ctx).to_string();
                if expr.eq_ignore_ascii_case(name) { expr } else { format!("{expr} AS {name}") }
            }),
            LogicalPlan::WindowAggregate {
                window,
                group_by,
                aggregates,
                ..
            } => format!(
                "{window} group by [{}] aggregates [{}]",
                join(group_by, Expr::to_string),
                join(aggregates, |a| a.as_expr().to_string())
            ),
        }
    }

    /// Indented tree, root first.
    pub fn explain(&self) -> String {
        let mut out = String::new();
        let mut node = Some(self);
        let mut depth = 0;
        while let Some(current) = node {
            writeln!(out, "{}{}: {}", "  ".repeat(depth), current.name(), current.detail()).unwrap();
            node = current.input();
            depth += 1;
        }
        out
    }

    /// The tree as JSON (`{"node", "detail", "input"}`), for UIs.
    pub fn to_json(&self) -> Value {
        json!({
            "node": self.name(),
            "detail": self.detail(),
            "input": self.input().map(LogicalPlan::to_json),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    fn plan_sql(sql: &str) -> Result<LogicalPlan, SqlError> {
        plan(&parse(sql).unwrap())
    }

    #[test]
    fn plans_the_plan_example() {
        let plan = plan_sql(
            "SELECT user_id, COUNT(*) AS tx_count, SUM(amount) AS total FROM transactions
             WHERE amount > 0 WINDOW TUMBLING (SIZE 5 MINUTES) GROUP BY user_id HAVING COUNT(*) > 10",
        )
        .unwrap();
        assert_eq!(
            plan.explain(),
            "Project: user_id, COUNT(*) AS tx_count, SUM(amount) AS total\n  \
             Filter: COUNT(*) > 10\n    \
             WindowAggregate: TUMBLING(SIZE 5 MINUTES) group by [user_id] aggregates [COUNT(*), SUM(amount)]\n      \
             Filter: amount > 0\n        \
             Scan: transactions\n"
        );
        let LogicalPlan::Project { columns, .. } = &plan else { panic!() };
        assert_eq!(columns[1].0, Expr::AggRef(0), "COUNT(*) in SELECT and HAVING share one aggregate");
    }

    #[test]
    fn stateless_queries_project_and_filter() {
        let plan = plan_sql("SELECT user_id, amount * 2 AS doubled, LOWER(item) FROM t WHERE amount > 1").unwrap();
        assert_eq!(
            plan.explain(),
            "Project: user_id, amount * 2 AS doubled, LOWER(item) AS lower_item\n  Filter: amount > 1\n    Scan: t\n"
        );
        assert_eq!(plan_sql("SELECT * FROM t").unwrap().explain(), "Scan: t\n");
    }

    #[test]
    fn rejects_invalid_queries() {
        let err = |sql: &str| plan_sql(sql).unwrap_err().to_string();
        assert!(err("SELECT user_id, COUNT(*) FROM t GROUP BY user_id").contains("need a WINDOW"));
        assert!(err("SELECT user_id, amount FROM t WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY user_id")
            .contains("column `amount` in SELECT must appear in GROUP BY"));
        assert!(err("SELECT a FROM t WHERE COUNT(*) > 1").contains("not allowed in WHERE"));
        assert!(err("SELECT SUM(COUNT(*)) FROM t WINDOW TUMBLING (SIZE 1 MINUTE)").contains("cannot be nested"));
        assert!(err("SELECT * FROM t WINDOW TUMBLING (SIZE 1 MINUTE)").contains("SELECT * is not allowed"));
        assert!(err("SELECT a, a FROM t").contains("duplicate output column `a`"));
        assert!(err("SELECT WINDOW_START FROM t").contains("only available in windowed queries"));
        assert!(err("SELECT a FROM t WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY a HAVING b > 1")
            .contains("column `b` in HAVING"));
    }

    #[test]
    fn window_bounds_and_group_expressions_resolve() {
        let plan = plan_sql(
            "SELECT LOWER(user_id) AS u, WINDOW_START, MAX(amount) - MIN(amount) AS spread
             FROM t WINDOW SLIDING (SIZE 1 MINUTE, SLIDE 10 SECONDS) GROUP BY LOWER(user_id)",
        )
        .unwrap();
        let LogicalPlan::Project { columns, .. } = &plan else { panic!() };
        assert_eq!(columns[0].0, Expr::GroupRef(0));
        assert_eq!(columns[1], (Expr::WindowStart, "window_start".into()));
        assert_eq!(columns[2].0.to_string(), "#agg0 - #agg1");
        assert!(plan.explain().starts_with("Project: LOWER(user_id) AS u, WINDOW_START, MAX(amount) - MIN(amount) AS spread"));
    }
}
