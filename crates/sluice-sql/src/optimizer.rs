//! Rule-based logical optimizer.
//!
//! 1. **Constant folding**: input-independent sub-expressions are evaluated
//!    once at planning time (`amount > 2 * 5` → `amount > 10`), and boolean
//!    identities are simplified (`x AND TRUE` → `x`). A filter that folds to
//!    TRUE disappears.
//! 2. **Predicate pushdown**: HAVING conjuncts that only test grouping
//!    columns move below the window aggregation (filtering records before
//!    they create state instead of filtering results after); adjacent
//!    filters merge; and a filter directly above the scan moves into it, so
//!    rejected records never leave the source stage.
//! 3. **Projection pruning**: the scan keeps only the fields the rest of
//!    the plan reads, shrinking every record that flows downstream.
//!
//! Each rule application is recorded for EXPLAIN.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::ast::{BinaryOp, Expr, UnaryOp};
use crate::expr::{EmptyScope, eval};
use crate::logical::{AggCall, LogicalPlan, resolve};

pub struct Optimized {
    pub plan: LogicalPlan,
    /// What the optimizer did, in order.
    pub applied: Vec<String>,
}

pub fn optimize(plan: LogicalPlan) -> Optimized {
    let mut applied = Vec::new();
    let plan = fold_plan(plan, &mut applied);
    let plan = push_having_below_aggregation(plan, &mut applied);
    let plan = merge_filters(plan);
    let plan = push_into_scan(plan, &mut applied);
    let plan = prune_scan(plan, &mut applied);
    Optimized { plan, applied }
}

fn is_literal(expr: &Expr, value: &Value) -> bool {
    matches!(expr, Expr::Literal(v) if v == value)
}

/// Folds constant sub-expressions (largest first) and simplifies boolean
/// identities.
pub fn fold(expr: Expr, applied: &mut Vec<String>) -> Expr {
    let expr = expr.rewrite(&mut |e| {
        if matches!(e, Expr::Literal(_)) || !e.is_constant() {
            return None;
        }
        let value = eval(e, &EmptyScope);
        let folded = Expr::Literal(value);
        applied.push(format!("constant folding: `{e}` → `{folded}`"));
        Some(folded)
    });
    simplify(expr, applied)
}

fn simplify(expr: Expr, applied: &mut Vec<String>) -> Expr {
    let (t, f) = (Value::Bool(true), Value::Bool(false));
    match expr {
        Expr::Binary { op, left, right } if matches!(op, BinaryOp::And | BinaryOp::Or) => {
            let left = simplify(*left, applied);
            let right = simplify(*right, applied);
            let (identity, absorbing) = if op == BinaryOp::And { (&t, &f) } else { (&f, &t) };
            let original = Expr::binary(op, left.clone(), right.clone());
            let simplified = if is_literal(&left, absorbing) || is_literal(&right, absorbing) {
                Expr::Literal(absorbing.clone())
            } else if is_literal(&left, identity) {
                right
            } else if is_literal(&right, identity) {
                left
            } else {
                return original;
            };
            applied.push(format!("simplified `{original}` → `{simplified}`"));
            simplified
        }
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(simplify(*expr, applied)),
        },
        other => other,
    }
}

fn fold_plan(plan: LogicalPlan, applied: &mut Vec<String>) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { .. } => plan,
        LogicalPlan::Filter { input, predicate } => {
            let input = fold_plan(*input, applied);
            let predicate = fold(predicate, applied);
            if is_literal(&predicate, &Value::Bool(true)) {
                applied.push("removed a filter that is always true".into());
                input
            } else {
                if matches!(predicate, Expr::Literal(_)) {
                    applied.push(format!("warning: filter `{predicate}` never passes: the query produces nothing"));
                }
                LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate,
                }
            }
        }
        LogicalPlan::Project { input, columns } => LogicalPlan::Project {
            input: Box::new(fold_plan(*input, applied)),
            columns: columns.into_iter().map(|(e, name)| (fold(e, applied), name)).collect(),
        },
        LogicalPlan::WindowAggregate {
            input,
            window,
            group_by,
            aggregates,
        } => LogicalPlan::WindowAggregate {
            input: Box::new(fold_plan(*input, applied)),
            window,
            group_by: group_by.into_iter().map(|e| fold(e, applied)).collect(),
            aggregates: aggregates
                .into_iter()
                .map(|a| AggCall {
                    func: a.func,
                    arg: a.arg.map(|e| fold(e, applied)),
                })
                .collect(),
        },
    }
}

fn with_input(plan: LogicalPlan, f: &mut impl FnMut(LogicalPlan) -> LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { .. } => plan,
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(f(*input)),
            predicate,
        },
        LogicalPlan::Project { input, columns } => LogicalPlan::Project {
            input: Box::new(f(*input)),
            columns,
        },
        LogicalPlan::WindowAggregate {
            input,
            window,
            group_by,
            aggregates,
        } => LogicalPlan::WindowAggregate {
            input: Box::new(f(*input)),
            window,
            group_by,
            aggregates,
        },
    }
}

/// `HAVING user_id = 'a' AND COUNT(*) > 10` over `GROUP BY user_id`: the
/// first conjunct only depends on the group, so it can filter records
/// before aggregation (`WHERE user_id = 'a'`) with the same result.
fn push_having_below_aggregation(plan: LogicalPlan, applied: &mut Vec<String>) -> LogicalPlan {
    let plan = with_input(plan, &mut |input| push_having_below_aggregation(input, applied));
    let LogicalPlan::Filter { input, predicate } = plan else { return plan };
    let LogicalPlan::WindowAggregate {
        input: agg_input,
        window,
        group_by,
        aggregates,
    } = *input
    else {
        return LogicalPlan::Filter { input, predicate };
    };

    let (pushable, kept): (Vec<_>, Vec<_>) = predicate.conjuncts().into_iter().partition(|c| {
        !c.any(&|e| matches!(e, Expr::AggRef(_) | Expr::WindowStart | Expr::WindowEnd))
    });
    let ctx = Some((group_by.as_slice(), aggregates.as_slice()));
    let mut agg_input = *agg_input;
    if let Some(pushed) = Expr::and_all(pushable.iter().map(|c| resolve(c, ctx))) {
        applied.push(format!("predicate pushdown: `{pushed}` moved from HAVING below the window aggregation"));
        agg_input = LogicalPlan::Filter {
            input: Box::new(agg_input),
            predicate: pushed,
        };
    }
    let aggregate = LogicalPlan::WindowAggregate {
        input: Box::new(agg_input),
        window,
        group_by,
        aggregates,
    };
    match Expr::and_all(kept) {
        Some(predicate) => LogicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        },
        None => aggregate,
    }
}

fn merge_filters(plan: LogicalPlan) -> LogicalPlan {
    let plan = with_input(plan, &mut merge_filters);
    match plan {
        LogicalPlan::Filter { input, predicate } => match *input {
            LogicalPlan::Filter {
                input: inner,
                predicate: inner_predicate,
            } => LogicalPlan::Filter {
                input: inner,
                predicate: Expr::binary(BinaryOp::And, inner_predicate, predicate),
            },
            other => LogicalPlan::Filter {
                input: Box::new(other),
                predicate,
            },
        },
        other => other,
    }
}

fn push_into_scan(plan: LogicalPlan, applied: &mut Vec<String>) -> LogicalPlan {
    let plan = with_input(plan, &mut |input| push_into_scan(input, applied));
    match plan {
        LogicalPlan::Filter { input, predicate } => match *input {
            LogicalPlan::Scan {
                stream,
                predicate: existing,
                fields,
            } => {
                applied.push(format!("predicate pushdown: `{predicate}` evaluated inside the scan of `{stream}`"));
                let predicate = match existing {
                    Some(existing) => Expr::binary(BinaryOp::And, existing, predicate),
                    None => predicate,
                };
                LogicalPlan::Scan {
                    stream,
                    predicate: Some(predicate),
                    fields,
                }
            }
            other => LogicalPlan::Filter {
                input: Box::new(other),
                predicate,
            },
        },
        other => other,
    }
}

/// Fields read above the scan, or `None` if records pass through whole
/// (`SELECT *`).
fn required_fields(plan: &LogicalPlan) -> Option<BTreeSet<Vec<String>>> {
    let mut fields = BTreeSet::new();
    let mut node = plan;
    let mut projected = false;
    loop {
        match node {
            LogicalPlan::Scan { .. } => break,
            LogicalPlan::Filter { input, predicate } => {
                predicate.columns(&mut fields);
                node = input;
            }
            LogicalPlan::Project { input, columns } => {
                projected = true;
                for (expr, _) in columns {
                    expr.columns(&mut fields);
                }
                node = input;
            }
            LogicalPlan::WindowAggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                projected = true; // nothing above an aggregation reads raw fields
                for expr in group_by {
                    expr.columns(&mut fields);
                }
                for aggregate in aggregates {
                    if let Some(arg) = &aggregate.arg {
                        arg.columns(&mut fields);
                    }
                }
                node = input;
            }
        }
    }
    projected.then_some(fields)
}

fn prune_scan(plan: LogicalPlan, applied: &mut Vec<String>) -> LogicalPlan {
    let Some(fields) = required_fields(&plan) else {
        return plan;
    };
    // Paths under another required path are covered by it.
    let fields: Vec<Vec<String>> = fields
        .iter()
        .filter(|f| !fields.iter().any(|g| g != *f && f.starts_with(g)))
        .cloned()
        .collect();
    fn set(plan: LogicalPlan, fields: &[Vec<String>], applied: &mut Vec<String>) -> LogicalPlan {
        match plan {
            LogicalPlan::Scan { stream, predicate, .. } => {
                let names: Vec<_> = fields.iter().map(|f| f.join(".")).collect();
                applied.push(format!(
                    "projection pruning: the scan of `{stream}` keeps only [{}]",
                    names.join(", ")
                ));
                LogicalPlan::Scan {
                    stream,
                    predicate,
                    fields: Some(fields.to_vec()),
                }
            }
            other => with_input(other, &mut |input| set(input, fields, applied)),
        }
    }
    set(plan, &fields, applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::plan;
    use crate::parser::parse;

    fn optimized(sql: &str) -> Optimized {
        optimize(plan(&parse(sql).unwrap()).unwrap())
    }

    #[test]
    fn optimizes_the_plan_example() {
        let o = optimized(
            "SELECT user_id, COUNT(*) AS tx_count, SUM(amount) AS total FROM transactions
             WHERE amount > 0 WINDOW TUMBLING (SIZE 5 MINUTES) GROUP BY user_id HAVING COUNT(*) > 10",
        );
        assert_eq!(
            o.plan.explain(),
            "Project: user_id, COUNT(*) AS tx_count, SUM(amount) AS total\n  \
             Filter: COUNT(*) > 10\n    \
             WindowAggregate: TUMBLING(SIZE 5 MINUTES) group by [user_id] aggregates [COUNT(*), SUM(amount)]\n      \
             Scan: transactions where amount > 0 fields [amount, user_id]\n"
        );
    }

    #[test]
    fn folds_constants_and_drops_trivial_filters() {
        let o = optimized("SELECT a, 60 * 60 * 1000 AS hour_ms FROM t WHERE a > 2 * 5 AND 1 = 1");
        assert_eq!(
            o.plan.explain(),
            "Project: a, 3600000 AS hour_ms\n  Scan: t where a > 10 fields [a]\n"
        );
        assert!(o.applied.iter().any(|r| r.contains("`60 * 60 * 1000` → `3600000`")), "{:?}", o.applied);
        assert!(o.applied.iter().any(|r| r.contains("`1 = 1` → `TRUE`")));

        let o = optimized("SELECT a FROM t WHERE TRUE OR a > 1");
        assert_eq!(o.plan.explain(), "Project: a\n  Scan: t fields [a]\n");
        let o = optimized("SELECT a FROM t WHERE 1 > 2");
        assert!(o.applied.iter().any(|r| r.contains("never passes")), "{:?}", o.applied);
    }

    #[test]
    fn pushes_group_only_having_conjuncts_below_the_aggregation() {
        let o = optimized(
            "SELECT user_id, COUNT(*) AS n FROM t WHERE amount > 0
             WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY user_id
             HAVING user_id != 'bot' AND COUNT(*) > 2",
        );
        assert_eq!(
            o.plan.explain(),
            "Project: user_id, COUNT(*) AS n\n  \
             Filter: COUNT(*) > 2\n    \
             WindowAggregate: TUMBLING(SIZE 1 MINUTE) group by [user_id] aggregates [COUNT(*)]\n      \
             Scan: t where amount > 0 AND user_id != 'bot' fields [user_id]\n"
        );
        assert!(o.applied.iter().any(|r| r.contains("`user_id != 'bot'` moved from HAVING")));
    }

    #[test]
    fn select_star_is_not_pruned_and_nested_fields_are() {
        assert_eq!(optimized("SELECT * FROM t WHERE a = 1").plan.explain(), "Scan: t where a = 1\n");
        let o = optimized("SELECT order.id, order.total FROM t WHERE order IS NOT NULL");
        assert_eq!(
            o.plan.explain(),
            "Project: order.id AS id, order.total AS total\n  Scan: t where order IS NOT NULL fields [order.id, order.total]\n"
        );
        let o = optimized("SELECT COUNT(*) AS n FROM t WINDOW TUMBLING (SIZE 1 MINUTE)");
        assert!(o.plan.explain().contains("Scan: t fields []"), "{}", o.plan.explain());
    }
}
