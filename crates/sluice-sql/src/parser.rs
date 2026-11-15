//! SQL text → [`Query`] AST.

use std::sync::LazyLock;

use pest::Parser;
use pest::iterators::{Pair, Pairs};
use pest::pratt_parser::{Assoc, Op, PrattParser};
use serde_json::Value;

use crate::SqlError;
use crate::ast::{AggFunc, BinaryOp, Expr, Query, SCALAR_FUNCTIONS, SelectItem, UnaryOp, WindowClause, WindowKind};

#[derive(pest_derive::Parser)]
#[grammar = "grammar.pest"]
struct SqlParser;

/// Operator precedence, lowest first.
static PRATT: LazyLock<PrattParser<Rule>> = LazyLock::new(|| {
    PrattParser::new()
        .op(Op::infix(Rule::or, Assoc::Left))
        .op(Op::infix(Rule::and, Assoc::Left))
        .op(Op::prefix(Rule::not))
        .op(Op::infix(Rule::eq, Assoc::Left)
            | Op::infix(Rule::neq, Assoc::Left)
            | Op::infix(Rule::lt, Assoc::Left)
            | Op::infix(Rule::lte, Assoc::Left)
            | Op::infix(Rule::gt, Assoc::Left)
            | Op::infix(Rule::gte, Assoc::Left))
        .op(Op::postfix(Rule::is_null) | Op::postfix(Rule::is_not_null))
        .op(Op::infix(Rule::add, Assoc::Left) | Op::infix(Rule::sub, Assoc::Left))
        .op(Op::infix(Rule::mul, Assoc::Left) | Op::infix(Rule::div, Assoc::Left) | Op::infix(Rule::modulo, Assoc::Left))
        .op(Op::prefix(Rule::neg))
});

fn rule_name(rule: &Rule) -> String {
    match rule {
        Rule::expr | Rule::primary => "an expression".into(),
        Rule::identifier | Rule::bare_name | Rule::quoted_name => "a name".into(),
        Rule::select_clause => "SELECT".into(),
        Rule::from_clause => "FROM".into(),
        Rule::where_clause => "WHERE".into(),
        Rule::window_clause => "WINDOW".into(),
        Rule::group_by_clause => "GROUP BY".into(),
        Rule::having_clause => "HAVING".into(),
        Rule::alias => "an alias".into(),
        Rule::select_item => "a column or expression".into(),
        Rule::tumbling | Rule::sliding | Rule::session => "TUMBLING, SLIDING or SESSION".into(),
        Rule::lateness => "ALLOWED LATENESS".into(),
        Rule::duration | Rule::number => "a number".into(),
        Rule::time_unit => "a time unit (MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS)".into(),
        Rule::EOI => "end of query".into(),
        Rule::or | Rule::and => "AND/OR".into(),
        Rule::eq | Rule::neq | Rule::lt | Rule::lte | Rule::gt | Rule::gte => "a comparison".into(),
        Rule::add | Rule::sub | Rule::mul | Rule::div | Rule::modulo => "an operator".into(),
        other => format!("{other:?}"),
    }
}

impl SqlError {
    fn from_pest(error: pest::error::Error<Rule>) -> Self {
        let error = error.renamed_rules(rule_name);
        let (line, column) = match error.line_col {
            pest::error::LineColLocation::Pos(pos) | pest::error::LineColLocation::Span(pos, _) => pos,
        };
        SqlError::Syntax {
            line,
            column,
            message: error.variant.message().into_owned(),
        }
    }

    fn at(pair: &Pair<'_, Rule>, message: impl Into<String>) -> Self {
        let (line, column) = pair.as_span().start_pos().line_col();
        SqlError::Syntax {
            line,
            column,
            message: message.into(),
        }
    }
}

/// Parses one query.
pub fn parse(sql: &str) -> Result<Query, SqlError> {
    let query = SqlParser::parse(Rule::query, sql)
        .map_err(SqlError::from_pest)?
        .next()
        .expect("the query rule always produces a pair");

    let mut ast = Query {
        select: Vec::new(),
        from: String::new(),
        selection: None,
        window: None,
        group_by: Vec::new(),
        having: None,
    };
    for clause in query.into_inner() {
        match clause.as_rule() {
            Rule::select_clause => {
                for item in clause.into_inner() {
                    ast.select.push(parse_select_item(item)?);
                }
            }
            Rule::from_clause => ast.from = identifier(first(clause)),
            Rule::where_clause => ast.selection = Some(parse_expr(first(clause))?),
            Rule::window_clause => ast.window = Some(parse_window(clause)?),
            Rule::group_by_clause => {
                for expr in clause.into_inner() {
                    ast.group_by.push(parse_expr(expr)?);
                }
            }
            Rule::having_clause => ast.having = Some(parse_expr(first(clause))?),
            _ => {}
        }
    }
    Ok(ast)
}

fn first(pair: Pair<'_, Rule>) -> Pair<'_, Rule> {
    pair.into_inner().next().expect("grammar guarantees a child")
}

fn identifier(pair: Pair<'_, Rule>) -> String {
    let inner = first(pair);
    match inner.as_rule() {
        Rule::quoted_name => inner.as_str().trim_matches('"').to_owned(),
        _ => inner.as_str().to_owned(),
    }
}

fn parse_select_item(item: Pair<'_, Rule>) -> Result<SelectItem, SqlError> {
    let mut parts = item.into_inner();
    let first = parts.next().expect("select item has content");
    if first.as_rule() == Rule::star {
        return Ok(SelectItem::Wildcard);
    }
    let expr = parse_expr(first)?;
    let alias = parts.next().map(|alias| {
        let name = alias.into_inner().find(|p| p.as_rule() == Rule::identifier).expect("alias has a name");
        identifier(name)
    });
    Ok(SelectItem::Expr { expr, alias })
}

fn parse_duration(pair: Pair<'_, Rule>) -> Result<u64, SqlError> {
    let mut parts = pair.clone().into_inner();
    let number = parts.next().expect("duration has a number");
    let unit = parts.next().expect("duration has a unit").as_str().to_ascii_uppercase();
    let value: f64 = number.as_str().parse().map_err(|_| SqlError::at(&number, "invalid number"))?;
    let scale = match unit.as_str() {
        "MILLISECOND" | "MILLISECONDS" | "MS" => 1.0,
        "SECOND" | "SECONDS" | "S" => 1_000.0,
        "MINUTE" | "MINUTES" => 60_000.0,
        "HOUR" | "HOURS" => 3_600_000.0,
        _ => 86_400_000.0,
    };
    let ms = value * scale;
    if ms < 1.0 || ms.fract() != 0.0 {
        return Err(SqlError::at(&pair, "durations must be a whole, positive number of milliseconds"));
    }
    Ok(ms as u64)
}

fn parse_window(clause: Pair<'_, Rule>) -> Result<WindowClause, SqlError> {
    let mut parts = clause.into_inner();
    let spec = parts.next().expect("window clause has a kind");
    let rule = spec.as_rule();
    let durations = spec
        .clone()
        .into_inner()
        .map(parse_duration)
        .collect::<Result<Vec<_>, _>>()?;
    let kind = match rule {
        Rule::tumbling => WindowKind::Tumbling { size_ms: durations[0] },
        Rule::sliding => {
            let (size_ms, slide_ms) = (durations[0], durations[1]);
            if slide_ms > size_ms {
                return Err(SqlError::at(&spec, "SLIDE must not be larger than SIZE"));
            }
            WindowKind::Sliding { size_ms, slide_ms }
        }
        _ => WindowKind::Session { gap_ms: durations[0] },
    };
    let allowed_lateness_ms = match parts.next() {
        Some(lateness) => parse_duration(first(lateness))?,
        None => 0,
    };
    Ok(WindowClause {
        kind,
        allowed_lateness_ms,
    })
}

fn parse_expr(pair: Pair<'_, Rule>) -> Result<Expr, SqlError> {
    parse_pratt(pair.into_inner())
}

fn parse_pratt(pairs: Pairs<'_, Rule>) -> Result<Expr, SqlError> {
    PRATT
        .map_primary(parse_primary)
        .map_prefix(|op, operand| {
            let op = match op.as_rule() {
                Rule::not => UnaryOp::Not,
                _ => UnaryOp::Neg,
            };
            Ok(Expr::Unary {
                op,
                expr: Box::new(operand?),
            })
        })
        .map_postfix(|operand, op| {
            let op = match op.as_rule() {
                Rule::is_null => UnaryOp::IsNull,
                _ => UnaryOp::IsNotNull,
            };
            Ok(Expr::Unary {
                op,
                expr: Box::new(operand?),
            })
        })
        .map_infix(|left, op, right| {
            let op = match op.as_rule() {
                Rule::or => BinaryOp::Or,
                Rule::and => BinaryOp::And,
                Rule::eq => BinaryOp::Eq,
                Rule::neq => BinaryOp::NotEq,
                Rule::lt => BinaryOp::Lt,
                Rule::lte => BinaryOp::LtEq,
                Rule::gt => BinaryOp::Gt,
                Rule::gte => BinaryOp::GtEq,
                Rule::add => BinaryOp::Plus,
                Rule::sub => BinaryOp::Minus,
                Rule::mul => BinaryOp::Multiply,
                Rule::div => BinaryOp::Divide,
                _ => BinaryOp::Modulo,
            };
            Ok(Expr::binary(op, left?, right?))
        })
        .parse(pairs)
}

fn parse_primary(pair: Pair<'_, Rule>) -> Result<Expr, SqlError> {
    Ok(match pair.as_rule() {
        Rule::number => {
            let text = pair.as_str();
            let value = match text.parse::<i64>() {
                Ok(n) => Value::from(n),
                Err(_) => Value::from(text.parse::<f64>().map_err(|_| SqlError::at(&pair, "invalid number"))?),
            };
            Expr::Literal(value)
        }
        Rule::string => Expr::Literal(Value::String(first(pair).as_str().replace("''", "'"))),
        Rule::boolean => Expr::Literal(Value::Bool(pair.as_str().eq_ignore_ascii_case("true"))),
        Rule::null => Expr::Literal(Value::Null),
        Rule::window_bound => {
            if pair.as_str().eq_ignore_ascii_case("window_start") {
                Expr::WindowStart
            } else {
                Expr::WindowEnd
            }
        }
        Rule::column => Expr::Column(
            pair.into_inner()
                .map(|segment| match segment.as_rule() {
                    Rule::path_index => segment.as_str().to_owned(),
                    _ => identifier(segment),
                })
                .collect(),
        ),
        Rule::expr => parse_expr(pair)?,
        Rule::function_call => parse_function(pair)?,
        other => unreachable!("unexpected primary {other:?}"),
    })
}

fn parse_function(pair: Pair<'_, Rule>) -> Result<Expr, SqlError> {
    let mut parts = pair.clone().into_inner();
    let name = identifier(parts.next().expect("function has a name")).to_ascii_uppercase();
    let rest: Vec<_> = parts.collect();
    let star = rest.first().is_some_and(|p| p.as_rule() == Rule::count_star);

    if let Some(func) = AggFunc::from_name(&name) {
        if star {
            if func != AggFunc::Count {
                return Err(SqlError::at(&pair, format!("{name}(*) is not valid; only COUNT(*)")));
            }
            return Ok(Expr::Aggregate { func, arg: None });
        }
        if rest.len() != 1 {
            return Err(SqlError::at(&pair, format!("{name} takes exactly one argument")));
        }
        let arg = parse_expr(rest.into_iter().next().expect("checked"))?;
        return Ok(Expr::Aggregate {
            func,
            arg: Some(Box::new(arg)),
        });
    }

    let Some(&(_, min, max)) = SCALAR_FUNCTIONS.iter().find(|(n, _, _)| *n == name) else {
        let known: Vec<_> = ["COUNT", "SUM", "AVG", "MIN", "MAX"]
            .into_iter()
            .chain(SCALAR_FUNCTIONS.iter().map(|(n, _, _)| *n))
            .collect();
        return Err(SqlError::at(
            &pair,
            format!("unknown function {name}; available: {}", known.join(", ")),
        ));
    };
    if star {
        return Err(SqlError::at(&pair, format!("{name}(*) is not valid")));
    }
    if rest.len() < min || rest.len() > max {
        return Err(SqlError::at(&pair, format!("wrong number of arguments to {name}")));
    }
    let args = rest.into_iter().map(parse_expr).collect::<Result<_, _>>()?;
    Ok(Expr::Function { name, args })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expr(sql: &str) -> String {
        let query = parse(&format!("SELECT {sql} FROM t")).unwrap();
        match &query.select[0] {
            SelectItem::Expr { expr, .. } => expr.to_string(),
            SelectItem::Wildcard => "*".into(),
        }
    }

    #[test]
    fn parses_the_plan_example() {
        let q = parse(
            "SELECT user_id, COUNT(*) as tx_count, SUM(amount) as total
             FROM transactions
             WINDOW TUMBLING (SIZE 5 MINUTES)
             GROUP BY user_id
             HAVING COUNT(*) > 10",
        )
        .unwrap();
        assert_eq!(q.from, "transactions");
        assert_eq!(q.select.len(), 3);
        assert!(matches!(&q.select[1], SelectItem::Expr { alias: Some(a), .. } if a == "tx_count"));
        assert_eq!(q.window.unwrap().kind, WindowKind::Tumbling { size_ms: 300_000 });
        assert_eq!(q.group_by, vec![Expr::column("user_id")]);
        assert_eq!(q.having.unwrap().to_string(), "COUNT(*) > 10");
    }

    #[test]
    fn operator_precedence() {
        assert_eq!(expr("a + b * c"), "a + b * c");
        assert_eq!(expr("(a + b) * c"), "(a + b) * c");
        assert_eq!(expr("a - b - c"), "a - b - c");
        assert_eq!(expr("a OR b AND c"), "a OR b AND c");
        assert_eq!(expr("(a OR b) AND c"), "(a OR b) AND c");
        assert_eq!(expr("NOT a = 1 AND b IS NOT NULL"), "NOT a = 1 AND b IS NOT NULL");
        assert_eq!(expr("-x * 2"), "-x * 2");
        assert_eq!(expr("a <> 'it''s'"), "a != 'it''s'");
    }

    #[test]
    fn literals_paths_functions_and_keywords_are_case_insensitive() {
        assert_eq!(expr("order.items.price"), "order.items.price");
        assert_eq!(expr("\"window\""), "window");
        assert_eq!(expr("lower(name)"), "LOWER(name)");
        assert_eq!(expr("coalesce(a, b, 0)"), "COALESCE(a, b, 0)");
        assert_eq!(expr("window_start"), "WINDOW_START");
        assert_eq!(expr("true AND null"), "TRUE AND NULL");
        assert_eq!(expr("1.5"), "1.5");
        let q = parse("select a as b from t where a > 1 window sliding (size 1 minute, slide 10 seconds) allowed lateness 5 s group by a").unwrap();
        let window = q.window.unwrap();
        assert_eq!(window.kind, WindowKind::Sliding { size_ms: 60_000, slide_ms: 10_000 });
        assert!(parse("SELECT a FROM t WINDOW SESSION (GAP 30 SECONDS) ALLOWED LATENESS 1 MINUTE GROUP BY a").is_ok());
        assert_eq!(
            parse("SELECT a FROM t WINDOW SESSION (GAP 30 SECONDS) ALLOWED LATENESS 1 MINUTE GROUP BY a")
                .unwrap()
                .window
                .unwrap()
                .allowed_lateness_ms,
            60_000
        );
    }

    #[test]
    fn syntax_errors_point_at_the_problem() {
        let err = parse("SELECT a FROM").unwrap_err();
        assert!(matches!(&err, SqlError::Syntax { line: 1, column: 14, message } if message.contains("a name")), "{err}");

        let err = parse("SELECT a,\nFROM t").unwrap_err();
        assert!(matches!(err, SqlError::Syntax { line: 2, column: 1, .. }), "{err}");

        let err = parse("SELECT a, FROM t").unwrap_err();
        assert!(err.to_string().contains("expected a column or expression"), "{err}");

        let err = parse("SELECT FOO(a) FROM t").unwrap_err();
        assert!(err.to_string().contains("unknown function FOO"), "{err}");

        let err = parse("SELECT SUM(*) FROM t").unwrap_err();
        assert!(err.to_string().contains("only COUNT(*)"), "{err}");

        let err = parse("SELECT a FROM t WINDOW TUMBLING (SIZE 5 FORTNIGHTS)").unwrap_err();
        assert!(err.to_string().contains("time unit"), "{err}");

        let err = parse("SELECT a FROM t WINDOW SLIDING (SIZE 1 MINUTE, SLIDE 2 MINUTES)").unwrap_err();
        assert!(err.to_string().contains("SLIDE"), "{err}");

        assert!(parse("SELECT a FROM t -- trailing comment\n;").is_ok());
    }
}
