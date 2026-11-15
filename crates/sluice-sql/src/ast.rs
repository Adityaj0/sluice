//! Abstract syntax tree of a streaming query.

use std::collections::BTreeSet;
use std::fmt;

use serde::Serialize;
use serde_json::Value;

/// `SELECT ... FROM ... [WHERE] [WINDOW] [GROUP BY] [HAVING]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub select: Vec<SelectItem>,
    pub from: String,
    pub selection: Option<Expr>,
    pub window: Option<WindowClause>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WindowKind {
    Tumbling { size_ms: u64 },
    Sliding { size_ms: u64, slide_ms: u64 },
    Session { gap_ms: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct WindowClause {
    #[serde(flatten)]
    pub kind: WindowKind,
    pub allowed_lateness_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Or,
    And,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "COUNT" => Self::Count,
            "SUM" => Self::Sum,
            "AVG" => Self::Avg,
            "MIN" => Self::Min,
            "MAX" => Self::Max,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}

/// Scalar functions, with their accepted argument counts.
pub const SCALAR_FUNCTIONS: &[(&str, usize, usize)] = &[
    ("LOWER", 1, 1),
    ("UPPER", 1, 1),
    ("LENGTH", 1, 1),
    ("ABS", 1, 1),
    ("ROUND", 1, 2),
    ("COALESCE", 1, usize::MAX),
];

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Value),
    /// A field of the input record; nested fields as a dotted path.
    Column(Vec<String>),
    WindowStart,
    WindowEnd,
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// A scalar function call; `name` is upper case.
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// An aggregate call; `COUNT(*)` has no argument.
    Aggregate {
        func: AggFunc,
        arg: Option<Box<Expr>>,
    },
    /// After planning: the i-th GROUP BY value of an aggregated row.
    GroupRef(usize),
    /// After planning: the i-th aggregate of an aggregated row.
    AggRef(usize),
}

impl Expr {
    pub fn column(path: &str) -> Self {
        Expr::Column(path.split('.').map(str::to_owned).collect())
    }

    pub fn binary(op: BinaryOp, left: Expr, right: Expr) -> Self {
        Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn children(&self) -> Vec<&Expr> {
        match self {
            Expr::Unary { expr, .. } => vec![expr],
            Expr::Binary { left, right, .. } => vec![left, right],
            Expr::Function { args, .. } => args.iter().collect(),
            Expr::Aggregate { arg, .. } => arg.iter().map(|a| &**a).collect(),
            _ => Vec::new(),
        }
    }

    /// Whether `pred` holds for this expression or any sub-expression.
    pub fn any(&self, pred: &impl Fn(&Expr) -> bool) -> bool {
        pred(self) || self.children().into_iter().any(|c| c.any(pred))
    }

    pub fn contains_aggregate(&self) -> bool {
        self.any(&|e| matches!(e, Expr::Aggregate { .. }))
    }

    pub fn contains_window_bound(&self) -> bool {
        self.any(&|e| matches!(e, Expr::WindowStart | Expr::WindowEnd))
    }

    /// No input dependence at all: can be evaluated at planning time.
    pub fn is_constant(&self) -> bool {
        !self.any(&|e| {
            matches!(
                e,
                Expr::Column(_)
                    | Expr::WindowStart
                    | Expr::WindowEnd
                    | Expr::Aggregate { .. }
                    | Expr::GroupRef(_)
                    | Expr::AggRef(_)
            )
        })
    }

    /// Input fields this expression reads.
    pub fn columns(&self, out: &mut BTreeSet<Vec<String>>) {
        if let Expr::Column(path) = self {
            out.insert(path.clone());
        }
        for child in self.children() {
            child.columns(out);
        }
    }

    /// Rewrites the tree top-down: `f` returns `Some(replacement)` to stop
    /// descending, `None` to keep the node and rewrite its children.
    pub fn rewrite(self, f: &mut impl FnMut(&Expr) -> Option<Expr>) -> Expr {
        if let Some(replacement) = f(&self) {
            return replacement;
        }
        match self {
            Expr::Unary { op, expr } => Expr::Unary {
                op,
                expr: Box::new(expr.rewrite(f)),
            },
            Expr::Binary { op, left, right } => Expr::Binary {
                op,
                left: Box::new(left.rewrite(f)),
                right: Box::new(right.rewrite(f)),
            },
            Expr::Function { name, args } => Expr::Function {
                name,
                args: args.into_iter().map(|a| a.rewrite(f)).collect(),
            },
            Expr::Aggregate { func, arg } => Expr::Aggregate {
                func,
                arg: arg.map(|a| Box::new(a.rewrite(f))),
            },
            other => other,
        }
    }

    /// Splits `a AND b AND c` into its conjuncts.
    pub fn conjuncts(self) -> Vec<Expr> {
        match self {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                let mut out = left.conjuncts();
                out.extend(right.conjuncts());
                out
            }
            other => vec![other],
        }
    }

    /// Joins conjuncts back with AND (`None` if there are none).
    pub fn and_all(conjuncts: impl IntoIterator<Item = Expr>) -> Option<Expr> {
        conjuncts.into_iter().reduce(|a, b| Expr::binary(BinaryOp::And, a, b))
    }
}

impl BinaryOp {
    fn precedence(self) -> u8 {
        match self {
            BinaryOp::Or => 1,
            BinaryOp::And => 2,
            BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => 4,
            BinaryOp::Plus | BinaryOp::Minus => 5,
            BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo => 6,
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            BinaryOp::Or => "OR",
            BinaryOp::And => "AND",
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "!=",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::Plus => "+",
            BinaryOp::Minus => "-",
            BinaryOp::Multiply => "*",
            BinaryOp::Divide => "/",
            BinaryOp::Modulo => "%",
        }
    }
}

fn precedence(expr: &Expr) -> u8 {
    match expr {
        Expr::Binary { op, .. } => op.precedence(),
        Expr::Unary { op: UnaryOp::Not, .. } => 3,
        Expr::Unary {
            op: UnaryOp::IsNull | UnaryOp::IsNotNull,
            ..
        } => 4,
        _ => 10,
    }
}

fn write_operand(f: &mut fmt::Formatter<'_>, expr: &Expr, min: u8) -> fmt::Result {
    if precedence(expr) < min {
        write!(f, "({expr})")
    } else {
        write!(f, "{expr}")
    }
}

/// SQL text of the expression, parenthesised only where needed.
impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Literal(Value::String(s)) => write!(f, "'{}'", s.replace('\'', "''")),
            Expr::Literal(Value::Bool(b)) => f.write_str(if *b { "TRUE" } else { "FALSE" }),
            Expr::Literal(Value::Null) => f.write_str("NULL"),
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::Column(path) => f.write_str(&path.join(".")),
            Expr::WindowStart => f.write_str("WINDOW_START"),
            Expr::WindowEnd => f.write_str("WINDOW_END"),
            Expr::Unary { op, expr } => match op {
                UnaryOp::Not => {
                    f.write_str("NOT ")?;
                    write_operand(f, expr, 3)
                }
                UnaryOp::Neg => {
                    f.write_str("-")?;
                    write_operand(f, expr, 10)
                }
                UnaryOp::IsNull | UnaryOp::IsNotNull => {
                    write_operand(f, expr, 5)?;
                    f.write_str(if *op == UnaryOp::IsNull { " IS NULL" } else { " IS NOT NULL" })
                }
            },
            Expr::Binary { op, left, right } => {
                let p = op.precedence();
                write_operand(f, left, p)?;
                write!(f, " {} ", op.symbol())?;
                // Right operand of an equal-precedence operator needs
                // parentheses for non-associative operators: a - (b - c).
                write_operand(f, right, p + 1)
            }
            Expr::Function { name, args } => {
                write!(f, "{name}(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                f.write_str(")")
            }
            Expr::Aggregate { func, arg } => match arg {
                Some(arg) => write!(f, "{}({arg})", func.name()),
                None => write!(f, "{}(*)", func.name()),
            },
            Expr::GroupRef(i) => write!(f, "#group{i}"),
            Expr::AggRef(i) => write!(f, "#agg{i}"),
        }
    }
}

impl fmt::Display for WindowClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            WindowKind::Tumbling { size_ms } => write!(f, "TUMBLING(SIZE {})", duration(size_ms))?,
            WindowKind::Sliding { size_ms, slide_ms } => {
                write!(f, "SLIDING(SIZE {}, SLIDE {})", duration(size_ms), duration(slide_ms))?
            }
            WindowKind::Session { gap_ms } => write!(f, "SESSION(GAP {})", duration(gap_ms))?,
        }
        if self.allowed_lateness_ms > 0 {
            write!(f, " ALLOWED LATENESS {}", duration(self.allowed_lateness_ms))?;
        }
        Ok(())
    }
}

/// `300000` → `5 MINUTES`.
pub fn duration(ms: u64) -> String {
    for (unit, size) in [("DAYS", 86_400_000), ("HOURS", 3_600_000), ("MINUTES", 60_000), ("SECONDS", 1_000)] {
        if ms >= size && ms % size == 0 {
            let n = ms / size;
            let unit = if n == 1 { &unit[..unit.len() - 1] } else { unit };
            return format!("{n} {unit}");
        }
    }
    format!("{ms} MILLISECONDS")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn display_parenthesises_only_when_needed() {
        let a = Expr::column("a");
        let b = Expr::column("b");
        let c = Expr::column("c");
        let sum = Expr::binary(BinaryOp::Plus, a.clone(), b.clone());
        assert_eq!(Expr::binary(BinaryOp::Multiply, sum.clone(), c.clone()).to_string(), "(a + b) * c");
        assert_eq!(Expr::binary(BinaryOp::Plus, c.clone(), sum.clone()).to_string(), "c + (a + b)");
        assert_eq!(
            Expr::binary(BinaryOp::And, Expr::binary(BinaryOp::Gt, a.clone(), Expr::Literal(json!(1))), b.clone()).to_string(),
            "a > 1 AND b"
        );
        assert_eq!(Expr::Literal(json!("it's")).to_string(), "'it''s'");
        assert_eq!(duration(300_000), "5 MINUTES");
        assert_eq!(duration(1_000), "1 SECOND");
        assert_eq!(duration(1_500), "1500 MILLISECONDS");
    }
}
