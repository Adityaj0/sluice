//! Streaming SQL for Sluice.
//!
//! ```text
//! SQL text ──parser──▶ AST ──logical──▶ logical plan ──optimizer──▶ optimized plan
//!                                                                     │ physical
//!                                                                     ▼
//!                                         runtime stages on a DataStream (scan, key_by,
//!                                         window, filter, project)
//! ```
//!
//! ```
//! let query = sluice_sql::compile(
//!     "SELECT user_id, COUNT(*) AS tx_count, SUM(amount) AS total
//!      FROM transactions
//!      WHERE amount > 0
//!      WINDOW TUMBLING (SIZE 5 MINUTES)
//!      GROUP BY user_id
//!      HAVING COUNT(*) > 10",
//! )
//! .unwrap();
//! assert_eq!(query.physical.stream, "transactions");
//! println!("{}", query.explain());
//! ```

pub mod ast;
pub mod expr;
pub mod logical;
pub mod optimizer;
pub mod parser;
pub mod physical;

use serde::Serialize;
use serde_json::{Value, json};

pub use ast::Query;
pub use logical::LogicalPlan;
pub use physical::PhysicalPlan;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SqlError {
    #[error("syntax error at line {line}, column {column}: {message}")]
    Syntax { line: usize, column: usize, message: String },
    #[error("{message}")]
    Plan { message: String },
}

/// A query taken through every compiler stage.
#[derive(Debug, Clone)]
pub struct CompiledQuery {
    pub sql: String,
    pub ast: Query,
    /// The plan as written.
    pub logical: LogicalPlan,
    /// The plan after optimization.
    pub optimized: LogicalPlan,
    /// Optimizer rules that fired, in order.
    pub rules: Vec<String>,
    pub physical: PhysicalPlan,
}

/// Parses, plans, optimizes and lowers a query.
pub fn compile(sql: &str) -> Result<CompiledQuery, SqlError> {
    compile_with(sql, true)
}

/// Like [`compile`]; `optimize = false` lowers the plan as written (to
/// compare results, or to see what the optimizer buys).
pub fn compile_with(sql: &str, optimize: bool) -> Result<CompiledQuery, SqlError> {
    let ast = parser::parse(sql)?;
    let logical = logical::plan(&ast)?;
    let (optimized, rules) = if optimize {
        let result = optimizer::optimize(logical.clone());
        (result.plan, result.applied)
    } else {
        (logical.clone(), Vec::new())
    };
    let physical = PhysicalPlan::from_logical(&optimized);
    Ok(CompiledQuery {
        sql: sql.to_owned(),
        ast,
        logical,
        optimized,
        rules,
        physical,
    })
}

impl CompiledQuery {
    /// EXPLAIN output: logical plan, optimizations, optimized plan, stages.
    pub fn explain(&self) -> String {
        let mut out = String::from("== Logical plan ==\n");
        out += &self.logical.explain();
        out += "\n== Optimizations ==\n";
        if self.rules.is_empty() {
            out += "(none)\n";
        }
        for rule in &self.rules {
            out += &format!("- {rule}\n");
        }
        out += "\n== Optimized plan ==\n";
        out += &self.optimized.explain();
        out += "\n== Physical stages ==\n";
        out += &self.physical.explain();
        out
    }

    /// Everything [`explain`](Self::explain) shows, as JSON for UIs.
    pub fn to_json(&self) -> Value {
        json!({
            "stream": self.physical.stream,
            "logical": self.logical.to_json(),
            "optimized": self.optimized.to_json(),
            "optimizations": self.rules,
            "stages": self.physical.stages.iter().map(|s| json!({ "kind": s.kind(), "name": s.name() })).collect::<Vec<_>>(),
            "explain": self.explain(),
        })
    }
}
