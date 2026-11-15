//! REST API for Sluice: submit JSON job definitions, inspect their
//! status, per-operator metrics and recent output, and cancel them.

pub mod generators;
pub mod inspect;
pub mod job_def;
pub mod job_manager;
pub mod metrics;
pub mod routes;

pub use job_def::JobDefinition;
pub use job_manager::{JobError, JobManager};
pub use routes::{app, app_with_ui, serve};
