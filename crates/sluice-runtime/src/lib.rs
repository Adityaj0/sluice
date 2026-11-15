//! Sluice's execution engine.
//!
//! A one-off pipeline is built with [`StreamEnv`] and started with
//! [`StreamEnv::execute`]:
//!
//! ```no_run
//! # async fn demo() {
//! use std::time::Duration;
//! use sluice_core::Event;
//! use sluice_runtime::{StreamEnv, WatermarkStrategy, window::{Count, TumblingWindows}};
//!
//! let env = StreamEnv::new();
//! let counts = env
//!     .from_iter("clicks", (0..1_000u64).map(|i| Event::new(i % 7, i * 100)), WatermarkStrategy::monotonous())
//!     .key_by(|user: &u64| user.to_string())
//!     .window(TumblingWindows::of(Duration::from_secs(10)), Count)
//!     .collect();
//! env.execute().wait().await;
//! println!("{} window results", counts.len());
//! # }
//! ```
//!
//! A fault-tolerant job is started with [`run_job`], which checkpoints the
//! pipeline and rebuilds it from the latest checkpoint after a failure:
//!
//! ```no_run
//! # async fn demo() -> Result<(), sluice_runtime::RuntimeError> {
//! use std::time::Duration;
//! use sluice_core::Event;
//! use sluice_runtime::{run_job, CheckpointConfig, CollectSink, JobConfig, RestartStrategy, WatermarkStrategy};
//! use sluice_runtime::window::{Count, TumblingWindows};
//!
//! let output = CollectSink::new().exactly_once();
//! let sink = output.clone();
//! let config = JobConfig::new("clicks")
//!     .with_checkpointing(CheckpointConfig::new("/tmp/sluice/clicks").with_interval(Duration::from_secs(1)))
//!     .with_restart(RestartStrategy::default());
//! let job = run_job(config, move |env| {
//!     env.from_iter("clicks", (0..1_000u64).map(|i| Event::new(i % 7, i * 100)), WatermarkStrategy::monotonous())
//!         .key_by(|user: &u64| user.to_string())
//!         .window(TumblingWindows::of(Duration::from_secs(10)), Count)
//!         .sink("output", sink.clone());
//! })?;
//! job.wait().await;
//! # Ok(()) }
//! ```

pub mod channel;
pub mod dag;
pub mod job;
pub mod join;
pub mod metrics;
pub mod sink;
pub mod source;
mod stage;
pub mod window;

pub use dag::{
    DEFAULT_CHANNEL_CAPACITY, DataStream, JobStatus, JoinedStreams, KeyFn, KeyedJoin, RuntimeError, StreamEnv, WindowedJoin,
    WindowedStream,
};
pub use sluice_checkpoint::{
    CheckpointConfig, CheckpointHistory, CheckpointRecord, CheckpointStatus, RestartStrategy, StageInfo,
};
pub use sluice_state::StateBackendKind;
pub use job::{JobConfig, JobHandle, run_job};
pub use metrics::{LatencyPercentiles, OperatorMetricsSnapshot};
pub use sink::{CollectSink, FnSink, Sink, StdoutSink};
pub use source::{SourceFunction, SourceItem, WatermarkStrategy};
pub use stage::{SourceCheckpoint, decode_source_checkpoint};
