//! Event ingestion: a tonic gRPC server that appends incoming events to the
//! [`EventRouter`]'s per-stream logs, and [`LogSource`], the replayable job
//! source that reads them.

pub mod router;
pub mod server;
pub mod source;

#[allow(clippy::result_large_err)] // generated code
pub mod proto {
    tonic::include_proto!("sluice.ingest.v1");
}

pub use router::{DEFAULT_RETENTION, EventRouter, Offset, RawEvent, ReadResult, RouteError, StreamInfo, StreamLog, Subscription};
pub use server::{IngestService, serve};
pub use source::LogSource;
