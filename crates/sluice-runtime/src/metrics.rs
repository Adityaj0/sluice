use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sluice_core::Timestamp;
use serde::Serialize;

use crate::channel::ChannelStats;

/// Live counters for one pipeline stage. Updated by the stage's task and
/// read concurrently by the REST API, so everything is atomic.
#[derive(Debug)]
pub struct OperatorMetrics {
    id: usize,
    name: String,
    records_in: AtomicU64,
    records_out: AtomicU64,
    late_records: AtomicU64,
    watermark: AtomicU64,
    has_watermark: AtomicBool,
    idle: AtomicBool,
    inputs: Mutex<Vec<Arc<ChannelStats>>>,
    outputs: Mutex<Vec<Arc<ChannelStats>>>,
    ratio: Mutex<RatioWindow>,
    latency: [AtomicU64; LATENCY_BUCKETS.len()],
}

/// Upper bounds (ms) of the latency histogram buckets.
const LATENCY_BUCKETS: [u64; 15] = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 30_000, u64::MAX];

/// Latency percentiles (ms), each the upper bound of the histogram bucket
/// the percentile falls in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LatencyPercentiles {
    pub samples: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
}

#[derive(Debug)]
struct RatioWindow {
    since: Instant,
    blocked_at_start: u64,
    ratio: f64,
}

/// How often the backpressure ratio is recomputed.
const RATIO_WINDOW: Duration = Duration::from_millis(500);

/// Point-in-time copy of [`OperatorMetrics`].
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OperatorMetricsSnapshot {
    /// Position of the stage in the pipeline, in construction order.
    pub id: usize,
    pub name: String,
    pub records_in: u64,
    pub records_out: u64,
    /// Records dropped (or routed to a late side output) for arriving after
    /// the watermark passed their window plus the allowed lateness.
    pub late_records: u64,
    /// The last watermark this stage emitted (sources/operators) or
    /// received (sinks).
    pub watermark: Option<Timestamp>,
    /// The stage is idle (all its inputs are idle).
    pub idle: bool,
    /// Share of recent time spent waiting for downstream credit, 0..1.
    pub backpressure_ratio: f64,
    /// Total time spent waiting for downstream credit.
    pub backpressured_ms: u64,
    /// Elements waiting in the stage's input channels, and their capacity.
    pub input_queue: usize,
    pub input_capacity: usize,
    /// Sinks: time from ingestion to the record reaching the sink.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<LatencyPercentiles>,
}

impl OperatorMetrics {
    pub(crate) fn new(id: usize, name: &str) -> Self {
        Self {
            id,
            name: name.to_owned(),
            records_in: AtomicU64::new(0),
            records_out: AtomicU64::new(0),
            late_records: AtomicU64::new(0),
            watermark: AtomicU64::new(0),
            has_watermark: AtomicBool::new(false),
            idle: AtomicBool::new(false),
            inputs: Mutex::default(),
            outputs: Mutex::default(),
            ratio: Mutex::new(RatioWindow {
                since: Instant::now(),
                blocked_at_start: 0,
                ratio: 0.0,
            }),
            latency: Default::default(),
        }
    }

    pub(crate) fn record_latency(&self, ms: u64) {
        let bucket = LATENCY_BUCKETS.iter().position(|bound| ms <= *bound).unwrap_or(LATENCY_BUCKETS.len() - 1);
        self.latency[bucket].fetch_add(1, Relaxed);
    }

    fn latency_percentiles(&self) -> Option<LatencyPercentiles> {
        let counts: Vec<u64> = self.latency.iter().map(|c| c.load(Relaxed)).collect();
        let samples: u64 = counts.iter().sum();
        if samples == 0 {
            return None;
        }
        let percentile = |p: f64| {
            let rank = ((samples as f64) * p).ceil().max(1.0) as u64;
            let mut seen = 0;
            for (count, bound) in counts.iter().zip(LATENCY_BUCKETS) {
                seen += count;
                if seen >= rank {
                    return bound.min(60_000);
                }
            }
            60_000
        };
        Some(LatencyPercentiles {
            samples,
            p50: percentile(0.50),
            p95: percentile(0.95),
            p99: percentile(0.99),
        })
    }

    pub(crate) fn add_input(&self, stats: Arc<ChannelStats>) {
        self.inputs.lock().unwrap_or_else(PoisonError::into_inner).push(stats);
    }

    pub(crate) fn add_output(&self, stats: Arc<ChannelStats>) {
        self.outputs.lock().unwrap_or_else(PoisonError::into_inner).push(stats);
    }

    pub(crate) fn add_in(&self, n: u64) {
        self.records_in.fetch_add(n, Relaxed);
    }

    pub(crate) fn add_out(&self, n: u64) {
        self.records_out.fetch_add(n, Relaxed);
    }

    pub(crate) fn set_late(&self, n: u64) {
        self.late_records.store(n, Relaxed);
    }

    pub(crate) fn set_watermark(&self, watermark: Timestamp) {
        self.watermark.store(watermark, Relaxed);
        self.has_watermark.store(true, Relaxed);
    }

    pub(crate) fn set_idle(&self, idle: bool) {
        self.idle.store(idle, Relaxed);
    }

    fn blocked_nanos(&self) -> u64 {
        let outputs = self.outputs.lock().unwrap_or_else(PoisonError::into_inner);
        outputs.iter().map(|o| o.blocked_nanos()).sum()
    }

    /// Fraction of the last window (at least [`RATIO_WINDOW`]) spent blocked.
    fn backpressure_ratio(&self, blocked: u64) -> f64 {
        let mut window = self.ratio.lock().unwrap_or_else(PoisonError::into_inner);
        let elapsed = window.since.elapsed();
        if elapsed >= RATIO_WINDOW {
            let blocked_in_window = blocked.saturating_sub(window.blocked_at_start) as f64;
            window.ratio = (blocked_in_window / elapsed.as_nanos() as f64).clamp(0.0, 1.0);
            window.since = Instant::now();
            window.blocked_at_start = blocked;
        }
        window.ratio
    }

    pub fn snapshot(&self) -> OperatorMetricsSnapshot {
        let blocked = self.blocked_nanos();
        let (input_queue, input_capacity) = {
            let inputs = self.inputs.lock().unwrap_or_else(PoisonError::into_inner);
            (inputs.iter().map(|i| i.queued()).sum(), inputs.iter().map(|i| i.capacity()).sum())
        };
        OperatorMetricsSnapshot {
            id: self.id,
            name: self.name.clone(),
            records_in: self.records_in.load(Relaxed),
            records_out: self.records_out.load(Relaxed),
            late_records: self.late_records.load(Relaxed),
            watermark: self
                .has_watermark
                .load(Relaxed)
                .then(|| self.watermark.load(Relaxed)),
            idle: self.idle.load(Relaxed),
            backpressure_ratio: self.backpressure_ratio(blocked),
            backpressured_ms: blocked / 1_000_000,
            input_queue,
            input_capacity,
            latency_ms: self.latency_percentiles(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_percentiles_from_buckets() {
        let m = OperatorMetrics::new(0, "sink");
        assert_eq!(m.snapshot().latency_ms, None);
        for ms in 0..100 {
            m.record_latency(if ms < 90 { 3 } else { 150 });
        }
        let p = m.snapshot().latency_ms.unwrap();
        assert_eq!((p.samples, p.p50, p.p95, p.p99), (100, 5, 200, 200));
    }
}
