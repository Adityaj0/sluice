//! Prometheus text exposition of per-operator metrics.

use std::fmt::Write;

use sluice_core::MAX_WATERMARK;
use sluice_runtime::OperatorMetricsSnapshot;

use crate::job_manager::{JobManager, JobMetrics};

type Extract = fn(&OperatorMetricsSnapshot) -> Option<f64>;

const FAMILIES: [(&str, &str, &str, Extract); 8] = [
    (
        "sluice_operator_records_in_total",
        "counter",
        "Records received by the operator.",
        |m| Some(m.records_in as f64),
    ),
    (
        "sluice_operator_records_out_total",
        "counter",
        "Records emitted by the operator.",
        |m| Some(m.records_out as f64),
    ),
    (
        "sluice_operator_late_records_total",
        "counter",
        "Records dropped for arriving behind the watermark.",
        |m| Some(m.late_records as f64),
    ),
    (
        "sluice_operator_watermark_ms",
        "gauge",
        "Current event-time watermark of the operator (ms since epoch).",
        |m| m.watermark.filter(|wm| *wm != MAX_WATERMARK).map(|wm| wm as f64),
    ),
    (
        "sluice_operator_backpressure_ratio",
        "gauge",
        "Share of recent time the operator spent waiting for downstream credit (0..1).",
        |m| Some(m.backpressure_ratio),
    ),
    (
        "sluice_operator_backpressured_ms_total",
        "counter",
        "Total time the operator spent waiting for downstream credit.",
        |m| Some(m.backpressured_ms as f64),
    ),
    (
        "sluice_operator_input_queue_usage",
        "gauge",
        "Fraction of the operator's input buffers in use (0..1).",
        |m| (m.input_capacity > 0).then(|| m.input_queue as f64 / m.input_capacity as f64),
    ),
    (
        "sluice_operator_idle",
        "gauge",
        "1 if all of the operator's inputs are idle.",
        |m| Some(if m.idle { 1.0 } else { 0.0 }),
    ),
];

type JobExtract = fn(&JobMetrics) -> Option<u64>;

const JOB_FAMILIES: [(&str, &str, &str, JobExtract); 5] = [
    (
        "sluice_job_restarts_total",
        "counter",
        "Times the job was restarted after a failure.",
        |j| Some(j.restarts as u64),
    ),
    (
        "sluice_job_checkpoints_completed_total",
        "counter",
        "Completed checkpoints.",
        |j| Some(j.checkpoints.completed),
    ),
    (
        "sluice_job_checkpoints_failed_total",
        "counter",
        "Failed or aborted checkpoints.",
        |j| Some(j.checkpoints.failed),
    ),
    (
        "sluice_job_last_checkpoint_size_bytes",
        "gauge",
        "State size of the latest completed checkpoint.",
        |j| last_completed(j).map(|r| r.state_size),
    ),
    (
        "sluice_job_last_checkpoint_duration_ms",
        "gauge",
        "Duration of the latest completed checkpoint.",
        |j| last_completed(j).and_then(|r| r.duration_ms),
    ),
];

fn last_completed(job: &JobMetrics) -> Option<&sluice_runtime::CheckpointRecord> {
    let id = job.checkpoints.latest_completed?;
    job.checkpoints.record(id)
}

pub fn render(manager: &JobManager) -> String {
    let jobs = manager.all_metrics();
    let mut out = String::new();
    for (name, kind, help, extract) in JOB_FAMILIES {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        for job in &jobs {
            if let Some(value) = extract(job) {
                let _ = writeln!(out, "{name}{{job_id=\"{}\",job=\"{}\"}} {value}", job.id, escape(&job.name));
            }
        }
    }
    for (name, kind, help, extract) in FAMILIES {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        for job in &jobs {
            for op in &job.operators {
                if let Some(value) = extract(op) {
                    let _ = writeln!(
                        out,
                        "{name}{{job_id=\"{}\",job=\"{}\",operator_id=\"{}\",operator=\"{}\"}} {value}",
                        job.id,
                        escape(&job.name),
                        op.id,
                        escape(&op.name),
                    );
                }
            }
        }
    }
    out
}

fn escape(label: &str) -> String {
    label.replace('\\', r"\\").replace('"', "\\\"").replace('\n', r"\n")
}

#[cfg(test)]
mod tests {
    use super::escape;

    #[test]
    fn escapes_label_values() {
        assert_eq!(escape(r#"a "b" \c"#), r#"a \"b\" \\c"#);
    }
}
