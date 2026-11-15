import { LineChart } from "lucide-react";
import { useEffect, useState } from "react";

import { api } from "../../api.ts";
import { useLiveJob } from "../../hooks/useWebSocket.ts";
import { compact, duration, percent } from "../../lib/format.ts";
import { parseStage } from "../../lib/stages.ts";
import type { Sample } from "../../store/pipelineStore.ts";
import { useStore } from "../../store/pipelineStore.ts";
import type { CheckpointRecord, StageInfo } from "../../types/pipeline.ts";
import { Card, EmptyState, JobPicker, Page, PageHeader, Sparkline, tableClass, tdClass, thClass } from "../common/ui.tsx";
import { CheckpointDurationChart, CheckpointStateChart, CheckpointTable, STATE_SERIES } from "./CheckpointTable.tsx";
import { SERIES, SeriesLegend, TimeSeriesChart } from "./charts.tsx";
import type { Series } from "./charts.tsx";

const FLOW_SERIES: Series[] = [
  { key: "in", name: "Read from sources", color: SERIES.primary },
  { key: "out", name: "Delivered to sinks", color: SERIES.secondary, dashed: true },
];

function Collecting() {
  return <div className="grid h-full place-items-center text-[13px] text-slate-400">Collecting samples…</div>;
}

/** Job-level series from per-operator samples: flow in/out, worst lag, worst backpressure. */
function jobSeries(history: Sample[], stages: StageInfo[]) {
  const consumers = new Set(stages.flatMap((s) => s.inputs));
  const sum = (values: number[]) => values.reduce((a, b) => a + b, 0);
  return history.map((sample) => {
    const ops = sample.operators;
    const lags = ops.map((o) => o.watermarkLagMs).filter((v): v is number => v != null);
    return {
      ts: sample.ts,
      in: sum(ops.filter((_, i) => stages[i]?.is_source).map((o) => o.outPerSec)),
      out: sum(ops.filter((_, i) => stages[i] && !consumers.has(i)).map((o) => o.inPerSec)),
      lag: lags.length ? Math.max(...lags) : null,
      pressure: ops.length ? Math.max(...ops.map((o) => o.backpressure)) : 0,
    };
  });
}

export function MetricsView() {
  const jobId = useStore((s) => s.jobId);
  const live = useLiveJob(jobId);
  const [records, setRecords] = useState<CheckpointRecord[]>([]);

  useEffect(() => {
    setRecords([]);
    if (jobId == null) return;
    let alive = true;
    const load = () =>
      api
        .checkpoints(jobId)
        .then((c) => alive && setRecords(c.records))
        .catch(() => {});
    void load();
    const timer = setInterval(load, 2000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [jobId]);

  const history = live?.history ?? [];
  const stages = live?.latest?.stages ?? [];
  const rows = jobSeries(history, stages);
  const ready = rows.length > 1;
  const latest = history[history.length - 1];
  const completed = records.some((r) => r.status.state === "completed");

  return (
    <Page>
      <PageHeader title="Metrics" description="Live samples twice a second for the last five minutes; checkpoint history for the job's whole life." actions={<JobPicker />} />
      {jobId == null ? (
        <Card>
          <EmptyState icon={LineChart} title="No jobs yet" />
        </Card>
      ) : (
        <>
          <div className="grid gap-4 xl:grid-cols-2">
            <Card title="Throughput" description="Records per second entering and leaving the job." actions={<SeriesLegend series={FLOW_SERIES} />} bodyClassName="h-60 px-2 pt-3 pb-1">
              {ready ? <TimeSeriesChart rows={rows} series={FLOW_SERIES} format={compact} /> : <Collecting />}
            </Card>
            <Card title="Watermark lag" description="How far event time trails the wall clock (slowest operator)." bodyClassName="h-60 px-2 pt-3 pb-1">
              {ready ? (
                <TimeSeriesChart rows={rows} series={[{ key: "lag", name: "Watermark lag", color: SERIES.primary, area: true }]} format={(v) => duration(Math.round(v))} />
              ) : (
                <Collecting />
              )}
            </Card>
            <Card title="Backpressure" description="Share of time the most constrained operator waits on downstream." bodyClassName="h-60 px-2 pt-3 pb-1">
              {ready ? (
                <TimeSeriesChart rows={rows} series={[{ key: "pressure", name: "Backpressure", color: SERIES.primary, area: true }]} format={percent} domain={[0, 1]} />
              ) : (
                <Collecting />
              )}
            </Card>
            <Card title="Checkpoint duration" description="Barrier injection to manifest, per checkpoint." bodyClassName="h-60 px-2 pt-3 pb-1">
              {completed ? <CheckpointDurationChart records={records} /> : <EmptyState title="No completed checkpoints yet" />}
            </Card>
          </div>

          <Card title="Operators" description="Current rates per stage; the trend shows output over the last 20 seconds." bodyClassName="overflow-x-auto">
            {latest ? (
              <table className={tableClass}>
                <thead>
                  <tr>
                    <th className={thClass}>Operator</th>
                    <th className={`${thClass} text-right`}>In / s</th>
                    <th className={`${thClass} text-right`}>Out / s</th>
                    <th className={thClass}>Trend</th>
                    <th className={`${thClass} text-right`}>Watermark lag</th>
                    <th className={`${thClass} text-right`}>Backpressure</th>
                  </tr>
                </thead>
                <tbody>
                  {latest.operators.map((op, i) => {
                    const label = parseStage(op.name);
                    return (
                      <tr key={i}>
                        <td className={tdClass}>
                          <span className="font-medium text-slate-900">{label.title}</span>
                          {label.detail && <span className="ml-2 font-mono text-xs text-slate-500">{label.detail}</span>}
                        </td>
                        <td className={`${tdClass} text-right tabular-nums`}>{compact(op.inPerSec)}</td>
                        <td className={`${tdClass} text-right tabular-nums`}>{compact(op.outPerSec)}</td>
                        <td className={tdClass}>
                          <Sparkline values={history.slice(-40).map((s) => s.operators[i]?.outPerSec ?? 0)} width={96} height={20} />
                        </td>
                        <td className={`${tdClass} text-right tabular-nums`}>{op.watermarkLagMs == null ? "–" : duration(Math.round(op.watermarkLagMs))}</td>
                        <td className={`${tdClass} text-right tabular-nums ${op.backpressure >= 0.3 ? "font-semibold text-amber-700" : ""}`}>{percent(op.backpressure)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            ) : (
              <Collecting />
            )}
          </Card>

          <Card title="Checkpoint state" description="Total state size, and the bytes each incremental checkpoint actually wrote." actions={<SeriesLegend series={STATE_SERIES} />} bodyClassName="h-60 px-2 pt-3 pb-1">
            {completed ? <CheckpointStateChart records={records} /> : <EmptyState title="No completed checkpoints yet" />}
          </Card>
          <Card title="Checkpoint history" bodyClassName="max-h-96 overflow-auto">
            {records.length ? <CheckpointTable records={records} /> : <EmptyState title="No checkpoints">Deploy with fault tolerance on to see them here.</EmptyState>}
          </Card>
        </>
      )}
    </Page>
  );
}
