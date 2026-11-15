import { bytes, clock, duration } from "../../lib/format.ts";
import type { CheckpointRecord } from "../../types/pipeline.ts";
import { Badge, tableClass, tdClass, thClass } from "../common/ui.tsx";
import { BarSeriesChart, SERIES, TimeSeriesChart } from "./charts.tsx";
import type { Series } from "./charts.tsx";

export function CheckpointTable({ records }: { records: CheckpointRecord[] }) {
  return (
    <table className={tableClass}>
      <thead className="sticky top-0">
        <tr>
          <th className={thClass}>Checkpoint</th>
          <th className={thClass}>Started</th>
          <th className={`${thClass} text-right`}>Duration</th>
          <th className={`${thClass} text-right`}>State</th>
          <th className={`${thClass} text-right`} title="Bytes this checkpoint wrote; the rest was reused from earlier checkpoints">
            Written
          </th>
          <th className={`${thClass} text-right`}>Max alignment</th>
          <th className={thClass}>Status</th>
        </tr>
      </thead>
      <tbody>
        {[...records].reverse().map((r) => {
          const align = Math.max(0, ...r.stages.map((s) => s.alignment_ms));
          const share = r.state_size ? Math.round((r.uploaded_bytes / r.state_size) * 100) : 100;
          const done = r.status.state === "completed";
          return (
            <tr key={r.id}>
              <td className={`${tdClass} font-medium text-slate-900`}>#{r.id}</td>
              <td className={`${tdClass} text-slate-500 tabular-nums`}>{clock(r.triggered_at, true)}</td>
              <td className={`${tdClass} text-right tabular-nums`}>{duration(r.duration_ms)}</td>
              <td className={`${tdClass} text-right tabular-nums`}>{done ? bytes(r.state_size) : "–"}</td>
              <td className={`${tdClass} text-right tabular-nums`}>
                {done ? (
                  <>
                    {bytes(r.uploaded_bytes)} <span className="text-slate-400">({share}%)</span>
                  </>
                ) : (
                  "–"
                )}
              </td>
              <td className={`${tdClass} text-right tabular-nums`}>{r.stages.length ? `${align} ms` : "–"}</td>
              <td className={tdClass}>
                {r.status.state === "completed" && <Badge tone="emerald">Completed</Badge>}
                {r.status.state === "in_progress" && <Badge tone="slate">In progress</Badge>}
                {r.status.state === "failed" && (
                  <span title={r.status.reason}>
                    <Badge tone="rose">Failed</Badge>
                    <span className="ml-2 text-xs text-slate-500">{r.status.reason}</span>
                  </span>
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

/** How long each checkpoint took, barrier to manifest. */
export function CheckpointDurationChart({ records }: { records: CheckpointRecord[] }) {
  const rows = records
    .filter((r) => r.status.state === "completed")
    .slice(-60)
    .map((r) => ({ id: r.id, duration: r.duration_ms ?? 0 }));
  return (
    <BarSeriesChart
      rows={rows}
      xKey="id"
      series={{ key: "duration", name: "Duration", color: SERIES.primary }}
      format={(v) => duration(Math.round(v))}
      formatLabel={(id) => `Checkpoint #${id}`}
    />
  );
}

export const STATE_SERIES: Series[] = [
  { key: "size", name: "State size", color: SERIES.primary },
  { key: "written", name: "Newly written", color: SERIES.secondary, dashed: true },
];

/** Total state per checkpoint, and the part each one actually wrote (incremental). */
export function CheckpointStateChart({ records }: { records: CheckpointRecord[] }) {
  const rows = records
    .filter((r) => r.status.state === "completed")
    .slice(-60)
    .map((r) => ({ ts: r.triggered_at, size: r.state_size, written: r.uploaded_bytes }));
  return <TimeSeriesChart rows={rows} series={STATE_SERIES} format={(v) => bytes(v)} />;
}
