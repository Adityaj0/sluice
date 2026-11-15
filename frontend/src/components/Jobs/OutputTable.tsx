import { Inbox } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { api } from "../../api.ts";
import { clock } from "../../lib/format.ts";
import type { OutputRecord } from "../../types/pipeline.ts";
import { Badge, EmptyState, tableClass, tdClass, thClass } from "../common/ui.tsx";

const TIME_KEYS = /window_start|window_end|^minute$|^started$|^ended$|^until$|time/;

function cell(key: string, value: unknown) {
  if (value == null) return <span className="text-slate-300">null</span>;
  if (typeof value === "number") {
    if (TIME_KEYS.test(key) && value > 1e12 && value < 1e13) return clock(value);
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  if (typeof value === "object") return <span className="font-mono text-xs">{JSON.stringify(value)}</span>;
  return String(value);
}

export function OutputTable({ jobId }: { jobId: number }) {
  const [rows, setRows] = useState<OutputRecord[] | null>(null);
  useEffect(() => {
    let alive = true;
    const load = () => api.output(jobId, 50).then((r) => alive && setRows(r.reverse())).catch(() => {});
    load();
    const timer = setInterval(load, 2000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [jobId]);

  const columns = useMemo(() => {
    const keys: string[] = [];
    for (const row of rows ?? []) {
      if (row.value && typeof row.value === "object" && !Array.isArray(row.value)) {
        for (const key of Object.keys(row.value as object)) if (key !== "late_event" && !keys.includes(key)) keys.push(key);
      }
    }
    return keys;
  }, [rows]);

  if (!rows) return <div className="p-6 text-sm text-slate-500">Loading…</div>;
  if (!rows.length) {
    return (
      <EmptyState icon={Inbox} title="No results yet">
        Window results appear when their window closes; with checkpointing, once the checkpoint covering them completes.
      </EmptyState>
    );
  }
  return (
    <div className="overflow-auto">
      <table className={tableClass}>
        <thead className="sticky top-0">
          <tr>
            <th className={thClass}>Event time</th>
            {columns.map((c) => (
              <th key={c} className={thClass}>
                {c}
              </th>
            ))}
            {columns.length === 0 && <th className={thClass}>Value</th>}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const value = row.value as Record<string, unknown> | null;
            const late = value && typeof value === "object" && "late_event" in value;
            return (
              <tr key={i} className="hover:bg-slate-50">
                <td className={`${tdClass} text-slate-500 tabular-nums`}>{clock(row.event_time, true)}</td>
                {late ? (
                  <td className={tdClass} colSpan={Math.max(1, columns.length)}>
                    <Badge tone="amber">late event</Badge> <span className="ml-2 font-mono text-xs text-slate-600">{JSON.stringify(value.late_event)}</span>
                  </td>
                ) : columns.length ? (
                  columns.map((c) => (
                    <td key={c} className={`${tdClass} tabular-nums ${c === "key" || c.endsWith("_id") ? "font-medium text-slate-900" : ""}`}>
                      {cell(c, value?.[c])}
                    </td>
                  ))
                ) : (
                  <td className={tdClass}>{cell("value", row.value)}</td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
