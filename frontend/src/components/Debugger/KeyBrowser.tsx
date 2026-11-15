import { Search } from "lucide-react";
import { useMemo, useState } from "react";

import { clock, json } from "../../lib/format.ts";
import type { StateEntry } from "../../types/pipeline.ts";
import { Badge, Input, tableClass, tdClass, thClass } from "../common/ui.tsx";

export type Change = "new" | "changed" | "same";

const entryId = (e: StateEntry) => `${e.key}|${e.window.start}|${e.window.end}`;

/** Compares two checkpoints' entries of the same state. */
export function diffEntries(current: StateEntry[], previous: StateEntry[] | undefined) {
  const before = new Map((previous ?? []).map((e) => [entryId(e), JSON.stringify(e.value)]));
  const now = new Set(current.map(entryId));
  const changes = new Map<string, Change>();
  for (const entry of current) {
    const old = before.get(entryId(entry));
    changes.set(entryId(entry), previous === undefined || old === JSON.stringify(entry.value) ? "same" : old === undefined ? "new" : "changed");
  }
  const removed = (previous ?? []).filter((e) => !now.has(entryId(e)));
  return { changes, removed };
}

function formatValue(value: unknown) {
  if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  if (value && typeof value === "object" && "aggregates" in (value as object)) {
    const v = value as { aggregates: unknown[] };
    return v.aggregates.map((a) => (typeof a === "number" ? a.toLocaleString(undefined, { maximumFractionDigits: 2 }) : json(a))).join(" · ");
  }
  return json(value);
}

export function KeyBrowser({ entries, previous, compared }: { entries: StateEntry[]; previous?: StateEntry[]; compared: number | null }) {
  const [filter, setFilter] = useState("");
  const { changes, removed } = useMemo(() => diffEntries(entries, previous), [entries, previous]);
  const match = (e: StateEntry) => e.key.toLowerCase().includes(filter.toLowerCase());
  const visible = entries.filter(match);
  const counts = { new: 0, changed: 0, same: 0 };
  changes.forEach((c) => (counts[c] += 1));

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative w-64">
          <Search className="pointer-events-none absolute top-2.5 left-3 size-4 text-slate-400" />
          <Input className="pl-9" placeholder="Filter keys" value={filter} onChange={(e) => setFilter(e.target.value)} />
        </div>
        {compared != null && (
          <div className="flex items-center gap-2 text-xs text-slate-500">
            Since #{compared}:
            <span className="text-slate-800">
              <b className="font-semibold">{counts.new}</b> new · <b className="font-semibold">{counts.changed}</b> changed · <b className="font-semibold">{removed.length}</b> removed
            </span>
          </div>
        )}
      </div>
      <div className="max-h-96 overflow-auto rounded-lg ring-1 ring-slate-200">
        <table className={tableClass}>
          <thead className="sticky top-0">
            <tr>
              <th className={thClass}>Key</th>
              <th className={thClass}>Window</th>
              <th className={`${thClass} text-right`}>Value</th>
              <th className={thClass}>State</th>
              <th className={thClass}>Change</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((entry) => {
              const change = changes.get(entryId(entry)) ?? "same";
              return (
                <tr key={entryId(entry)}>
                  <td className={`${tdClass} font-medium text-slate-900`}>{entry.key || "(all)"}</td>
                  <td className={`${tdClass} whitespace-nowrap text-slate-500 tabular-nums`}>
                    {clock(entry.window.start)} – {clock(entry.window.end)}
                  </td>
                  <td className={`${tdClass} text-right font-medium text-slate-900 tabular-nums`}>{formatValue(entry.value)}</td>
                  <td className={`${tdClass} text-slate-500`}>{entry.kind === "session" ? (entry.fired ? "Emitted, open for late data" : "Open session") : "Open window"}</td>
                  <td className={tdClass}>
                    {change === "new" && <Badge tone="indigo">New</Badge>}
                    {change === "changed" && <Badge tone="slate">Changed</Badge>}
                  </td>
                </tr>
              );
            })}
            {removed.filter(match).map((entry) => (
              <tr key={`gone-${entryId(entry)}`} className="text-slate-400">
                <td className={`${tdClass} text-slate-400 line-through`}>{entry.key}</td>
                <td className={`${tdClass} whitespace-nowrap text-slate-400 tabular-nums`}>
                  {clock(entry.window.start)} – {clock(entry.window.end)}
                </td>
                <td className={`${tdClass} text-right text-slate-400 tabular-nums line-through`}>{formatValue(entry.value)}</td>
                <td className={`${tdClass} text-slate-400`}>Window fired</td>
                <td className={tdClass}>
                  <span className="text-xs text-slate-500">Removed</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!visible.length && !removed.length && <div className="p-6 text-center text-[13px] text-slate-500">No entries.</div>}
      </div>
    </div>
  );
}
