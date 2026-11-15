import { ChevronLeft, ChevronRight } from "lucide-react";

import { bytes, clock, duration } from "../../lib/format.ts";
import type { CheckpointRecord } from "../../types/pipeline.ts";
import { Button, cx } from "../common/ui.tsx";

function tone(record: CheckpointRecord, retained: boolean) {
  if (record.status.state === "failed") return "bg-rose-400";
  if (record.status.state === "in_progress") return "bg-slate-300 animate-pulse";
  return retained ? "bg-indigo-500 hover:bg-indigo-600" : "bg-indigo-200";
}

/** A scrubber over the checkpoint history; retained checkpoints can be opened. */
export function Timeline({
  records,
  retained,
  selected,
  onSelect,
}: {
  records: CheckpointRecord[];
  retained: number[];
  selected: number | null;
  onSelect: (id: number) => void;
}) {
  const shown = records.slice(-90);
  const position = selected == null ? -1 : retained.indexOf(selected);
  const step = (delta: number) => {
    const next = retained[position + delta];
    if (next != null) onSelect(next);
  };
  const maxSize = Math.max(1, ...shown.map((r) => r.state_size));
  return (
    <div className="space-y-4">
      <div className="flex h-16 items-end gap-[3px] overflow-x-auto rounded-lg bg-slate-50 px-3 pt-2 pb-2 ring-1 ring-slate-200">
        {shown.map((record) => {
          const isRetained = retained.includes(record.id);
          const isSelected = record.id === selected;
          const height = 12 + (record.state_size / maxSize) * 36;
          return (
            <button
              key={record.id}
              type="button"
              disabled={!isRetained}
              onClick={() => onSelect(record.id)}
              style={{ height }}
              title={[
                `Checkpoint #${record.id} · ${record.status.state.replace("_", " ")}`,
                record.status.state === "failed" ? record.status.reason : null,
                `at ${clock(record.triggered_at, true)} · took ${duration(record.duration_ms)}`,
                `state ${bytes(record.state_size)} (${bytes(record.uploaded_bytes)} written)`,
                isRetained ? "Click to inspect" : "No longer on disk",
              ]
                .filter(Boolean)
                .join("\n")}
              className={cx(
                "w-2.5 shrink-0 rounded-sm transition-all",
                tone(record, isRetained),
                isSelected && "!bg-indigo-700 ring-2 ring-indigo-500 ring-offset-2",
                !isRetained && "cursor-default",
              )}
            />
          );
        })}
      </div>
      <div className="flex items-center gap-3">
        <Button size="sm" icon={ChevronLeft} onClick={() => step(-1)} disabled={position <= 0}>
          Older
        </Button>
        <input
          type="range"
          min={0}
          max={Math.max(0, retained.length - 1)}
          value={Math.max(0, position)}
          disabled={retained.length < 2}
          onChange={(e) => onSelect(retained[Number(e.target.value)])}
          className="flex-1 accent-indigo-600"
          aria-label="Checkpoint"
        />
        <Button size="sm" onClick={() => step(1)} disabled={position < 0 || position >= retained.length - 1}>
          Newer <ChevronRight className="size-3.5" />
        </Button>
      </div>
      <div className="flex flex-wrap gap-5 text-xs text-slate-500">
        <span className="flex items-center gap-1.5">
          <span className="size-2.5 rounded-sm bg-indigo-500" />
          On disk, can be inspected
        </span>
        <span className="flex items-center gap-1.5">
          <span className="size-2.5 rounded-sm bg-indigo-200" />
          Completed, cleaned up
        </span>
        <span className="flex items-center gap-1.5">
          <span className="size-2.5 rounded-sm bg-rose-400" />
          Failed
        </span>
        <span>Bar height shows state size</span>
      </div>
    </div>
  );
}
