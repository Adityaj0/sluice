import { MousePointerClick, Trash2 } from "lucide-react";
import type { ReactNode } from "react";

import { NODE_LABELS, describe } from "../../lib/validate.ts";
import { useStore } from "../../store/pipelineStore.ts";
import type { AggregateFn, CompareOp, NodeData, WindowKind } from "../../types/pipeline.ts";
import { Button, Field, Input, Segmented, Select, Toggle } from "../common/ui.tsx";
import { OperatorIcon } from "./icons.tsx";

function Num({ value, onChange, min = 0 }: { value: number; onChange: (v: number) => void; min?: number }) {
  return (
    <Input
      type="number"
      min={min}
      className="tabular-nums"
      value={Number.isFinite(value) ? value : ""}
      onChange={(e) => onChange(e.target.value === "" ? NaN : Number(e.target.value))}
    />
  );
}

function NodeForm({ data, set }: { data: NodeData; set: (patch: Partial<NodeData>) => void }) {
  const rows: ReactNode[] = [];
  switch (data.kind) {
    case "source":
      rows.push(
        <Field key="s" label="Stream" hint="Events sent to this stream feed the job.">
          <Input value={data.stream} onChange={(e) => set({ stream: e.target.value })} />
        </Field>,
        <Field key="o" label="Max out-of-order (ms)" hint="How late events may arrive. The watermark trails the newest event by this much.">
          <Num value={data.maxOutOfOrdernessMs} onChange={(maxOutOfOrdernessMs) => set({ maxOutOfOrdernessMs })} />
        </Field>,
        <Field key="i" label="Idle after (ms)" hint="Mark the stream idle after this long without events. Leave empty to never.">
          <Input
            type="number"
            min={1}
            value={data.idleTimeoutMs ?? ""}
            placeholder="never"
            onChange={(e) => set({ idleTimeoutMs: e.target.value === "" ? null : Number(e.target.value) })}
          />
        </Field>,
      );
      break;
    case "filter":
      rows.push(
        <Field key="f" label="Field" hint="Use dots for nested fields, e.g. order.total.">
          <Input value={data.field} onChange={(e) => set({ field: e.target.value })} />
        </Field>,
        <div key="ov" className="grid grid-cols-[110px_1fr] gap-3">
          <Field label="Operator">
            <Select value={data.op} onChange={(e) => set({ op: e.target.value as CompareOp })}>
              <option value="eq">=</option>
              <option value="ne">≠</option>
              <option value="gt">&gt;</option>
              <option value="gte">≥</option>
              <option value="lt">&lt;</option>
              <option value="lte">≤</option>
            </Select>
          </Field>
          <Field label="Value">
            <Input value={data.value} onChange={(e) => set({ value: e.target.value })} />
          </Field>
        </div>,
        <p key="h" className="text-xs text-slate-500">
          Numbers and true/false are typed. Wrap text in quotes to compare it as a string: <code className="rounded bg-slate-100 px-1">'book'</code>.
        </p>,
      );
      break;
    case "map":
      rows.push(
        <Field key="s" label="Fields to keep" hint="Comma-separated. Other fields are dropped.">
          <Input value={data.select} onChange={(e) => set({ select: e.target.value })} />
        </Field>,
      );
      break;
    case "key_by":
      rows.push(
        <Field key="f" label="Group by field" hint="Windows after this keep separate totals per value.">
          <Input value={data.field} onChange={(e) => set({ field: e.target.value })} />
        </Field>,
      );
      break;
    case "window":
      rows.push(
        <Field key="k" label="Window type">
          <Segmented<WindowKind>
            value={data.windowKind}
            onChange={(windowKind) => set({ windowKind })}
            options={[
              { id: "tumbling", label: "Tumbling" },
              { id: "sliding", label: "Sliding" },
              { id: "session", label: "Session" },
            ]}
          />
        </Field>,
        <p key="kd" className="-mt-2 text-xs text-slate-500">
          {data.windowKind === "tumbling" && "Fixed, back-to-back windows: each event counts once."}
          {data.windowKind === "sliding" && "Overlapping windows that start every slide: each event counts in several."}
          {data.windowKind === "session" && "One window per burst of activity; it closes after a quiet gap."}
        </p>,
      );
      if (data.windowKind === "session") {
        rows.push(
          <Field key="g" label="Gap (ms)" hint="The session closes after this long without events for its key.">
            <Num value={data.gapMs} onChange={(gapMs) => set({ gapMs })} min={1} />
          </Field>,
        );
      } else {
        rows.push(
          <div key="sz" className="grid grid-cols-2 gap-3">
            <Field label="Size (ms)">
              <Num value={data.sizeMs} onChange={(sizeMs) => set({ sizeMs })} min={1} />
            </Field>
            {data.windowKind === "sliding" && (
              <Field label="Slide (ms)">
                <Num value={data.slideMs} onChange={(slideMs) => set({ slideMs })} min={1} />
              </Field>
            )}
          </div>,
        );
      }
      rows.push(
        <div key="a" className="grid grid-cols-2 gap-3">
          <Field label="Aggregate">
            <Select value={data.aggregateFn} onChange={(e) => set({ aggregateFn: e.target.value as AggregateFn })}>
              <option value="count">Count</option>
              <option value="sum">Sum</option>
              <option value="avg">Average</option>
              <option value="min">Minimum</option>
              <option value="max">Maximum</option>
            </Select>
          </Field>
          <Field label="Of field">
            <Input
              value={data.aggregateField}
              disabled={data.aggregateFn === "count"}
              placeholder={data.aggregateFn === "count" ? "all records" : "amount"}
              onChange={(e) => set({ aggregateField: e.target.value })}
            />
          </Field>
        </div>,
        <Field key="l" label="Allowed lateness (ms)" hint="Late events within this re-emit the window with an updated result.">
          <Num value={data.allowedLatenessMs} onChange={(allowedLatenessMs) => set({ allowedLatenessMs })} />
        </Field>,
        <Toggle
          key="e"
          checked={data.lateEvents === "emit"}
          onChange={(v) => set({ lateEvents: v ? "emit" : "drop" })}
          label="Send later events to the output"
          description="Otherwise they are dropped and counted."
        />,
      );
      break;
    case "sink":
      rows.push(
        <Field key="c" label="Keep the last N results">
          <Num value={data.capacity} onChange={(capacity) => set({ capacity })} min={1} />
        </Field>,
      );
      break;
    case "join":
      rows.push(
        <p key="j" className="text-[13px] leading-relaxed text-slate-600">
          Windowed stream-stream joins run in the Rust API (<code className="rounded bg-slate-100 px-1 text-xs">left.join(right).on(..).window(..)</code>).
          Jobs deployed from here read a single stream, so this operator can't be deployed yet.
        </p>,
      );
  }
  return <div className="space-y-4">{rows}</div>;
}

export function JobSettingsForm() {
  const settings = useStore((s) => s.settings);
  const setSettings = useStore((s) => s.setSettings);
  return (
    <div className="space-y-4">
      <Toggle
        checked={settings.checkpointing}
        onChange={(checkpointing) => setSettings({ checkpointing })}
        label="Fault tolerance"
        description="Checkpoint state, recover from crashes, exactly-once output."
      />
      {settings.checkpointing && (
        <div className="space-y-4 rounded-xl bg-slate-50 p-4 ring-1 ring-slate-200">
          <div className="grid grid-cols-2 gap-3">
            <Field label="Checkpoint every (ms)">
              <Num value={settings.intervalMs} onChange={(intervalMs) => setSettings({ intervalMs })} min={10} />
            </Field>
            <Field label="State backend">
              <Select value={settings.backend} onChange={(e) => setSettings({ backend: e.target.value as "rocksdb" | "memory" })}>
                <option value="rocksdb">RocksDB</option>
                <option value="memory">Memory</option>
              </Select>
            </Field>
            <Field label="Keep checkpoints">
              <Num value={settings.retained} onChange={(retained) => setSettings({ retained })} min={1} />
            </Field>
            <Field label="Max restarts">
              <Num value={settings.maxRestarts} onChange={(maxRestarts) => setSettings({ maxRestarts })} />
            </Field>
          </div>
        </div>
      )}
    </div>
  );
}

export function OperatorInspector() {
  const selected = useStore((s) => s.nodes.find((n) => n.id === s.selected));
  const updateNode = useStore((s) => s.updateNode);
  const removeNode = useStore((s) => s.removeNode);
  if (!selected) {
    return (
      <div className="flex flex-col items-center py-10 text-center">
        <div className="mb-3 grid size-10 place-items-center rounded-xl bg-slate-100 text-slate-500">
          <MousePointerClick className="size-5" />
        </div>
        <div className="text-sm font-semibold text-slate-900">Select an operator</div>
        <p className="mt-1 max-w-56 text-[13px] text-slate-500">Click a node on the canvas to configure it.</p>
      </div>
    );
  }
  const set = (patch: Partial<NodeData>) => updateNode(selected.id, { ...selected.data, ...patch } as NodeData);
  return (
    <div className="space-y-5">
      <div className="flex items-center gap-3">
        <OperatorIcon kind={selected.data.kind} size="lg" />
        <div className="min-w-0 flex-1">
          <div className="text-sm font-semibold text-slate-900">{NODE_LABELS[selected.data.kind]}</div>
          <div className="truncate font-mono text-xs text-slate-500">{describe(selected.data)}</div>
        </div>
        <Button size="sm" variant="ghost" icon={Trash2} onClick={() => removeNode(selected.id)} aria-label="Remove operator" />
      </div>
      <NodeForm data={selected.data} set={set} />
    </div>
  );
}
