import type { DragEvent } from "react";

import { NODE_LABELS } from "../../lib/validate.ts";
import { useStore } from "../../store/pipelineStore.ts";
import type { NodeKind } from "../../types/pipeline.ts";
import { OperatorIcon } from "./icons.tsx";

export const DRAG_TYPE = "application/sluice-node";

const groups: { title: string; items: { kind: NodeKind; hint: string; disabled?: boolean }[] }[] = [
  { title: "Sources", items: [{ kind: "source", hint: "Read events from a stream" }] },
  {
    title: "Transform",
    items: [
      { kind: "filter", hint: "Keep records matching a condition" },
      { kind: "map", hint: "Keep only selected fields" },
      { kind: "key_by", hint: "Group records by a field" },
    ],
  },
  { title: "Aggregate", items: [{ kind: "window", hint: "Tumbling, sliding or session totals" }] },
  { title: "Output", items: [{ kind: "sink", hint: "Collect results to view" }] },
  { title: "Rust API only", items: [{ kind: "join", hint: "Join two streams by window", disabled: true }] },
];

export function Palette() {
  const addNode = useStore((s) => s.addNode);
  const count = useStore((s) => s.nodes.length);
  const onDragStart = (event: DragEvent, kind: NodeKind) => {
    event.dataTransfer.setData(DRAG_TYPE, kind);
    event.dataTransfer.effectAllowed = "move";
  };
  return (
    <div className="flex flex-col gap-5 p-4">
      <p className="text-[13px] leading-relaxed text-slate-500">Drag operators onto the canvas, or click to add one.</p>
      {groups.map((group) => (
        <div key={group.title}>
          <div className="mb-2 text-[11px] font-semibold tracking-wider text-slate-400 uppercase">{group.title}</div>
          <div className="flex flex-col gap-2">
            {group.items.map(({ kind, hint, disabled }) => (
              <button
                key={kind}
                type="button"
                draggable={!disabled}
                disabled={disabled}
                onDragStart={(e) => onDragStart(e, kind)}
                onClick={() => addNode(kind, { x: 80 + (count % 4) * 70, y: 360 + (count % 3) * 50 })}
                title={hint}
                className="flex cursor-grab items-center gap-3 rounded-xl border border-slate-200 bg-white p-2.5 text-left transition hover:border-slate-300 hover:shadow-md active:cursor-grabbing disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:shadow-none"
              >
                <OperatorIcon kind={kind} />
                <span className="min-w-0">
                  <span className="block text-[13px] font-semibold text-slate-900">{NODE_LABELS[kind]}</span>
                  <span className="block truncate text-xs text-slate-500">{hint}</span>
                </span>
              </button>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
