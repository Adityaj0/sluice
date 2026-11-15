import { Handle, Position } from "@xyflow/react";
import type { NodeProps } from "@xyflow/react";
import { AlertCircle } from "lucide-react";
import { createContext, useContext } from "react";

import { NODE_LABELS, describe } from "../../lib/validate.ts";
import type { BuilderNode } from "../../store/pipelineStore.ts";
import { cx } from "../common/ui.tsx";
import { OperatorIcon } from "./icons.tsx";

/** Validation messages by node id, provided by the canvas. */
export const IssuesContext = createContext<Map<string, string[]>>(new Map());

export function OperatorNode({ id, data, selected }: NodeProps<BuilderNode>) {
  const issues = useContext(IssuesContext).get(id) ?? [];
  return (
    <div
      className={cx(
        "w-[260px] rounded-xl border bg-white transition-shadow",
        issues.length ? "border-rose-300" : selected ? "border-indigo-500 ring-4 ring-indigo-500/10" : "border-slate-200 hover:shadow-md",
      )}
      title={issues.join("\n")}
    >
      {data.kind !== "source" && <Handle type="target" position={Position.Top} />}
      <div className="flex items-center gap-3 p-3">
        <OperatorIcon kind={data.kind} />
        <div className="min-w-0">
          <div className="text-[13px] font-semibold text-slate-900">{NODE_LABELS[data.kind]}</div>
          <div className="truncate font-mono text-[11px] text-slate-500">{describe(data)}</div>
        </div>
      </div>
      {issues.length > 0 && (
        <div className="flex items-center gap-1.5 border-t border-rose-100 bg-rose-50/60 px-3 py-1.5 text-[11px] font-medium text-rose-700">
          <AlertCircle className="size-3.5" />
          {issues.length === 1 ? issues[0].replace(/^[^:]+: /, "") : `${issues.length} issues`}
        </div>
      )}
      {data.kind !== "sink" && <Handle type="source" position={Position.Bottom} />}
    </div>
  );
}
