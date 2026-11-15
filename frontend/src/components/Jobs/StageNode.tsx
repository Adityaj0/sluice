import { Handle, Position } from "@xyflow/react";
import type { Node, NodeProps } from "@xyflow/react";
import { Zap } from "lucide-react";

import { clock, compact, percent } from "../../lib/format.ts";
import type { Health } from "../../lib/live.ts";
import { parseStage } from "../../lib/stages.ts";
import type { OperatorMetrics, StageInfo } from "../../types/pipeline.ts";
import { Sparkline, cx } from "../common/ui.tsx";
import { STAGE_ICONS } from "./stageIcons.tsx";

export type StageNodeData = {
  stage: StageInfo;
  metrics: OperatorMetrics;
  rate: number;
  history: number[];
  health: Health;
  sink: boolean;
  onKill?: () => void;
};
export type StageFlowNode = Node<StageNodeData, "stage">;

export const healthStyle: Record<Health, { stripe: string; label: string; spark: string }> = {
  healthy: { stripe: "", label: "Healthy", spark: "#2272b4" },
  backpressured: { stripe: "bg-amber-500", label: "Backpressured", spark: "#c27718" },
  idle: { stripe: "", label: "Idle", spark: "#8f9ca9" },
  failed: { stripe: "bg-rose-500", label: "Failed", spark: "#c82d4c" },
};

export function StageNode({ data }: NodeProps<StageFlowNode>) {
  const { metrics, rate, history, health, stage, sink, onKill } = data;
  const label = parseStage(metrics.name);
  const Icon = STAGE_ICONS[label.kind];
  const style = healthStyle[health];
  const total = sink ? metrics.records_in : metrics.records_out;
  return (
    <div className="group relative w-[192px] overflow-hidden rounded-md border border-slate-300 bg-white">
      {style.stripe && <span className={cx("absolute inset-x-0 top-0 h-[3px]", style.stripe)} title={style.label} />}
      {stage.inputs.length > 0 && (
        <>
          <Handle id="l" type="target" position={Position.Left} isConnectable={false} />
          <Handle id="tr" type="target" position={Position.Right} isConnectable={false} />
          <Handle id="t" type="target" position={Position.Top} isConnectable={false} />
        </>
      )}
      <div className="px-3 pt-3 pb-2.5">
        <div className="flex items-center gap-2">
          <Icon className="size-4 shrink-0 text-slate-500" strokeWidth={1.75} />
          <span className="truncate text-[13px] font-semibold text-slate-900">{label.title}</span>
          {onKill && (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onKill();
              }}
              title="Crash this stage: the job restarts from its latest checkpoint"
              className="ml-auto hidden items-center gap-1 rounded-sm border border-rose-300 bg-white px-1.5 py-px text-[11px] font-medium text-rose-600 group-hover:flex hover:bg-rose-50"
            >
              <Zap className="size-3" />
              Kill
            </button>
          )}
        </div>
        <div className="mt-1 h-4 truncate font-mono text-[11px] text-slate-500" title={label.detail}>
          {label.detail}
        </div>
        <div className="mt-2 flex items-end justify-between gap-2">
          <div>
            <div className="text-[18px] leading-none font-semibold text-slate-900 tabular-nums">
              {compact(rate)}
              <span className="ml-0.5 text-xs font-normal text-slate-500">/s</span>
            </div>
            <div className="mt-1 text-[11px] text-slate-500">{compact(total)} records</div>
          </div>
          <Sparkline values={history} width={64} height={26} color={style.spark} />
        </div>
        <div className="mt-2 flex items-center justify-between gap-2 border-t border-slate-200 pt-1.5 text-[11px] whitespace-nowrap text-slate-500">
          <span title="Event-time watermark">{metrics.idle ? "idle" : `wm ${clock(metrics.watermark)}`}</span>
          {metrics.backpressure_ratio >= 0.05 ? (
            <span className="font-medium text-amber-700">{percent(metrics.backpressure_ratio)} backpressured</span>
          ) : metrics.late_records > 0 ? (
            <span className="font-medium text-amber-700">{compact(metrics.late_records)} late</span>
          ) : metrics.latency_ms ? (
            <span>p99 ≤{metrics.latency_ms.p99} ms</span>
          ) : (
            <span>{metrics.input_capacity ? `buffer ${metrics.input_queue}/${metrics.input_capacity}` : ""}</span>
          )}
        </div>
      </div>
      {!sink && (
        <>
          <Handle id="r" type="source" position={Position.Right} isConnectable={false} />
          <Handle id="sl" type="source" position={Position.Left} isConnectable={false} />
          <Handle id="b" type="source" position={Position.Bottom} isConnectable={false} />
        </>
      )}
    </div>
  );
}
