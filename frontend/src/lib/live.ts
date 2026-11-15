// Live-view helpers: laying out a running job's stage graph and turning
// cumulative counters into rates and health states.

import type { LiveSnapshot, OperatorMetrics, StageInfo } from "../types/pipeline.ts";

export interface Placed {
  id: number;
  layer: number;
  row: number;
}

/** Layers by longest path from a source; rows within a layer in order. */
export function layoutStages(stages: StageInfo[]): Placed[] {
  const layer = new Array<number>(stages.length).fill(0);
  // Stages are constructed after their inputs, so one pass in order works.
  stages.forEach((stage, i) => {
    layer[i] = stage.inputs.length ? Math.max(...stage.inputs.map((input) => layer[input] + 1)) : 0;
  });
  const rows = new Map<number, number>();
  return stages.map((_, id) => {
    const row = rows.get(layer[id]) ?? 0;
    rows.set(layer[id], row + 1);
    return { id, layer: layer[id], row };
  });
}

export interface OperatorRates {
  inPerSec: number;
  outPerSec: number;
}

/** Records/second between two snapshots of the same attempt. */
export function rates(previous: LiveSnapshot | undefined, current: LiveSnapshot): OperatorRates[] {
  return current.operators.map((op, i) => {
    const before = previous?.operators[i];
    const seconds = previous ? (current.ts - previous.ts) / 1000 : 0;
    // A restart resets counters: treat a decrease as a fresh start.
    if (!before || before.name !== op.name || seconds <= 0 || op.records_in < before.records_in) {
      return { inPerSec: 0, outPerSec: 0 };
    }
    return {
      inPerSec: (op.records_in - before.records_in) / seconds,
      outPerSec: (op.records_out - before.records_out) / seconds,
    };
  });
}

export type Health = "healthy" | "backpressured" | "idle" | "failed";

/** Node color: red when the job failed, yellow when mostly waiting on downstream. */
export function health(op: OperatorMetrics, jobState: string): Health {
  if (jobState === "failed") return "failed";
  if (op.backpressure_ratio >= 0.3) return "backpressured";
  if (op.idle) return "idle";
  return "healthy";
}
