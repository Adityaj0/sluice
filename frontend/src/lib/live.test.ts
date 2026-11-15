import assert from "node:assert/strict";
import { test } from "node:test";

import type { LiveSnapshot, OperatorMetrics, StageInfo } from "../types/pipeline.ts";
import { health, layoutStages, rates } from "./live.ts";

const stage = (inputs: number[]): StageInfo => ({ uid: "x", is_source: inputs.length === 0, inputs });

test("layout puts each stage one layer after its deepest input", () => {
  // source → window → split ─┬─────────→ union → sink
  //                          └→ late ───┘
  const stages = [stage([]), stage([0]), stage([1]), stage([2]), stage([2, 3]), stage([4])];
  assert.deepEqual(
    layoutStages(stages).map((p) => [p.layer, p.row]),
    [
      [0, 0],
      [1, 0],
      [2, 0],
      [3, 0],
      [4, 0],
      [5, 0],
    ],
  );
  // Two sources side by side in layer 0.
  assert.deepEqual(
    layoutStages([stage([]), stage([]), stage([0, 1])]).map((p) => [p.layer, p.row]),
    [
      [0, 0],
      [0, 1],
      [1, 0],
    ],
  );
});

const op = (records_in: number, records_out: number, extra: Partial<OperatorMetrics> = {}): OperatorMetrics => ({
  id: 0,
  name: "map",
  records_in,
  records_out,
  late_records: 0,
  watermark: null,
  idle: false,
  backpressure_ratio: 0,
  backpressured_ms: 0,
  input_queue: 0,
  input_capacity: 1024,
  ...extra,
});

const snapshot = (ts: number, operators: OperatorMetrics[]): LiveSnapshot => ({
  ts,
  job: { id: 1, name: "j", kind: "pipeline", stream: "purchases", status: { state: "running" }, submitted_at: 0, restarts: 0, checkpoints_completed: 0, latest_checkpoint: null },
  stages: [],
  operators,
  checkpoints: { completed: 0, failed: 0, latest_completed: null, restored_from: null, recent: [] },
});

test("rates are deltas over elapsed time and reset on restart", () => {
  const a = snapshot(1000, [op(100, 50)]);
  const b = snapshot(1500, [op(600, 300)]);
  assert.deepEqual(rates(a, b), [{ inPerSec: 1000, outPerSec: 500 }]);
  assert.deepEqual(rates(undefined, b), [{ inPerSec: 0, outPerSec: 0 }]);
  assert.deepEqual(rates(b, snapshot(2000, [op(10, 5)])), [{ inPerSec: 0, outPerSec: 0 }]);
});

test("health", () => {
  assert.equal(health(op(0, 0), "running"), "healthy");
  assert.equal(health(op(0, 0, { backpressure_ratio: 0.8 }), "running"), "backpressured");
  assert.equal(health(op(0, 0, { idle: true }), "running"), "idle");
  assert.equal(health(op(0, 0), "failed"), "failed");
});
