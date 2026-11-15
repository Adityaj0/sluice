// Types mirroring the Sluice engine's REST and WebSocket API.

export type JobState = "running" | "finished" | "failed" | "cancelled";

export interface JobStatus {
  state: JobState;
  error?: string;
}

export interface JobSummary {
  id: number;
  name: string;
  status: JobStatus;
  submitted_at: number;
  restarts: number;
  checkpoints_completed: number;
  latest_checkpoint: number | null;
  kind: "sql" | "pipeline";
  stream: string;
}

export interface StreamInfo {
  name: string;
  subscribers: number;
  next_offset: number;
  retained: number;
}

export interface GeneratorSpec {
  stream: string;
  rate: number;
  users: number;
  max_disorder_ms: number;
}

export interface GeneratorInfo extends GeneratorSpec {
  id: number;
  sent: number;
  started_at: number;
  waiting: boolean;
}

export interface StageInfo {
  uid: string;
  is_source: boolean;
  /** Indices of the stages feeding this one. */
  inputs: number[];
}

export interface LatencyPercentiles {
  samples: number;
  p50: number;
  p95: number;
  p99: number;
}

export interface OperatorMetrics {
  id: number;
  name: string;
  records_in: number;
  records_out: number;
  late_records: number;
  watermark: number | null;
  idle: boolean;
  backpressure_ratio: number;
  backpressured_ms: number;
  input_queue: number;
  input_capacity: number;
  latency_ms?: LatencyPercentiles;
}

export interface StageCheckpointStats {
  stage: string;
  alignment_ms: number;
  sync_ms: number;
  async_ms: number;
  state_size: number;
  uploaded_bytes: number;
}

export type CheckpointStatus =
  | { state: "in_progress" }
  | { state: "completed" }
  | { state: "failed"; reason: string };

export interface CheckpointRecord {
  id: number;
  status: CheckpointStatus;
  triggered_at: number;
  duration_ms: number | null;
  state_size: number;
  uploaded_bytes: number;
  stages: StageCheckpointStats[];
}

export interface CheckpointsView {
  completed: number;
  failed: number;
  latest_completed: number | null;
  restored_from: number | null;
  records: CheckpointRecord[];
  /** Completed checkpoints still on disk (inspectable). */
  retained: number[];
}

export interface LiveSnapshot {
  ts: number;
  job: JobSummary;
  stages: StageInfo[];
  operators: OperatorMetrics[];
  checkpoints: {
    completed: number;
    failed: number;
    latest_completed: number | null;
    restored_from: number | null;
    recent: CheckpointRecord[];
  };
}

export interface JobDetails extends JobSummary {
  definition: JobDefinition;
  checkpoint_dir: string | null;
  stages: StageInfo[];
  operators: OperatorMetrics[];
  failures: string[];
}

export interface OutputRecord {
  key: string | null;
  event_time: number;
  value: unknown;
}

// ---- Job definitions -------------------------------------------------------

export type CompareOp = "eq" | "ne" | "gt" | "gte" | "lt" | "lte";
export type AggregateFn = "count" | "sum" | "avg" | "min" | "max";
export type WindowKind = "tumbling" | "sliding" | "session";
export type StateBackend = "rocksdb" | "memory";

export type WindowDef =
  | { kind: "tumbling"; size_ms: number }
  | { kind: "sliding"; size_ms: number; slide_ms: number }
  | { kind: "session"; gap_ms: number };

export type OperatorDef =
  | { type: "filter"; field: string; op: CompareOp; value: unknown }
  | { type: "map"; select: string[] }
  | { type: "key_by"; field: string }
  | {
      type: "window";
      window: WindowDef;
      aggregate: { fn: AggregateFn; field?: string };
      allowed_lateness_ms?: number;
      late_events?: "drop" | "emit";
    };

export interface JobDefinition {
  name: string;
  source: { type: "grpc"; stream: string; max_out_of_orderness_ms?: number; idle_timeout_ms?: number };
  operators?: OperatorDef[];
  query?: string;
  sink?: { type: "buffer"; capacity?: number; log?: boolean };
  checkpointing?: { interval_ms?: number; timeout_ms?: number; retained?: number; backend?: StateBackend };
  restart?: { max_restarts?: number; delay_ms?: number };
}

// ---- Pipeline builder ------------------------------------------------------

export type NodeData =
  | { kind: "source"; stream: string; maxOutOfOrdernessMs: number; idleTimeoutMs: number | null }
  | { kind: "filter"; field: string; op: CompareOp; value: string }
  | { kind: "map"; select: string }
  | { kind: "key_by"; field: string }
  | {
      kind: "window";
      windowKind: WindowKind;
      sizeMs: number;
      slideMs: number;
      gapMs: number;
      aggregateFn: AggregateFn;
      aggregateField: string;
      allowedLatenessMs: number;
      lateEvents: "drop" | "emit";
    }
  | { kind: "sink"; capacity: number }
  | { kind: "join" };

export type NodeKind = NodeData["kind"];

export interface JobSettings {
  name: string;
  checkpointing: boolean;
  intervalMs: number;
  backend: StateBackend;
  retained: number;
  maxRestarts: number;
  restartDelayMs: number;
}

/** The builder graph, independent of React Flow. */
export interface BuilderGraph {
  nodes: { id: string; data: NodeData }[];
  edges: { source: string; target: string }[];
}

// ---- State inspection ------------------------------------------------------

export interface StateEntry {
  kind: "pane" | "session";
  window: { start: number; end: number };
  key: string;
  fired: boolean | null;
  value: unknown;
  accumulator?: unknown;
}

export interface InspectedStage {
  id: number;
  uid: string;
  name: string;
  size: number;
  operator_state?: {
    source?: { position: number; max_seen: number | null; last_watermark: number | null };
    window?: { watermark: number | null; fired_through: number | null; late_records: number };
    bytes?: number;
  };
  keyed_state?: {
    backend: StateBackend;
    states: { name: string; kind: string; total: number; truncated: boolean; internal: boolean; entries: StateEntry[] }[];
  };
}

export interface InspectedCheckpoint {
  checkpoint: number;
  triggered_at: number;
  completed_at: number;
  size: number;
  stages: InspectedStage[];
}

// ---- SQL -------------------------------------------------------------------

export interface PlanNode {
  node: string;
  detail: string;
  input: PlanNode | null;
}

export interface ExplainResult {
  stream: string;
  logical: PlanNode;
  optimized: PlanNode;
  optimizations: string[];
  stages: { kind: string; name: string }[];
  explain: string;
}

export type SqlError =
  | { kind: "syntax"; line: number; column: number; message: string }
  | { kind: "plan"; message: string };
