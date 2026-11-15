// Topology validation and serialization for the pipeline builder.
//
// The engine runs linear pipelines: Source → operators → Sink. The builder
// lets users draw any graph, so before submitting we check that it is one:
// no cycles, no fan-in/fan-out, every operator on the path from the Source
// to the Sink, a Key By before every Window, and complete configuration.

import type { BuilderGraph, JobDefinition, JobSettings, NodeData, OperatorDef } from "../types/pipeline.ts";

export interface Issue {
  nodeId?: string;
  message: string;
}

export interface Validation {
  issues: Issue[];
  /** Node ids from Source to Sink, when the graph is valid. */
  chain: string[] | null;
}

export const NODE_LABELS: Record<NodeData["kind"], string> = {
  source: "Source",
  filter: "Filter",
  map: "Map",
  key_by: "Key By",
  window: "Window",
  sink: "Sink",
  join: "Join",
};

function configIssues(id: string, data: NodeData): Issue[] {
  const issue = (message: string) => [{ nodeId: id, message: `${NODE_LABELS[data.kind]}: ${message}` }];
  switch (data.kind) {
    case "source":
      if (!data.stream.trim()) return issue("set the stream name");
      if (data.maxOutOfOrdernessMs < 0) return issue("out-of-orderness cannot be negative");
      return [];
    case "filter":
      return data.field.trim() ? [] : issue("set the field to test");
    case "map":
      return data.select.split(",").some((f) => f.trim()) ? [] : issue("list at least one field to keep");
    case "key_by":
      return data.field.trim() ? [] : issue("set the key field");
    case "window":
      if (data.windowKind === "session") {
        if (!(data.gapMs > 0)) return issue("session gap must be positive");
      } else {
        if (!(data.sizeMs > 0)) return issue("window size must be positive");
        if (data.windowKind === "sliding" && !(data.slideMs > 0 && data.slideMs <= data.sizeMs)) {
          return issue("slide must be positive and no larger than the size");
        }
      }
      if (data.aggregateFn !== "count" && !data.aggregateField.trim()) return issue(`set the field to ${data.aggregateFn}`);
      if (data.allowedLatenessMs < 0) return issue("allowed lateness cannot be negative");
      return [];
    case "sink":
      return data.capacity > 0 ? [] : issue("capacity must be positive");
    case "join":
      return issue("needs two inputs; windowed joins are available in the Rust API, not in submitted jobs yet");
  }
}

/** Node ids that lie on a cycle (Kahn's algorithm leaves them unsorted). */
export function findCycle(graph: BuilderGraph): Set<string> {
  const indegree = new Map(graph.nodes.map((n) => [n.id, 0]));
  for (const e of graph.edges) indegree.set(e.target, (indegree.get(e.target) ?? 0) + 1);
  const queue = [...indegree].filter(([, d]) => d === 0).map(([id]) => id);
  const sorted = new Set<string>();
  while (queue.length) {
    const id = queue.shift()!;
    sorted.add(id);
    for (const e of graph.edges) {
      if (e.source !== id) continue;
      const d = (indegree.get(e.target) ?? 0) - 1;
      indegree.set(e.target, d);
      if (d === 0) queue.push(e.target);
    }
  }
  return new Set(graph.nodes.map((n) => n.id).filter((id) => !sorted.has(id)));
}

export function validate(graph: BuilderGraph): Validation {
  const issues: Issue[] = [];
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  if (graph.nodes.length === 0) {
    return { issues: [{ message: "Drag a Source onto the canvas to start." }], chain: null };
  }

  const sources = graph.nodes.filter((n) => n.data.kind === "source");
  const sinks = graph.nodes.filter((n) => n.data.kind === "sink");
  if (sources.length === 0) issues.push({ message: "Add a Source." });
  sources.slice(1).forEach((n) => issues.push({ nodeId: n.id, message: "Only one Source per job." }));
  if (sinks.length === 0) issues.push({ message: "Add a Sink." });
  sinks.slice(1).forEach((n) => issues.push({ nodeId: n.id, message: "Only one Sink per job." }));
  for (const node of graph.nodes) issues.push(...configIssues(node.id, node.data));

  const cycle = findCycle(graph);
  if (cycle.size) cycle.forEach((id) => issues.push({ nodeId: id, message: "This operator is part of a cycle." }));

  const outgoing = new Map<string, string[]>();
  const incoming = new Map<string, string[]>();
  for (const e of graph.edges) {
    outgoing.set(e.source, [...(outgoing.get(e.source) ?? []), e.target]);
    incoming.set(e.target, [...(incoming.get(e.target) ?? []), e.source]);
  }
  for (const node of graph.nodes) {
    const label = NODE_LABELS[node.data.kind];
    if ((incoming.get(node.id)?.length ?? 0) > 1 && node.data.kind !== "join") {
      issues.push({ nodeId: node.id, message: `${label} has several inputs; only Join merges streams.` });
    }
    if ((outgoing.get(node.id)?.length ?? 0) > 1) {
      issues.push({ nodeId: node.id, message: `${label} feeds several operators; pipelines cannot fan out yet.` });
    }
    if (node.data.kind === "source" && incoming.has(node.id)) issues.push({ nodeId: node.id, message: "A Source cannot have inputs." });
    if (node.data.kind === "sink" && outgoing.has(node.id)) issues.push({ nodeId: node.id, message: "A Sink cannot have outputs." });
  }

  // Walk from the source; everything must be on that path.
  let chain: string[] = [];
  if (sources.length === 1 && cycle.size === 0) {
    let current: string | undefined = sources[0].id;
    while (current && !chain.includes(current)) {
      chain.push(current);
      current = outgoing.get(current)?.[0];
    }
    const last = byId.get(chain[chain.length - 1]);
    if (last && last.data.kind !== "sink") {
      issues.push({ nodeId: last.id, message: `${NODE_LABELS[last.data.kind]} does not lead to the Sink: connect it.` });
    }
    for (const node of graph.nodes) {
      if (!chain.includes(node.id)) {
        issues.push({ nodeId: node.id, message: `${NODE_LABELS[node.data.kind]} is not connected to the Source → Sink path.` });
      }
    }
    let keyed = false;
    for (const id of chain) {
      const data = byId.get(id)!.data;
      if (data.kind === "key_by") keyed = true;
      if (data.kind === "window" && !keyed) issues.push({ nodeId: id, message: "Window needs a Key By before it." });
    }
  }

  return { issues, chain: issues.length === 0 ? chain : null };
}

/** `10` → 10, `true` → true, `'a b'` or anything else → string. */
export function parseLiteral(text: string): unknown {
  const t = text.trim();
  if (/^-?\d+(\.\d+)?$/.test(t)) return Number(t);
  if (t === "true" || t === "false") return t === "true";
  if (t === "null") return null;
  const quoted = t.match(/^'(.*)'$|^"(.*)"$/);
  return quoted ? (quoted[1] ?? quoted[2]) : t;
}

function operatorDef(data: NodeData): OperatorDef | null {
  switch (data.kind) {
    case "filter":
      return { type: "filter", field: data.field.trim(), op: data.op, value: parseLiteral(data.value) };
    case "map":
      return { type: "map", select: data.select.split(",").map((f) => f.trim()).filter(Boolean) };
    case "key_by":
      return { type: "key_by", field: data.field.trim() };
    case "window": {
      const window =
        data.windowKind === "tumbling"
          ? { kind: "tumbling" as const, size_ms: data.sizeMs }
          : data.windowKind === "sliding"
            ? { kind: "sliding" as const, size_ms: data.sizeMs, slide_ms: data.slideMs }
            : { kind: "session" as const, gap_ms: data.gapMs };
      return {
        type: "window",
        window,
        aggregate: data.aggregateFn === "count" ? { fn: "count" } : { fn: data.aggregateFn, field: data.aggregateField.trim() },
        allowed_lateness_ms: data.allowedLatenessMs,
        late_events: data.lateEvents,
      };
    }
    default:
      return null;
  }
}

/** The job definition for a validated chain. */
export function toJobDefinition(graph: BuilderGraph, chain: string[], settings: JobSettings): JobDefinition {
  const byId = new Map(graph.nodes.map((n) => [n.id, n.data]));
  const source = byId.get(chain[0]);
  const sink = byId.get(chain[chain.length - 1]);
  if (source?.kind !== "source" || sink?.kind !== "sink") throw new Error("chain must run from Source to Sink");
  const definition: JobDefinition = {
    name: settings.name.trim() || "untitled",
    source: {
      type: "grpc",
      stream: source.stream.trim(),
      max_out_of_orderness_ms: source.maxOutOfOrdernessMs,
      ...(source.idleTimeoutMs ? { idle_timeout_ms: source.idleTimeoutMs } : {}),
    },
    operators: chain.slice(1, -1).map((id) => operatorDef(byId.get(id)!)).filter((op): op is OperatorDef => op !== null),
    sink: { type: "buffer", capacity: sink.capacity },
  };
  if (settings.checkpointing) {
    definition.checkpointing = { interval_ms: settings.intervalMs, backend: settings.backend, retained: settings.retained };
    definition.restart = { max_restarts: settings.maxRestarts, delay_ms: settings.restartDelayMs };
  }
  return definition;
}

/** The builder graph (and settings) for an existing job definition: the
 * inverse of {@link toJobDefinition}, for "edit as new pipeline". */
export function definitionToGraph(definition: JobDefinition): { graph: BuilderGraph; settings: Partial<JobSettings> } {
  const data: NodeData[] = [
    {
      kind: "source",
      stream: definition.source.stream,
      maxOutOfOrdernessMs: definition.source.max_out_of_orderness_ms ?? 0,
      idleTimeoutMs: definition.source.idle_timeout_ms ?? null,
    },
  ];
  for (const op of definition.operators ?? []) {
    switch (op.type) {
      case "filter":
        data.push({
          kind: "filter",
          field: op.field,
          op: op.op,
          value: typeof op.value === "string" ? `'${op.value}'` : JSON.stringify(op.value),
        });
        break;
      case "map":
        data.push({ kind: "map", select: op.select.join(", ") });
        break;
      case "key_by":
        data.push({ kind: "key_by", field: op.field });
        break;
      case "window": {
        const base = defaultData("window") as Extract<NodeData, { kind: "window" }>;
        const w = op.window;
        data.push({
          ...base,
          windowKind: w.kind,
          sizeMs: w.kind === "session" ? base.sizeMs : w.size_ms,
          slideMs: w.kind === "sliding" ? w.slide_ms : base.slideMs,
          gapMs: w.kind === "session" ? w.gap_ms : base.gapMs,
          aggregateFn: op.aggregate.fn,
          aggregateField: op.aggregate.field ?? "",
          allowedLatenessMs: op.allowed_lateness_ms ?? 0,
          lateEvents: op.late_events ?? "drop",
        });
        break;
      }
    }
  }
  data.push({ kind: "sink", capacity: definition.sink?.capacity ?? 1000 });
  const nodes = data.map((d, i) => ({ id: `n${i}`, data: d }));
  const edges = nodes.slice(1).map((n, i) => ({ source: nodes[i].id, target: n.id }));
  const settings: Partial<JobSettings> = {
    name: definition.name,
    checkpointing: Boolean(definition.checkpointing),
    ...(definition.checkpointing?.interval_ms ? { intervalMs: definition.checkpointing.interval_ms } : {}),
    ...(definition.checkpointing?.backend ? { backend: definition.checkpointing.backend } : {}),
    ...(definition.checkpointing?.retained ? { retained: definition.checkpointing.retained } : {}),
    ...(definition.restart?.max_restarts != null ? { maxRestarts: definition.restart.max_restarts } : {}),
    ...(definition.restart?.delay_ms != null ? { restartDelayMs: definition.restart.delay_ms } : {}),
  };
  return { graph: { nodes, edges }, settings };
}

/** Default configuration for a new node. */
export function defaultData(kind: NodeData["kind"]): NodeData {
  switch (kind) {
    case "source":
      return { kind, stream: "purchases", maxOutOfOrdernessMs: 2000, idleTimeoutMs: null };
    case "filter":
      return { kind, field: "amount", op: "gt", value: "0" };
    case "map":
      return { kind, select: "user_id, amount" };
    case "key_by":
      return { kind, field: "user_id" };
    case "window":
      return {
        kind,
        windowKind: "tumbling",
        sizeMs: 60_000,
        slideMs: 10_000,
        gapMs: 30_000,
        aggregateFn: "sum",
        aggregateField: "amount",
        allowedLatenessMs: 0,
        lateEvents: "drop",
      };
    case "sink":
      return { kind, capacity: 5000 };
    case "join":
      return { kind };
  }
}

/** One-line summary shown on a node. */
export function describe(data: NodeData): string {
  switch (data.kind) {
    case "source":
      return `stream ${data.stream || "?"} · ${data.maxOutOfOrdernessMs} ms disorder`;
    case "filter":
      return `${data.field || "?"} ${{ eq: "=", ne: "≠", gt: ">", gte: "≥", lt: "<", lte: "≤" }[data.op]} ${data.value}`;
    case "map":
      return `keep ${data.select || "?"}`;
    case "key_by":
      return `by ${data.field || "?"}`;
    case "window": {
      const fmt = (ms: number) => (ms % 60_000 === 0 ? `${ms / 60_000}m` : ms % 1000 === 0 ? `${ms / 1000}s` : `${ms}ms`);
      const span =
        data.windowKind === "tumbling"
          ? `tumbling ${fmt(data.sizeMs)}`
          : data.windowKind === "sliding"
            ? `sliding ${fmt(data.sizeMs)}/${fmt(data.slideMs)}`
            : `session gap ${fmt(data.gapMs)}`;
      const agg = data.aggregateFn === "count" ? "count(*)" : `${data.aggregateFn}(${data.aggregateField})`;
      return `${span} · ${agg}`;
    }
    case "sink":
      return `buffer · last ${data.capacity}`;
    case "join":
      return "Rust API only";
  }
}
