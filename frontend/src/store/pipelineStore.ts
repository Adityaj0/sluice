import { addEdge, applyEdgeChanges, applyNodeChanges } from "@xyflow/react";
import type { Connection, Edge, EdgeChange, Node, NodeChange, XYPosition } from "@xyflow/react";
import { create } from "zustand";

import { api } from "../api.ts";
import { rates } from "../lib/live.ts";
import { defaultData, definitionToGraph } from "../lib/validate.ts";
import type {
  GeneratorInfo,
  JobDefinition,
  JobSettings,
  JobSummary,
  LiveSnapshot,
  NodeData,
  NodeKind,
  StreamInfo,
} from "../types/pipeline.ts";

export type View = "overview" | "data" | "builder" | "sql" | "jobs" | "job" | "debugger" | "metrics";
export type BuilderNode = Node<NodeData, "operator">;

/** One point of a job's metric history (per operator, same order as stages). */
export interface Sample {
  ts: number;
  operators: {
    name: string;
    inPerSec: number;
    outPerSec: number;
    watermarkLagMs: number | null;
    backpressure: number;
    queueFill: number;
  }[];
}

export interface LiveJob {
  latest?: LiveSnapshot;
  previous?: LiveSnapshot;
  history: Sample[];
  connected: boolean;
}

const HISTORY = 600; // 5 minutes at 2 samples/s

interface Store {
  view: View;
  jobId: number | null;
  navigate: (view: View, jobId?: number | null) => void;

  connected: boolean;
  jobs: JobSummary[];
  streams: StreamInfo[];
  streamRates: Record<string, number>;
  /** Total ingest rate across streams, one point per refresh (about 10 minutes). */
  ingestHistory: { ts: number; rate: number }[];
  generators: GeneratorInfo[];
  refresh: () => Promise<void>;

  nodes: BuilderNode[];
  edges: Edge[];
  selected: string | null;
  settings: JobSettings;
  onNodesChange: (changes: NodeChange<BuilderNode>[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;
  addNode: (kind: NodeKind, position: XYPosition) => void;
  updateNode: (id: string, data: NodeData) => void;
  removeNode: (id: string) => void;
  select: (id: string | null) => void;
  setSettings: (settings: Partial<JobSettings>) => void;
  loadExample: () => void;
  loadDefinition: (definition: JobDefinition) => void;
  clear: () => void;

  sqlDraft: string | null;
  setSqlDraft: (sql: string | null) => void;

  live: Record<number, LiveJob>;
  pushSnapshot: (jobId: number, snapshot: LiveSnapshot) => void;
  setConnected: (jobId: number, connected: boolean) => void;
}

const EDGE_STYLE = {
  type: "smoothstep",
  style: { strokeWidth: 1.75, stroke: "#8f9ca9" },
  markerEnd: { type: "arrowclosed" as const, color: "#8f9ca9", width: 16, height: 16 },
};

let nextNodeId = 1;
const nodeId = () => `node-${nextNodeId++}`;

function placed(kinds: NodeData[]): { nodes: BuilderNode[]; edges: Edge[] } {
  const nodes = kinds.map((data, i) => ({
    id: nodeId(),
    type: "operator" as const,
    position: { x: 0, y: i * 120 },
    data,
  }));
  const edges = nodes.slice(1).map((node, i) => ({ id: `e-${nodes[i].id}-${node.id}`, source: nodes[i].id, target: node.id, ...EDGE_STYLE }));
  return { nodes, edges };
}

const exampleGraph = () => placed((["source", "filter", "key_by", "window", "sink"] as NodeKind[]).map(defaultData));

const VIEWS: View[] = ["overview", "data", "builder", "sql", "jobs", "job", "debugger", "metrics"];

function readHash(): { view: View; jobId: number | null } {
  const [view, id] = location.hash.replace(/^#\/?/, "").split("/");
  const jobId = id && /^\d+$/.test(id) ? Number(id) : null;
  if (view === "jobs" && jobId != null) return { view: "job", jobId };
  return { view: VIEWS.includes(view as View) ? (view as View) : "overview", jobId };
}

function hashFor(view: View, jobId: number | null): string {
  if (view === "job") return `#/jobs/${jobId}`;
  const withJob = view === "debugger" || view === "metrics";
  return `#/${view}${withJob && jobId != null ? `/${jobId}` : ""}`;
}

let lastStreams: { at: number; streams: StreamInfo[] } | null = null;

export const useStore = create<Store>((set, get) => ({
  ...readHash(),
  navigate: (view, jobId) => {
    const id = jobId === undefined ? get().jobId : jobId;
    set({ view, jobId: id });
    const hash = hashFor(view, id);
    if (location.hash !== hash) history.pushState(null, "", hash);
  },

  connected: true,
  jobs: [],
  streams: [],
  streamRates: {},
  ingestHistory: [],
  generators: [],
  refresh: async () => {
    try {
      const [jobs, streams, generators] = await Promise.all([api.jobs(), api.streams(), api.generators()]);
      const now = Date.now();
      const streamRates: Record<string, number> = {};
      if (lastStreams) {
        const seconds = (now - lastStreams.at) / 1000;
        for (const s of streams) {
          const before = lastStreams.streams.find((p) => p.name === s.name);
          streamRates[s.name] = before && seconds > 0 ? Math.max(0, (s.next_offset - before.next_offset) / seconds) : 0;
        }
      }
      const hadBaseline = lastStreams != null;
      lastStreams = { at: now, streams };
      const total = Object.values(streamRates).reduce((a, b) => a + b, 0);
      const ingestHistory = hadBaseline ? [...get().ingestHistory, { ts: now, rate: total }].slice(-300) : get().ingestHistory;
      set({ jobs, streams, generators, streamRates, ingestHistory, connected: true });
      if (get().jobId == null && jobs.length) set({ jobId: jobs[jobs.length - 1].id });
    } catch {
      set({ connected: false });
    }
  },

  ...exampleGraph(),
  selected: null,
  settings: {
    name: "spend-per-user",
    checkpointing: true,
    intervalMs: 1000,
    backend: "rocksdb",
    retained: 10,
    maxRestarts: 3,
    restartDelayMs: 200,
  },
  onNodesChange: (changes) => set({ nodes: applyNodeChanges(changes, get().nodes) }),
  onEdgesChange: (changes) => set({ edges: applyEdgeChanges(changes, get().edges) }),
  onConnect: (connection) => set({ edges: addEdge({ ...connection, ...EDGE_STYLE, id: `e-${connection.source}-${connection.target}` }, get().edges) }),
  addNode: (kind, position) => {
    const node: BuilderNode = { id: nodeId(), type: "operator", position, data: defaultData(kind) };
    set({ nodes: [...get().nodes, node], selected: node.id });
  },
  updateNode: (id, data) => set({ nodes: get().nodes.map((n) => (n.id === id ? { ...n, data } : n)) }),
  removeNode: (id) =>
    set({
      nodes: get().nodes.filter((n) => n.id !== id),
      edges: get().edges.filter((e) => e.source !== id && e.target !== id),
      selected: get().selected === id ? null : get().selected,
    }),
  select: (id) => set({ selected: id }),
  setSettings: (settings) => set({ settings: { ...get().settings, ...settings } }),
  loadExample: () => set({ ...exampleGraph(), selected: null }),
  loadDefinition: (definition) => {
    const { graph, settings } = definitionToGraph(definition);
    set({
      ...placed(graph.nodes.map((n) => n.data)),
      selected: null,
      settings: { ...get().settings, ...settings, name: `${definition.name}-v2` },
    });
  },
  clear: () => set({ nodes: [], edges: [], selected: null }),

  sqlDraft: null,
  setSqlDraft: (sqlDraft) => set({ sqlDraft }),

  live: {},
  pushSnapshot: (jobId, snapshot) => {
    const job = get().live[jobId] ?? { history: [], connected: true };
    const previous = job.latest;
    const restarted = previous && previous.job.restarts !== snapshot.job.restarts;
    const perOp = rates(restarted ? undefined : previous, snapshot);
    const sample: Sample = {
      ts: snapshot.ts,
      operators: snapshot.operators.map((op, i) => ({
        name: op.name,
        inPerSec: perOp[i].inPerSec,
        outPerSec: perOp[i].outPerSec,
        watermarkLagMs: op.watermark != null && op.watermark < Number.MAX_SAFE_INTEGER ? snapshot.ts - op.watermark : null,
        backpressure: op.backpressure_ratio,
        queueFill: op.input_capacity ? op.input_queue / op.input_capacity : 0,
      })),
    };
    const history = previous ? [...job.history, sample].slice(-HISTORY) : job.history;
    set({ live: { ...get().live, [jobId]: { ...job, previous, latest: snapshot, history } } });
  },
  setConnected: (jobId, connected) => {
    const job = get().live[jobId] ?? { history: [], connected };
    set({ live: { ...get().live, [jobId]: { ...job, connected } } });
  },
}));

window.addEventListener("popstate", () => useStore.setState(readHash()));
window.addEventListener("hashchange", () => useStore.setState(readHash()));
