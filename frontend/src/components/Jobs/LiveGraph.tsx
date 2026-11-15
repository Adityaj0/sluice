import { Background, BackgroundVariant, Controls, MarkerType, ReactFlow, ReactFlowProvider, useReactFlow } from "@xyflow/react";
import type { Edge } from "@xyflow/react";
import { useEffect, useMemo } from "react";

import { api } from "../../api.ts";
import { health, layoutStages, rates } from "../../lib/live.ts";
import { parseStage } from "../../lib/stages.ts";
import type { Sample } from "../../store/pipelineStore.ts";
import type { LiveSnapshot } from "../../types/pipeline.ts";
import { StageNode } from "./StageNode.tsx";
import type { StageFlowNode } from "./StageNode.tsx";

const nodeTypes = { stage: StageNode };
const COL = 240;
const ROW = 170;
const PER_LINE = 5;

/**
 * Grid cell for a stage. Branching DAGs keep their layers as columns. A long
 * straight pipeline wraps like a snake (left to right, then right to left) so
 * every stage stays readable instead of shrinking to fit one row.
 */
export function snakeCell(layer: number, row: number, placed: { layer: number; row: number }[]) {
  const linear = placed.every((p) => p.row === 0);
  const layers = placed.length ? Math.max(...placed.map((p) => p.layer)) + 1 : 0;
  if (!linear || layers <= PER_LINE) return { col: layer, line: row };
  const perLine = Math.ceil(layers / Math.ceil(layers / PER_LINE));
  const line = Math.floor(layer / perLine);
  const offset = layer % perLine;
  return { col: line % 2 === 0 ? offset : perLine - 1 - offset, line };
}

/** Number of grid rows the graph occupies, for sizing its container. */
export function graphLines(placed: { layer: number; row: number }[]) {
  return placed.length ? Math.max(...placed.map((p) => snakeCell(p.layer, p.row, placed).line)) + 1 : 1;
}

function Graph({ snapshot, previous, history }: { snapshot: LiveSnapshot; previous?: LiveSnapshot; history: Sample[] }) {
  const { fitView } = useReactFlow();
  const shape = snapshot.stages.map((s) => s.inputs.join(",")).join("|");

  const { nodes, edges } = useMemo(() => {
    const perOp = rates(previous, snapshot);
    const consumers = new Set(snapshot.stages.flatMap((s) => s.inputs));
    const state = snapshot.job.status.state;
    const recent = history.slice(-40);
    const placed = layoutStages(snapshot.stages);
    const cells = new Map<number, { col: number; line: number }>();
    for (const { id, layer, row } of placed) {
      cells.set(id, snakeCell(layer, row, placed));
    }
    const nodes: StageFlowNode[] = placed.map(({ id }) => {
      const sink = !consumers.has(id);
      const cell = cells.get(id)!;
      return {
        id: String(id),
        type: "stage",
        position: { x: cell.col * COL, y: cell.line * ROW },
        data: {
          stage: snapshot.stages[id],
          metrics: snapshot.operators[id],
          rate: sink ? perOp[id].inPerSec : perOp[id].outPerSec,
          history: recent.map((s) => (sink ? s.operators[id]?.inPerSec : s.operators[id]?.outPerSec) ?? 0),
          health: health(snapshot.operators[id], state),
          sink,
          onKill:
            state === "running"
              ? () => {
                  const { title } = parseStage(snapshot.operators[id].name);
                  if (confirm(`Crash the “${title}” stage? The job will restart from its latest checkpoint.`)) {
                    void api.killStage(snapshot.job.id, id);
                  }
                }
              : undefined,
        },
      };
    });
    const edges: Edge[] = snapshot.stages.flatMap((stage, target) =>
      stage.inputs.map((source) => {
        const rate = perOp[source]?.outPerSec ?? 0;
        const pressured = (snapshot.operators[source]?.backpressure_ratio ?? 0) >= 0.3;
        const flowing = rate > 0 && state === "running";
        const from = cells.get(source)!;
        const to = cells.get(target)!;
        const [sourceHandle, targetHandle] = to.line > from.line && to.col === from.col ? ["b", "t"] : to.col < from.col ? ["sl", "tr"] : ["r", "l"];
        return {
          id: `${source}-${target}`,
          source: String(source),
          target: String(target),
          sourceHandle,
          targetHandle,
          type: "smoothstep",
          animated: flowing,
          className: pressured ? "backpressured" : undefined,
          markerEnd: { type: MarkerType.ArrowClosed, color: pressured ? "#c82d4c" : flowing ? "#2272b4" : "#a3afbb", width: 9, height: 9 },
          style: { strokeWidth: flowing ? 2 : 1.5, stroke: flowing ? "#2272b4" : "#a3afbb" },
        };
      }),
    );
    return { nodes, edges };
  }, [snapshot, previous, history]);

  useEffect(() => {
    const timer = setTimeout(() => void fitView({ padding: 0.06, maxZoom: 1 }), 60);
    return () => clearTimeout(timer);
  }, [shape, fitView]);

  return (
    <ReactFlow
      colorMode="light"
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      nodesConnectable={false}
      nodesDraggable={false}
      fitView
      fitViewOptions={{ padding: 0.06, maxZoom: 1 }}
      minZoom={0.3}
      proOptions={{ hideAttribution: true }}
    >
      <Background variant={BackgroundVariant.Dots} gap={18} size={1.2} color="#e1e5ea" />
      <Controls showInteractive={false} position="bottom-right" />
    </ReactFlow>
  );
}

export function LiveGraph(props: { snapshot: LiveSnapshot; previous?: LiveSnapshot; history: Sample[] }) {
  return (
    <ReactFlowProvider>
      <Graph {...props} />
    </ReactFlowProvider>
  );
}
