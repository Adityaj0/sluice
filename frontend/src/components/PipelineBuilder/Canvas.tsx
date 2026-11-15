import { Background, BackgroundVariant, Controls, MarkerType, ReactFlow, useReactFlow } from "@xyflow/react";
import type { DragEvent } from "react";
import { useShallow } from "zustand/react/shallow";

import { useStore } from "../../store/pipelineStore.ts";
import type { NodeKind } from "../../types/pipeline.ts";
import { OperatorNode } from "./OperatorNode.tsx";
import { DRAG_TYPE } from "./Palette.tsx";

const nodeTypes = { operator: OperatorNode };

const EDGE_OPTIONS = {
  type: "smoothstep",
  style: { strokeWidth: 1.75, stroke: "#8f9ca9" },
  markerEnd: { type: MarkerType.ArrowClosed, color: "#8f9ca9", width: 16, height: 16 },
};

export function Canvas() {
  const { nodes, edges, onNodesChange, onEdgesChange, onConnect, addNode, select } = useStore(
    useShallow((s) => ({
      nodes: s.nodes,
      edges: s.edges,
      onNodesChange: s.onNodesChange,
      onEdgesChange: s.onEdgesChange,
      onConnect: s.onConnect,
      addNode: s.addNode,
      select: s.select,
    })),
  );
  const { screenToFlowPosition } = useReactFlow();

  const onDrop = (event: DragEvent) => {
    event.preventDefault();
    const kind = event.dataTransfer.getData(DRAG_TYPE) as NodeKind;
    if (kind) addNode(kind, screenToFlowPosition({ x: event.clientX - 110, y: event.clientY - 28 }));
  };

  return (
    <ReactFlow
      colorMode="light"
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      onNodeClick={(_, node) => select(node.id)}
      onPaneClick={() => select(null)}
      onDragOver={(e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = "move";
      }}
      onDrop={onDrop}
      isValidConnection={(c) => c.source !== c.target}
      defaultEdgeOptions={EDGE_OPTIONS}
      deleteKeyCode={["Backspace", "Delete"]}
      fitView
      fitViewOptions={{ padding: 0.2, maxZoom: 1 }}
      proOptions={{ hideAttribution: true }}
    >
      <Background variant={BackgroundVariant.Dots} gap={18} size={1.2} color="#cbd2d9" />
      <Controls showInteractive={false} position="bottom-left" />
    </ReactFlow>
  );
}
