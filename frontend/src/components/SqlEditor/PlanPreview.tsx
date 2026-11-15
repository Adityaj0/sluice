import { Check, TriangleAlert } from "lucide-react";
import { useState } from "react";

import type { ExplainResult, PlanNode } from "../../types/pipeline.ts";
import { Segmented } from "../common/ui.tsx";

function chain(plan: PlanNode): PlanNode[] {
  const nodes: PlanNode[] = [];
  for (let node: PlanNode | null = plan; node; node = node.input) nodes.push(node);
  return nodes.reverse(); // data flows from the scan down
}

function PlanDiagram({ plan }: { plan: PlanNode }) {
  const nodes = chain(plan);
  return (
    <div className="flex flex-col">
      {nodes.map((node, i) => {
        return (
          <div key={i} className="flex flex-col items-center">
            <div className="w-full rounded-md border border-slate-300 bg-white px-3 py-2">
              <div className="text-xs font-semibold text-slate-900">{node.node.replace("WindowAggregate", "Window aggregate")}</div>
              <div className="mt-0.5 font-mono text-xs leading-relaxed break-words text-slate-700">{node.detail}</div>
            </div>
            {i < nodes.length - 1 && <div className="h-4 w-px bg-slate-300" />}
          </div>
        );
      })}
    </div>
  );
}

export function PlanPreview({ result }: { result: ExplainResult }) {
  const [tab, setTab] = useState<"optimized" | "logical" | "explain">("optimized");
  return (
    <div className="space-y-6">
      <Segmented
        value={tab}
        onChange={setTab}
        options={[
          { id: "optimized", label: "Optimized" },
          { id: "logical", label: "As written" },
          { id: "explain", label: "EXPLAIN" },
        ]}
      />
      {tab === "explain" ? (
        <pre className="overflow-x-auto rounded-lg bg-slate-50 p-3 font-mono text-[11px] leading-relaxed text-slate-700 ring-1 ring-slate-200">{result.explain}</pre>
      ) : (
        <>
          <PlanDiagram plan={tab === "optimized" ? result.optimized : result.logical} />
          <section>
            <h3 className="mb-2 text-[13px] font-semibold text-slate-900">What the optimizer did</h3>
            {result.optimizations.length ? (
              <ul className="space-y-2">
                {result.optimizations.map((rule, i) => {
                  const warning = rule.startsWith("warning");
                  const [title, ...rest] = rule.split(": ");
                  return (
                    <li key={i} className="flex gap-2 text-[13px]">
                      {warning ? <TriangleAlert className="mt-0.5 size-4 shrink-0 text-amber-500" /> : <Check className="mt-0.5 size-4 shrink-0 text-emerald-500" />}
                      <span className="min-w-0 text-slate-600">
                        <span className="font-medium text-slate-900 capitalize">{title}</span>
                        {rest.length > 0 && <span className="block font-mono text-xs break-words text-slate-500">{rest.join(": ")}</span>}
                      </span>
                    </li>
                  );
                })}
              </ul>
            ) : (
              <p className="text-[13px] text-slate-500">Nothing to optimize.</p>
            )}
          </section>
          <section>
            <h3 className="mb-2 text-[13px] font-semibold text-slate-900">Runs as {result.stages.length} stages</h3>
            <ol className="space-y-1.5">
              {result.stages.map((stage, i) => (
                <li key={i} className="flex gap-2.5 text-[13px]">
                  <span className="grid size-5 shrink-0 place-items-center rounded-full bg-slate-100 text-[11px] font-semibold text-slate-600">{i + 1}</span>
                  <span className="min-w-0">
                    <span className="font-medium text-slate-900 capitalize">{stage.kind.replace("_", " ")}</span>{" "}
                    <span className="font-mono text-xs break-words text-slate-500">{stage.name.replace(/^[a-z_]+\(|\)$/g, "")}</span>
                  </span>
                </li>
              ))}
            </ol>
          </section>
        </>
      )}
    </div>
  );
}
