import { ReactFlowProvider } from "@xyflow/react";
import { AlertCircle, CheckCircle2, Code, Eraser, Rocket, Sparkles } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { useSubmitJob } from "../../hooks/usePipeline.ts";
import { toJobDefinition, validate } from "../../lib/validate.ts";
import { useStore } from "../../store/pipelineStore.ts";
import { Button, Callout, Tabs } from "../common/ui.tsx";
import { Canvas } from "./Canvas.tsx";
import { JobSettingsForm, OperatorInspector } from "./ConfigPanel.tsx";
import { IssuesContext } from "./OperatorNode.tsx";
import { Palette } from "./Palette.tsx";

export function BuilderView() {
  const { nodes, edges, settings, selected, select, setSettings, loadExample, clear } = useStore();
  const { submit, submitting, error } = useSubmitJob();
  const [tab, setTab] = useState<"operator" | "job" | "json">("operator");

  useEffect(() => {
    if (selected) setTab("operator");
  }, [selected]);

  const graph = useMemo(
    () => ({ nodes: nodes.map((n) => ({ id: n.id, data: n.data })), edges: edges.map((e) => ({ source: e.source, target: e.target })) }),
    [nodes, edges],
  );
  const validation = useMemo(() => validate(graph), [graph]);
  const definition = useMemo(() => (validation.chain ? toJobDefinition(graph, validation.chain, settings) : null), [graph, validation, settings]);
  const issuesByNode = useMemo(() => {
    const map = new Map<string, string[]>();
    for (const issue of validation.issues) if (issue.nodeId) map.set(issue.nodeId, [...(map.get(issue.nodeId) ?? []), issue.message]);
    return map;
  }, [validation]);
  const ok = validation.issues.length === 0;

  return (
    <div className="flex h-full flex-col">
      <header className="flex h-16 shrink-0 items-center gap-4 border-b border-slate-200 bg-white px-6">
        <div className="min-w-0">
          <div className="text-xs text-slate-500">Pipeline builder</div>
          <input
            value={settings.name}
            onChange={(e) => setSettings({ name: e.target.value })}
            aria-label="Job name"
            className="-ml-1 w-72 rounded-md px-1 text-[15px] font-semibold text-slate-900 outline-none hover:bg-slate-50 focus:bg-slate-50 focus:ring-2 focus:ring-indigo-500/20"
          />
        </div>
        <span className="inline-flex items-center gap-1.5 text-[13px] text-slate-800">
          {ok ? <CheckCircle2 className="size-4 text-emerald-600" /> : <AlertCircle className="size-4 text-amber-600" />}
          {ok ? "Ready to deploy" : `${validation.issues.length} issue${validation.issues.length === 1 ? "" : "s"} to fix`}
        </span>
        <div className="ml-auto flex items-center gap-2">
          <Button variant="ghost" icon={Sparkles} onClick={loadExample}>
            Example
          </Button>
          <Button variant="ghost" icon={Eraser} onClick={clear}>
            Clear
          </Button>
          <Button variant="primary" icon={Rocket} disabled={!definition || submitting} onClick={() => definition && void submit(definition)}>
            {submitting ? "Deploying…" : "Deploy"}
          </Button>
        </div>
      </header>

      <div className="grid min-h-0 flex-1 grid-cols-[264px_1fr_360px]">
        <aside className="overflow-y-auto border-r border-slate-200 bg-white">
          <Palette />
        </aside>

        <div className="relative bg-slate-50">
          <IssuesContext.Provider value={issuesByNode}>
            <ReactFlowProvider>
              <Canvas />
            </ReactFlowProvider>
          </IssuesContext.Provider>
        </div>

        <aside className="flex min-h-0 flex-col border-l border-slate-200 bg-white">
          <div className="px-5 pt-4">
            <Tabs
              value={tab}
              onChange={setTab}
              tabs={[
                { id: "operator", label: "Operator" },
                { id: "job", label: "Job settings" },
                { id: "json", label: <span className="inline-flex items-center gap-1"><Code className="size-3.5" />JSON</span> },
              ]}
            />
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto p-5">
            {tab === "operator" && <OperatorInspector />}
            {tab === "job" && <JobSettingsForm />}
            {tab === "json" &&
              (definition ? (
                <pre className="overflow-x-auto rounded-lg bg-slate-50 p-3 font-mono text-xs leading-relaxed text-slate-700 ring-1 ring-slate-200">
                  {JSON.stringify(definition, null, 2)}
                </pre>
              ) : (
                <p className="text-[13px] text-slate-500">Fix the issues below to see the job definition that will be submitted.</p>
              ))}
          </div>
          <div className="border-t border-slate-200 p-5">
            {ok ? (
              <p className="flex items-start gap-2 text-[13px] text-emerald-700">
                <CheckCircle2 className="mt-0.5 size-4 shrink-0" />
                Linear pipeline from Source to Sink, every operator configured.
              </p>
            ) : (
              <ul className="max-h-40 space-y-2 overflow-y-auto">
                {validation.issues.map((issue, i) => (
                  <li key={i}>
                    <button
                      type="button"
                      onClick={() => issue.nodeId && select(issue.nodeId)}
                      className="flex w-full items-start gap-2 text-left text-[13px] text-slate-700 hover:text-slate-900"
                    >
                      <AlertCircle className="mt-0.5 size-4 shrink-0 text-amber-500" />
                      {issue.message}
                    </button>
                  </li>
                ))}
              </ul>
            )}
            {error && (
              <div className="mt-3">
                <Callout>{error}</Callout>
              </div>
            )}
          </div>
        </aside>
      </div>
    </div>
  );
}
