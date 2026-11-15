import { Activity, ArrowLeft, Clock3, Copy, History, Inbox, LineChart, RotateCcw, ShieldCheck, Square } from "lucide-react";
import { useEffect, useState } from "react";

import { api } from "../../api.ts";
import { useLiveJob } from "../../hooks/useWebSocket.ts";
import { ago, clock, compact, percent } from "../../lib/format.ts";
import { layoutStages, rates } from "../../lib/live.ts";
import { parseStage } from "../../lib/stages.ts";
import { useStore } from "../../store/pipelineStore.ts";
import type { JobDetails } from "../../types/pipeline.ts";
import {
  Button,
  Callout,
  Card,
  EmptyState,
  KindBadge,
  Metric,
  Page,
  PageHeader,
  Sparkline,
  StatusBadge,
  Tabs,
  tableClass,
  tdClass,
  thClass,
} from "../common/ui.tsx";
import { LiveGraph, graphLines } from "./LiveGraph.tsx";
import { OutputTable } from "./OutputTable.tsx";
import { STAGE_ICONS } from "./stageIcons.tsx";

type Tab = "dataflow" | "output" | "operators" | "definition";

export function JobDetailView() {
  const jobId = useStore((s) => s.jobId);
  const navigate = useStore((s) => s.navigate);
  const loadDefinition = useStore((s) => s.loadDefinition);
  const setSqlDraft = useStore((s) => s.setSqlDraft);
  const live = useLiveJob(jobId);
  const [details, setDetails] = useState<JobDetails | null>(null);
  const [tab, setTab] = useState<Tab>("dataflow");
  const snapshot = live?.latest;
  const restarts = snapshot?.job.restarts;
  const state = snapshot?.job.status.state;

  useEffect(() => {
    if (jobId == null) return;
    api.job(jobId).then(setDetails).catch(() => setDetails(null));
  }, [jobId, restarts, state]);

  if (jobId == null) return null;
  if (!snapshot || !details) {
    return (
      <Page>
        <EmptyState icon={Activity} title={live?.connected === false ? "Connecting to the engine…" : "Loading live metrics…"} />
      </Page>
    );
  }

  const { job, checkpoints, operators } = snapshot;
  const perOp = rates(live.previous, snapshot);
  const sources = snapshot.stages.flatMap((s, i) => (s.is_source ? [i] : []));
  const consumers = new Set(snapshot.stages.flatMap((s) => s.inputs));
  const sinks = snapshot.stages.flatMap((_, i) => (consumers.has(i) ? [] : [i]));
  const ingest = sources.reduce((sum, i) => sum + perOp[i].outPerSec, 0);
  const ingestHistory = live.history.slice(-40).map((s) => sources.reduce((sum, i) => sum + (s.operators[i]?.outPerSec ?? 0), 0));
  const results = sinks.reduce((sum, i) => sum + operators[i].records_in, 0);
  const latency = operators[sinks[0]]?.latency_ms;
  const running = job.status.state === "running";

  const editAsNew = () => {
    if (details.definition.query) {
      setSqlDraft(details.definition.query);
      navigate("sql");
    } else {
      loadDefinition(details.definition);
      navigate("builder");
    }
  };

  return (
    <Page>
      <PageHeader
        eyebrow={
          <button type="button" onClick={() => navigate("jobs")} className="inline-flex items-center gap-1 hover:text-slate-800">
            <ArrowLeft className="size-3.5" /> Jobs
          </button>
        }
        title={
          <span className="flex flex-wrap items-center gap-3">
            {job.name}
            <StatusBadge status={job.status} />
            <KindBadge kind={job.kind} />
          </span>
        }
        description={
          <>
            Job #{job.id} · reads <span className="font-mono text-slate-700">{job.stream}</span> · started {ago(job.submitted_at)}
            {!live.connected && <span className="ml-2 text-amber-600">reconnecting…</span>}
          </>
        }
        actions={
          <>
            <Button icon={History} onClick={() => navigate("debugger", job.id)}>
              Debugger
            </Button>
            <Button icon={LineChart} onClick={() => navigate("metrics", job.id)}>
              Metrics
            </Button>
            <Button icon={Copy} onClick={editAsNew}>
              Edit as new
            </Button>
            {running && (
              <Button variant="danger" icon={Square} onClick={() => confirm(`Cancel “${job.name}”?`) && void api.cancel(job.id)}>
                Cancel
              </Button>
            )}
          </>
        }
      />

      {details.failures.length > 0 && (
        <Callout tone={job.status.state === "failed" ? "rose" : "amber"} title={job.status.state === "failed" ? "The job failed" : `Recovered ${job.restarts} time${job.restarts === 1 ? "" : "s"}`}>
          {details.failures.map((f, i) => (
            <div key={i} className="font-mono text-xs">
              {f}
            </div>
          ))}
          {checkpoints.restored_from != null && job.status.state !== "failed" && (
            <div className="mt-1">Restored from checkpoint #{checkpoints.restored_from}; no event was lost or counted twice.</div>
          )}
        </Callout>
      )}

      <div className="grid grid-cols-2 gap-4 xl:grid-cols-5">
        <Metric label="Ingest" icon={Activity} value={compact(ingest)} unit="/s" trend={<Sparkline values={ingestHistory} />} />
        <Metric label="Results emitted" icon={Inbox} value={compact(results)} hint="committed to the output" />
        <Metric
          label="Checkpoints"
          icon={ShieldCheck}
          value={compact(checkpoints.completed)}
          hint={checkpoints.failed ? `${checkpoints.failed} failed or aborted` : checkpoints.latest_completed ? `latest #${checkpoints.latest_completed}` : "none yet"}
        />
        <Metric
          label="Recoveries"
          icon={RotateCcw}
          value={job.restarts}
          tone={job.restarts ? "amber" : "slate"}
          hint={checkpoints.restored_from ? `from checkpoint #${checkpoints.restored_from}` : "no failures"}
        />
        <Metric label="Latency p99" icon={Clock3} value={latency ? `≤${latency.p99}` : "–"} unit={latency ? "ms" : undefined} hint="ingestion → sink" />
      </div>

      <Tabs<Tab>
        value={tab}
        onChange={setTab}
        tabs={[
          { id: "dataflow", label: "Dataflow" },
          { id: "output", label: "Output" },
          { id: "operators", label: "Operators" },
          { id: "definition", label: "Definition" },
        ]}
      />

      {tab === "dataflow" && (
        <Card
          title="Live dataflow"
          description="Updated twice a second. Stages turn amber when backpressured and red when failed. Hover a stage and click Kill to crash it and watch the job recover."
          bodyClassName="p-0"
        >
          <div style={{ height: 110 + 170 * graphLines(layoutStages(snapshot.stages)) }}>
            <LiveGraph snapshot={snapshot} previous={live.previous} history={live.history} />
          </div>
        </Card>
      )}

      {tab === "output" && (
        <Card title="Latest output" description="The 50 most recent results, newest first." bodyClassName="max-h-[560px] overflow-auto">
          <OutputTable jobId={job.id} />
        </Card>
      )}

      {tab === "operators" && (
        <Card title="Operators" description="Per-stage counters for the current attempt." bodyClassName="overflow-x-auto">
          <table className={tableClass}>
            <thead>
              <tr>
                <th className={thClass}>Stage</th>
                <th className={`${thClass} text-right`}>In / s</th>
                <th className={`${thClass} text-right`}>Out / s</th>
                <th className={`${thClass} text-right`}>Records in</th>
                <th className={`${thClass} text-right`}>Records out</th>
                <th className={`${thClass} text-right`}>Late</th>
                <th className={thClass}>Watermark</th>
                <th className={thClass}>Backpressure</th>
              </tr>
            </thead>
            <tbody>
              {operators.map((op, i) => {
                const label = parseStage(op.name);
                const Icon = STAGE_ICONS[label.kind];
                return (
                  <tr key={i}>
                    <td className={tdClass}>
                      <div className="flex items-center gap-2.5">
                        <span className="grid size-7 shrink-0 place-items-center rounded-md bg-slate-100 text-slate-600">
                          <Icon className="size-3.5" />
                        </span>
                        <div className="min-w-0">
                          <div className="font-medium text-slate-900">{label.title}</div>
                          <div className="max-w-xs truncate font-mono text-[11px] text-slate-500" title={label.detail}>
                            {label.detail}
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className={`${tdClass} text-right tabular-nums`}>{compact(perOp[i].inPerSec)}</td>
                    <td className={`${tdClass} text-right font-medium tabular-nums text-slate-900`}>{compact(perOp[i].outPerSec)}</td>
                    <td className={`${tdClass} text-right tabular-nums`}>{op.records_in.toLocaleString()}</td>
                    <td className={`${tdClass} text-right tabular-nums`}>{op.records_out.toLocaleString()}</td>
                    <td className={`${tdClass} text-right tabular-nums ${op.late_records ? "font-medium text-amber-700" : "text-slate-400"}`}>{op.late_records}</td>
                    <td className={`${tdClass} tabular-nums text-slate-600`}>{op.idle ? "idle" : clock(op.watermark)}</td>
                    <td className={tdClass}>
                      <div className="flex items-center gap-2">
                        <div className="h-1.5 w-20 overflow-hidden rounded-full bg-slate-100">
                          <div className="h-full rounded-full bg-amber-500" style={{ width: percent(op.backpressure_ratio) }} />
                        </div>
                        <span className="text-xs text-slate-500 tabular-nums">{percent(op.backpressure_ratio)}</span>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </Card>
      )}

      {tab === "definition" && (
        <Card
          title={details.definition.query ? "SQL query" : "Job definition"}
          description="What was submitted. Use “Edit as new” to change it and deploy a new version."
          actions={
            <Button size="sm" icon={Copy} onClick={editAsNew}>
              Edit as new
            </Button>
          }
        >
          <pre className="overflow-x-auto rounded-lg bg-slate-50 p-4 font-mono text-[13px] leading-relaxed text-slate-800 ring-1 ring-slate-200">
            {details.definition.query ?? JSON.stringify(details.definition, null, 2)}
          </pre>
        </Card>
      )}
    </Page>
  );
}
