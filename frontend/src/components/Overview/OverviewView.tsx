import { Activity, ArrowRight, Boxes, Check, Code2, Database, Play, RotateCcw, ShieldCheck, Workflow } from "lucide-react";
import { useState } from "react";

import { api } from "../../api.ts";
import { compact } from "../../lib/format.ts";
import { useStore } from "../../store/pipelineStore.ts";
import { Button, Card, EmptyState, Metric, Page, PageHeader, Sparkline, cx } from "../common/ui.tsx";
import { JobsTable } from "../Jobs/JobsTable.tsx";
import { SERIES, TimeSeriesChart } from "../Metrics/charts.tsx";

export const SAMPLE_GENERATOR = { stream: "purchases", rate: 1000, users: 50, max_disorder_ms: 1000 };

export function useStartSampleData() {
  const refresh = useStore((s) => s.refresh);
  const [busy, setBusy] = useState(false);
  return {
    busy,
    start: async () => {
      setBusy(true);
      try {
        await api.startGenerator(SAMPLE_GENERATOR);
        await refresh();
      } finally {
        setBusy(false);
      }
    },
  };
}

function Step({ n, done, title, text, children }: { n: number; done: boolean; title: string; text: string; children?: React.ReactNode }) {
  return (
    <div className="flex gap-3.5">
      <div
        className={cx(
          "grid size-6 shrink-0 place-items-center rounded-full text-xs font-semibold",
          done ? "bg-emerald-600 text-white" : "border border-slate-300 bg-white text-slate-700",
        )}
      >
        {done ? <Check className="size-4" strokeWidth={3} /> : n}
      </div>
      <div className="min-w-0">
        <div className={cx("text-sm font-semibold", done ? "text-slate-500" : "text-slate-900")}>{title}</div>
        <p className="mt-0.5 text-[13px] leading-relaxed text-slate-500">{text}</p>
        {!done && children && <div className="mt-3 flex flex-wrap gap-2">{children}</div>}
      </div>
    </div>
  );
}

function GettingStarted() {
  const { jobs, streams, generators, navigate } = useStore();
  const sample = useStartSampleData();
  const hasData = generators.length > 0 || streams.some((s) => s.next_offset > 0);
  const hasJob = jobs.length > 0;
  const running = jobs.find((j) => j.status.state === "running");
  if (hasData && hasJob && running) return null;
  return (
    <section className="rounded-md border border-slate-200 bg-slate-50 p-5">
      <div className="mb-5">
        <h2 className="text-base font-semibold text-slate-900">Get started in three steps</h2>
        <p className="mt-0.5 text-[13px] text-slate-500">Everything runs from this UI: stream data in, deploy a job, then watch, break and inspect it.</p>
      </div>
      <div className="grid gap-6 md:grid-cols-3">
        <Step n={1} done={hasData} title="Stream some data" text="Start a generator of synthetic purchases (user, amount, item) on the stream “purchases”.">
          <Button variant="primary" size="sm" icon={Play} onClick={() => void sample.start()} disabled={sample.busy}>
            Start sample data
          </Button>
        </Step>
        <Step n={2} done={hasJob} title="Deploy a job" text="Drag operators into a pipeline, or describe it in streaming SQL.">
          <Button size="sm" icon={Workflow} onClick={() => navigate("builder")}>
            Build a pipeline
          </Button>
          <Button size="sm" icon={Code2} onClick={() => navigate("sql")}>
            Write SQL
          </Button>
        </Step>
        <Step n={3} done={Boolean(running)} title="Watch it run" text="See events flow live, crash an operator on purpose, and step back through checkpoints.">
          {hasJob && (
            <Button size="sm" icon={ArrowRight} onClick={() => navigate("jobs")}>
              Open jobs
            </Button>
          )}
        </Step>
      </div>
    </section>
  );
}

function IngestChart({ points }: { points: { ts: number; rate: number }[] }) {
  if (points.length < 2) return <div className="grid h-full place-items-center text-[13px] text-slate-400">Collecting samples…</div>;
  return <TimeSeriesChart rows={points} series={[{ key: "rate", name: "Events / s", color: SERIES.primary, area: true }]} />;
}

export function OverviewView() {
  const { jobs, streams, streamRates, generators, navigate } = useStore();
  const sample = useStartSampleData();
  const ingest = Object.values(streamRates).reduce((a, b) => a + b, 0);
  const ingestHistory = useStore((s) => s.ingestHistory);
  const running = jobs.filter((j) => j.status.state === "running").length;
  const checkpoints = jobs.reduce((sum, j) => sum + j.checkpoints_completed, 0);
  const restarts = jobs.reduce((sum, j) => sum + j.restarts, 0);

  return (
    <Page>
      <PageHeader
        title="Overview"
        description="Your streaming engine at a glance."
        actions={
          <>
            {generators.length === 0 && (
              <Button icon={Play} onClick={() => void sample.start()} disabled={sample.busy}>
                Start sample data
              </Button>
            )}
            <Button variant="primary" icon={Workflow} onClick={() => navigate("builder")}>
              New pipeline
            </Button>
          </>
        }
      />
      <GettingStarted />
      <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">
        <Metric
          label="Events ingested"
          icon={Activity}
          value={compact(ingest)}
          unit="/s"
          hint={`${compact(streams.reduce((a, s) => a + s.next_offset, 0))} events in total`}
          trend={<Sparkline values={ingestHistory.slice(-40).map((p) => p.rate)} />}
        />
        <Metric label="Running jobs" icon={Boxes} value={running} hint={`${jobs.length} submitted`} />
        <Metric label="Checkpoints" icon={ShieldCheck} value={compact(checkpoints)} hint="completed across all jobs" />
        <Metric
          label="Recoveries"
          icon={RotateCcw}
          value={restarts}
          tone={restarts ? "amber" : "slate"}
          hint={restarts ? "restored from a checkpoint after a failure" : "no failures so far"}
        />
      </div>
      <div className="grid gap-4 xl:grid-cols-3">
        <Card className="xl:col-span-2" title="Event throughput" description="Events per second arriving across all streams." bodyClassName="h-64 px-3 pt-3 pb-2">
          <IngestChart points={ingestHistory} />
        </Card>
        <Card
          title="Streams"
          description="Where events arrive; jobs read from them."
          bodyClassName="p-2"
          actions={
            <Button size="sm" variant="ghost" onClick={() => navigate("data")}>
              Manage
            </Button>
          }
        >
          {streams.length ? (
            <ul className="divide-y divide-slate-100">
              {streams.map((s) => (
                <li key={s.name} className="flex items-center gap-3 px-3 py-3">
                  <Database className="size-4 shrink-0 text-slate-500" strokeWidth={1.75} />
                  <div className="min-w-0 flex-1">
                    <div className="truncate font-mono text-[13px] font-medium text-slate-900">{s.name}</div>
                    <div className="text-xs text-slate-500">
                      {s.subscribers} job{s.subscribers === 1 ? "" : "s"} reading · {compact(s.next_offset)} events
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-sm font-semibold text-slate-900 tabular-nums">{compact(streamRates[s.name] ?? 0)}/s</div>
                    <div className="text-xs text-slate-400">ingest</div>
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <EmptyState icon={Database} title="No streams yet">
              A stream appears when a job reads from it.
            </EmptyState>
          )}
        </Card>
      </div>
        <Card
          title="Jobs"
          description="Pipelines and SQL queries deployed on this engine."
          bodyClassName=""
          actions={
            jobs.length > 0 && (
              <Button size="sm" variant="ghost" onClick={() => navigate("jobs")}>
                View all
              </Button>
            )
          }
        >
          {jobs.length ? (
            <JobsTable jobs={jobs.slice(-6)} />
          ) : (
            <EmptyState icon={Boxes} title="No jobs yet">
              Deploy one from the pipeline builder or the SQL editor.
            </EmptyState>
          )}
        </Card>
    </Page>
  );
}
