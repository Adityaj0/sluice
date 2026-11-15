import { History } from "lucide-react";
import { useEffect, useState } from "react";

import { api } from "../../api.ts";
import { bytes, clock, duration } from "../../lib/format.ts";
import { useStore } from "../../store/pipelineStore.ts";
import type { CheckpointsView, InspectedCheckpoint } from "../../types/pipeline.ts";
import { Callout, Card, EmptyState, JobPicker, Metric, Page, PageHeader, Toggle } from "../common/ui.tsx";
import { StateInspector } from "./StateInspector.tsx";
import { Timeline } from "./Timeline.tsx";

export function DebuggerView() {
  const jobId = useStore((s) => s.jobId);
  const [history, setHistory] = useState<CheckpointsView | null>(null);
  const [selected, setSelected] = useState<number | null>(null);
  const [follow, setFollow] = useState(true);
  const [state, setState] = useState<{ current?: InspectedCheckpoint; previous?: InspectedCheckpoint; error?: string }>({});

  useEffect(() => {
    setHistory(null);
    setSelected(null);
    setFollow(true);
    setState({});
    if (jobId == null) return;
    let alive = true;
    const load = () =>
      api
        .checkpoints(jobId)
        .then((h) => alive && setHistory(h))
        .catch(() => {});
    void load();
    const timer = setInterval(load, 2000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [jobId]);

  const retained = history?.retained ?? [];
  const newest = retained[retained.length - 1] ?? null;
  const shown = follow ? newest : selected;
  const previousId = shown == null ? null : (retained[retained.indexOf(shown) - 1] ?? null);
  const record = history?.records.find((r) => r.id === shown);

  useEffect(() => {
    if (jobId == null || shown == null) return;
    let alive = true;
    Promise.all([api.checkpointState(jobId, shown), previousId == null ? undefined : api.checkpointState(jobId, previousId).catch(() => undefined)])
      .then(([current, previous]) => alive && setState({ current, previous }))
      .catch((e: Error) => alive && setState({ error: e.message }));
    return () => {
      alive = false;
    };
  }, [jobId, shown, previousId]);

  return (
    <Page>
      <PageHeader
        title="Time-travel debugger"
        description="Every checkpoint is a consistent snapshot of a job. Step through them to see exactly what each window held, and what changed."
        actions={<JobPicker />}
      />
      {jobId == null ? (
        <Card>
          <EmptyState icon={History} title="No jobs yet">
            Deploy a job with fault tolerance on, then come back to inspect its state.
          </EmptyState>
        </Card>
      ) : (
        <>
          <Card
            title="Checkpoints"
            description={history ? `${history.completed.toLocaleString()} completed · ${history.failed} failed · ${retained.length} kept on disk` : undefined}
            actions={
              <div className="w-48">
                <Toggle checked={follow} onChange={setFollow} label="Follow latest" />
              </div>
            }
          >
            {!history ? (
              <div className="text-sm text-slate-500">Loading…</div>
            ) : history.records.length === 0 ? (
              <EmptyState icon={History} title="No checkpoints yet">
                This job doesn't checkpoint, or hasn't completed one. Deploy with fault tolerance on to travel through its state.
              </EmptyState>
            ) : (
              <Timeline
                records={history.records}
                retained={retained}
                selected={shown}
                onSelect={(id) => {
                  setFollow(false);
                  setSelected(id);
                }}
              />
            )}
          </Card>
          {state.error && <Callout>{state.error}</Callout>}
          {state.current && shown != null && (
            <>
              <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">
                <Metric label="Checkpoint" value={`#${state.current.checkpoint}`} hint={`completed ${clock(state.current.completed_at, true)}`} />
                <Metric label="Took" value={duration(record?.duration_ms ?? null)} hint="barrier to manifest" />
                <Metric label="State size" value={bytes(state.current.size)} hint={record ? `${bytes(record.uploaded_bytes)} newly written` : undefined} />
                <Metric label="Compared with" value={state.previous ? `#${state.previous.checkpoint}` : "–"} hint="the previous checkpoint on disk" />
              </div>
              <StateInspector checkpoint={state.current} previous={state.previous} />
            </>
          )}
        </>
      )}
    </Page>
  );
}
