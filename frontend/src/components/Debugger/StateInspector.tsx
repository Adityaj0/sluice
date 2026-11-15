import { bytes, clock, compact } from "../../lib/format.ts";
import { parseStage } from "../../lib/stages.ts";
import type { InspectedCheckpoint, InspectedStage } from "../../types/pipeline.ts";
import { Badge } from "../common/ui.tsx";
import { STAGE_ICONS } from "../Jobs/stageIcons.tsx";
import { KeyBrowser } from "./KeyBrowser.tsx";

function Fact({ label, value, hint }: { label: string; value: React.ReactNode; hint?: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <div className="text-xs text-slate-500">{label}</div>
      <div className="mt-0.5 truncate text-[15px] font-semibold text-slate-900 tabular-nums">{value}</div>
      {hint && <div className="truncate text-xs text-slate-400">{hint}</div>}
    </div>
  );
}

export function StateInspector({ checkpoint, previous }: { checkpoint: InspectedCheckpoint; previous?: InspectedCheckpoint }) {
  const before = (stage: InspectedStage) => previous?.stages.find((s) => s.uid === stage.uid);
  return (
    <div className="space-y-4">
      {checkpoint.stages.map((stage) => {
        const prior = before(stage);
        const source = stage.operator_state?.source;
        const window = stage.operator_state?.window;
        if (!source && !window && !stage.keyed_state) return null;
        const label = parseStage(stage.name);
        const Icon = STAGE_ICONS[label.kind];
        return (
          <section key={stage.uid} className="rounded-xl border border-slate-200 bg-white">
            <header className="flex items-center gap-3 border-b border-slate-100 px-5 py-3">
              <span className="grid size-8 place-items-center rounded-lg bg-slate-100 text-slate-600">
                <Icon className="size-4" />
              </span>
              <div className="min-w-0 flex-1">
                <div className="text-sm font-semibold text-slate-900">{label.title}</div>
                <div className="truncate font-mono text-xs text-slate-500">{label.detail}</div>
              </div>
              {stage.keyed_state && <Badge tone="indigo">{stage.keyed_state.backend === "rocksdb" ? "RocksDB" : "Memory"}</Badge>}
              <span className="text-xs text-slate-500">{bytes(stage.size)}</span>
            </header>
            <div className="space-y-5 px-5 py-4">
              {source && (
                <div className="grid grid-cols-3 gap-6">
                  <Fact
                    label="Read position"
                    value={compact(source.position)}
                    hint={
                      prior?.operator_state?.source
                        ? `+${compact(source.position - prior.operator_state.source.position)} since #${previous?.checkpoint}`
                        : "replayed from here after a crash"
                    }
                  />
                  <Fact label="Newest event time" value={clock(source.max_seen, true)} />
                  <Fact label="Watermark" value={clock(source.last_watermark, true)} />
                </div>
              )}
              {window && (
                <div className="grid grid-cols-3 gap-6">
                  <Fact label="Watermark" value={clock(window.watermark, true)} />
                  <Fact label="Last window emitted" value={window.fired_through != null ? `ends ${clock(window.fired_through)}` : "none yet"} />
                  <Fact label="Late records dropped" value={window.late_records.toLocaleString()} />
                </div>
              )}
              {stage.keyed_state?.states
                .filter((s) => !s.internal)
                .map((state) => (
                  <div key={state.name}>
                    <div className="mb-2 text-[13px] text-slate-600">
                      <span className="font-semibold text-slate-900">
                        {state.total.toLocaleString()} {state.kind === "sessions" ? "open sessions" : "open window panes"}
                      </span>
                      {state.truncated && <span> · showing {state.entries.length}</span>}
                    </div>
                    <KeyBrowser
                      entries={state.entries}
                      previous={prior?.keyed_state?.states.find((s) => s.name === state.name)?.entries ?? (prior ? [] : undefined)}
                      compared={previous?.checkpoint ?? null}
                    />
                  </div>
                ))}
            </div>
          </section>
        );
      })}
    </div>
  );
}
