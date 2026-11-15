import { AlertCircle, CheckCircle2, Loader2, Rocket } from "lucide-react";
import { useEffect, useState } from "react";

import { api } from "../../api.ts";
import { useSubmitJob } from "../../hooks/usePipeline.ts";
import { useStore } from "../../store/pipelineStore.ts";
import type { ExplainResult, SqlError } from "../../types/pipeline.ts";
import { Button, Callout, Input, Select, Toggle, cx } from "../common/ui.tsx";
import { Editor } from "./Editor.tsx";
import { PlanPreview } from "./PlanPreview.tsx";

const EXAMPLES: Record<string, string> = {
  "Big spenders per minute": `-- Users with more than 10 purchases in a minute
SELECT user_id, COUNT(*) AS tx_count, SUM(amount) AS total, WINDOW_START AS minute
FROM purchases
WHERE amount > 0
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY user_id
HAVING COUNT(*) > 10`,
  "Sliding average by item": `SELECT item, AVG(amount) AS avg_amount, COUNT(*) AS n, WINDOW_END AS until
FROM purchases
WINDOW SLIDING (SIZE 5 MINUTES, SLIDE 1 MINUTE) ALLOWED LATENESS 10 SECONDS
GROUP BY item`,
  "Shopping sessions": `SELECT user_id, COUNT(*) AS purchases, MAX(amount) AS biggest,
       WINDOW_START AS started, WINDOW_END AS ended
FROM purchases
WINDOW SESSION (GAP 30 SECONDS)
GROUP BY user_id`,
  "Optimizer showcase": `-- Constant folding, HAVING pushdown, scan pruning
SELECT user_id, SUM(amount * 100) / 100 AS total
FROM purchases
WHERE amount > 2 * 5 AND 1 = 1
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY user_id
HAVING user_id != 'user-000' AND SUM(amount) > 100`,
  "Stateless transform": `SELECT UPPER(user_id) AS user, ROUND(amount * 1.2, 2) AS with_tax, item
FROM purchases
WHERE item = 'book' OR amount >= 150`,
};

export function SqlView() {
  const sqlDraft = useStore((s) => s.sqlDraft);
  const setSqlDraft = useStore((s) => s.setSqlDraft);
  const [sql, setSql] = useState(sqlDraft ?? EXAMPLES["Big spenders per minute"]);
  const [result, setResult] = useState<ExplainResult | null>(null);
  const [error, setError] = useState<SqlError | null>(null);
  const [compiling, setCompiling] = useState(true);
  const [name, setName] = useState("sql-job");
  const [disorderMs, setDisorderMs] = useState(2000);
  const [checkpointing, setCheckpointing] = useState(true);
  const { submit, submitting, error: submitError } = useSubmitJob();

  useEffect(() => {
    if (sqlDraft) setSqlDraft(null);
  }, [sqlDraft, setSqlDraft]);

  // Live plan preview: compile on every pause in typing.
  useEffect(() => {
    let alive = true;
    setCompiling(true);
    const timer = setTimeout(async () => {
      const response = await api.explain(sql);
      if (!alive) return;
      setCompiling(false);
      if (response.ok) {
        setResult(response.result);
        setError(null);
      } else {
        setError(response.error);
      }
    }, 300);
    return () => {
      alive = false;
      clearTimeout(timer);
    };
  }, [sql]);

  const deploy = () =>
    result &&
    void submit({
      name,
      source: { type: "grpc", stream: result.stream, max_out_of_orderness_ms: disorderMs },
      query: sql,
      sink: { type: "buffer", capacity: 5000 },
      ...(checkpointing ? { checkpointing: { interval_ms: 1000, backend: "rocksdb", retained: 10 } } : {}),
    });

  const status = compiling ? (
    <span className="inline-flex items-center gap-1.5 text-[13px] text-slate-600">
      <Loader2 className="size-3.5 animate-spin" /> Compiling
    </span>
  ) : error ? (
    <span className="inline-flex items-center gap-1.5 text-[13px] text-slate-800">
      <AlertCircle className="size-4 text-rose-600" /> {error.kind === "syntax" ? `Syntax error on line ${error.line}` : "Can't plan this query"}
    </span>
  ) : (
    <span className="inline-flex items-center gap-1.5 text-[13px] text-slate-800">
      <CheckCircle2 className="size-4 text-emerald-600" /> Compiles · reads “{result?.stream}”
    </span>
  );

  return (
    <div className="flex h-full flex-col">
      <header className="flex h-16 shrink-0 items-center gap-4 border-b border-slate-200 bg-white px-6">
        <div className="min-w-0">
          <div className="text-xs text-slate-500">SQL editor</div>
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            aria-label="Job name"
            className="-ml-1 w-60 rounded-md px-1 text-[15px] font-semibold text-slate-900 outline-none hover:bg-slate-50 focus:bg-slate-50 focus:ring-2 focus:ring-indigo-500/20"
          />
        </div>
        {status}
        <div className="ml-auto flex items-center gap-2">
          <Select className="w-56" value="" onChange={(e) => e.target.value && setSql(EXAMPLES[e.target.value])}>
            <option value="">Load an example…</option>
            {Object.keys(EXAMPLES).map((k) => (
              <option key={k}>{k}</option>
            ))}
          </Select>
          <Button variant="primary" icon={Rocket} disabled={!result || !!error || submitting} onClick={deploy}>
            {submitting ? "Deploying…" : "Run query"}
          </Button>
        </div>
      </header>

      <div className="grid min-h-0 flex-1 grid-cols-[1fr_440px]">
        <div className="flex min-h-0 flex-col">
          <div className="relative min-h-0 flex-1 bg-white">
            <div className="absolute inset-0">
              <Editor value={sql} onChange={setSql} error={error} />
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-8 border-t border-slate-200 bg-slate-50 px-6 py-3">
            <label className="flex items-center gap-2 text-[13px] whitespace-nowrap text-slate-700">
              <span className="font-semibold">Out-of-order tolerance</span>
              <span className="w-24">
                <Input type="number" min={0} className="tabular-nums" value={disorderMs} onChange={(e) => setDisorderMs(Number(e.target.value))} />
              </span>
              ms
            </label>
            <div className="w-80">
              <Toggle checked={checkpointing} onChange={setCheckpointing} label="Fault tolerance" description="RocksDB checkpoints, exactly-once output" />
            </div>
            {submitError && <div className="w-full"><Callout>{submitError}</Callout></div>}
          </div>
        </div>
        <aside className="min-h-0 overflow-y-auto border-l border-slate-200 bg-white p-5">
          <h2 className="mb-1 text-sm font-semibold text-slate-900">Query plan</h2>
          <p className="mb-5 text-[13px] text-slate-500">Recompiled as you type.</p>
          {error && (
            <div className="mb-5">
              <Callout title={error.kind === "syntax" ? `Line ${error.line}, column ${error.column}` : "Planning error"}>{error.message}</Callout>
            </div>
          )}
          {result && (
            <div className={cx(error && "opacity-40")}>
              <PlanPreview result={result} />
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}
