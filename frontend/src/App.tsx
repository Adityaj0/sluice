import { Boxes, ChevronDown, User, Code2, Database, History, Home, LineChart, Plus, Search, Workflow } from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { Suspense, lazy, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";

import { cx } from "./components/common/ui.tsx";
import { DataView } from "./components/Data/DataView.tsx";
import { DebuggerView } from "./components/Debugger/DebuggerView.tsx";
import { JobDetailView } from "./components/Jobs/JobDetailView.tsx";
import { JobsView } from "./components/Jobs/JobsView.tsx";
import { MetricsView } from "./components/Metrics/MetricsView.tsx";
import { OverviewView } from "./components/Overview/OverviewView.tsx";
import { BuilderView } from "./components/PipelineBuilder/BuilderView.tsx";
import { usePolling } from "./hooks/usePipeline.ts";
import { useStore } from "./store/pipelineStore.ts";
import type { View } from "./store/pipelineStore.ts";

// Monaco is large: load the SQL editor only when it is opened.
const SqlView = lazy(() => import("./components/SqlEditor/SqlView.tsx").then((m) => ({ default: m.SqlView })));

type NavItem = { view: View; label: string; icon: LucideIcon };
const NAV: { group?: string; items: NavItem[] }[] = [
  { items: [{ view: "overview", label: "Home", icon: Home }] },
  {
    group: "Build",
    items: [
      { view: "data", label: "Data sources", icon: Database },
      { view: "builder", label: "Pipelines", icon: Workflow },
      { view: "sql", label: "SQL editor", icon: Code2 },
    ],
  },
  {
    group: "Operate",
    items: [
      { view: "jobs", label: "Jobs", icon: Boxes },
      { view: "debugger", label: "Debugger", icon: History },
      { view: "metrics", label: "Metrics", icon: LineChart },
    ],
  },
];
const ALL_ITEMS = NAV.flatMap((g) => g.items);

function useOutsideClose(open: boolean, close: () => void) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => ref.current && !ref.current.contains(e.target as Node) && close();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && close();
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open, close]);
  return ref;
}

function Brand() {
  return (
    <div className="flex w-[216px] shrink-0 items-center gap-2 px-4">
      <svg viewBox="0 0 24 24" className="size-6" aria-hidden>
        <rect width="24" height="24" rx="4" fill="#11171c" />
        {/* water held back by a gate, released as an even flow */}
        <path d="M4 9.5c1.5-1.2 3-1.2 4.5 0" stroke="#5aa9e6" strokeWidth="1.8" fill="none" strokeLinecap="round" />
        <path d="M4 14c1.5-1.2 3-1.2 4.5 0" stroke="#5aa9e6" strokeWidth="1.8" fill="none" strokeLinecap="round" />
        <rect x="10.5" y="4.5" width="3" height="11" rx="0.8" fill="#ffffff" />
        <path d="M15.5 17.5h4.5M15.5 13.5h4.5" stroke="#ffffff" strokeWidth="1.8" strokeLinecap="round" />
        <path d="M4 18.5h16" stroke="#5aa9e6" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
      <span className="text-[15px] font-bold tracking-tight text-slate-900">Sluice</span>
    </div>
  );
}

/** Jump to any page or job by name. "/" focuses it. */
function GlobalSearch() {
  const jobs = useStore((s) => s.jobs);
  const navigate = useStore((s) => s.navigate);
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  const ref = useOutsideClose(open, () => setOpen(false));

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const typing = (e.target as HTMLElement).closest("input, textarea, select, .monaco-editor");
      if (e.key === "/" && !typing) {
        e.preventDefault();
        input.current?.focus();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  const q = query.trim().toLowerCase();
  const results: { key: string; label: string; detail: string; icon: LucideIcon; go: () => void }[] = [
    ...ALL_ITEMS.filter((i) => !q || i.label.toLowerCase().includes(q)).map((i) => ({
      key: `page-${i.view}`,
      label: i.label,
      detail: "Page",
      icon: i.icon,
      go: () => navigate(i.view),
    })),
    ...[...jobs]
      .reverse()
      .filter((j) => !q || j.name.toLowerCase().includes(q) || String(j.id) === q)
      .map((j) => ({
        key: `job-${j.id}`,
        label: j.name,
        detail: `Job #${j.id} · ${j.status.state}`,
        icon: j.kind === "sql" ? Code2 : Workflow,
        go: () => navigate("job", j.id),
      })),
  ].slice(0, 9);

  const pick = (go: () => void) => {
    go();
    setQuery("");
    setOpen(false);
    input.current?.blur();
  };

  return (
    <div ref={ref} className="relative w-full max-w-[560px]">
      <Search className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-slate-500" />
      <input
        ref={input}
        value={query}
        onChange={(e) => {
          setQuery(e.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onKeyDown={(e) => e.key === "Enter" && results[0] && pick(results[0].go)}
        placeholder="Search jobs and pages"
        className="h-8 w-full rounded-md border border-slate-200 bg-slate-100 pr-9 pl-8 text-[13px] text-slate-900 outline-none placeholder:text-slate-500 hover:border-slate-300 focus:border-indigo-600 focus:bg-white focus:ring-1 focus:ring-indigo-600"
      />
      <kbd className="pointer-events-none absolute top-1/2 right-2 -translate-y-1/2 rounded-sm border border-slate-300 bg-white px-1.5 font-sans text-[11px] text-slate-500">/</kbd>
      {open && results.length > 0 && (
        <div className="absolute top-9 right-0 left-0 z-50 overflow-hidden rounded-md border border-slate-200 bg-white py-1 shadow-md">
          {results.map((r) => (
            <button
              key={r.key}
              type="button"
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => pick(r.go)}
              className="flex w-full items-center gap-2.5 px-3 py-1.5 text-left text-[13px] hover:bg-slate-100"
            >
              <r.icon className="size-4 text-slate-500" strokeWidth={1.75} />
              <span className="truncate text-slate-900">{r.label}</span>
              <span className="ml-auto shrink-0 text-xs text-slate-500">{r.detail}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function TopBar() {
  const connected = useStore((s) => s.connected);
  return (
    <header className="flex h-12 shrink-0 items-center border-b border-slate-200 bg-white">
      <Brand />
      <div className="flex flex-1 justify-center px-6">
        <GlobalSearch />
      </div>
      <div className="flex items-center gap-4 pr-4 text-[13px]">
        <span className="flex items-center gap-1.5 text-slate-700" title={connected ? "REST and WebSocket on :8080, gRPC ingest on :50051" : "Start it with cargo run --release"}>
          <span className={cx("size-2 rounded-full", connected ? "bg-emerald-500" : "bg-rose-500")} />
          {connected ? "Engine online" : "Engine offline"}
        </span>
        <span className="h-5 w-px bg-slate-200" />
        <span className="text-slate-500">localhost</span>
        <span className="grid size-7 place-items-center rounded-full bg-slate-200 text-slate-600" title="Local user">
          <User className="size-4" strokeWidth={1.75} />
        </span>
      </div>
    </header>
  );
}

function NewMenu() {
  const navigate = useStore((s) => s.navigate);
  const clear = useStore((s) => s.clear);
  const loadExample = useStore((s) => s.loadExample);
  const [open, setOpen] = useState(false);
  const ref = useOutsideClose(open, () => setOpen(false));
  const items: { label: string; detail: string; icon: LucideIcon; go: () => void }[] = [
    { label: "Pipeline", detail: "Visual dataflow", icon: Workflow, go: () => (loadExample(), navigate("builder")) },
    { label: "Empty pipeline", detail: "Start from scratch", icon: Workflow, go: () => (clear(), navigate("builder")) },
    { label: "SQL query", detail: "Streaming SQL job", icon: Code2, go: () => navigate("sql") },
    { label: "Data generator", detail: "Synthetic events", icon: Database, go: () => navigate("data") },
  ];
  return (
    <div ref={ref} className="relative px-3 pt-3 pb-2">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="flex h-8 w-full items-center gap-2 rounded-md border border-slate-300 bg-white px-2.5 text-[13px] font-semibold text-slate-800 hover:bg-slate-50"
      >
        <Plus className="size-4" strokeWidth={2} />
        New
        <ChevronDown className="ml-auto size-3.5 text-slate-500" />
      </button>
      {open && (
        <div className="absolute top-12 right-3 left-3 z-50 rounded-md border border-slate-200 bg-white py-1 shadow-md">
          {items.map((item) => (
            <button
              key={item.label}
              type="button"
              onClick={() => {
                item.go();
                setOpen(false);
              }}
              className="flex w-full items-center gap-2.5 px-3 py-1.5 text-left hover:bg-slate-100"
            >
              <item.icon className="size-4 text-slate-500" strokeWidth={1.75} />
              <span>
                <span className="block text-[13px] text-slate-900">{item.label}</span>
                <span className="block text-[11px] text-slate-500">{item.detail}</span>
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function Sidebar() {
  const view = useStore((s) => s.view);
  const navigate = useStore((s) => s.navigate);
  const running = useStore((s) => s.jobs.filter((j) => j.status.state === "running").length);
  const generators = useStore((s) => s.generators.length);
  const active = (v: View) => view === v || (v === "jobs" && view === "job");
  const count = (v: View) => (v === "jobs" && running ? running : v === "data" && generators ? generators : null);

  return (
    <aside className="flex w-[216px] shrink-0 flex-col border-r border-slate-200 bg-slate-50">
      <NewMenu />
      <nav className="flex flex-col px-2 pb-3">
        {NAV.map((section, i) => (
          <div key={section.group ?? i} className={cx(i > 0 && "mt-2 border-t border-slate-200 pt-2")}>
            {section.group && <div className="px-2 pt-1 pb-1 text-[11px] font-semibold text-slate-500">{section.group}</div>}
            {section.items.map(({ view: v, label, icon: Icon }) => (
              <button
                key={v}
                type="button"
                onClick={() => navigate(v)}
                className={cx(
                  "flex h-8 w-full items-center gap-2.5 rounded-md px-2 text-left text-[13px] transition-colors",
                  active(v) ? "bg-indigo-100/70 font-semibold text-indigo-800" : "text-slate-700 hover:bg-slate-200/60 hover:text-slate-900",
                )}
              >
                <Icon className={cx("size-4", active(v) ? "text-indigo-700" : "text-slate-500")} strokeWidth={1.75} />
                {label}
                {count(v) != null && <span className={cx("ml-auto text-xs tabular-nums", active(v) ? "text-indigo-700" : "text-slate-500")}>{count(v)}</span>}
              </button>
            ))}
          </div>
        ))}
      </nav>
      <div className="mt-auto border-t border-slate-200 px-4 py-3 text-[11px] leading-4 text-slate-500">
        Sluice 0.1.0
        <br />
        Exactly-once stream processing
      </div>
    </aside>
  );
}

export default function App() {
  usePolling();
  const view = useStore((s) => s.view);
  const pages: Record<View, ReactNode> = {
    overview: <OverviewView />,
    data: <DataView />,
    builder: <BuilderView />,
    sql: (
      <Suspense fallback={<div className="p-6 text-[13px] text-slate-500">Loading the SQL editor…</div>}>
        <SqlView />
      </Suspense>
    ),
    jobs: <JobsView />,
    job: <JobDetailView />,
    debugger: <DebuggerView />,
    metrics: <MetricsView />,
  };
  const fullBleed = view === "builder" || view === "sql";
  return (
    <div className="flex h-full flex-col">
      <TopBar />
      <div className="flex min-h-0 flex-1">
        <Sidebar />
        <main className={cx("min-w-0 flex-1 bg-white", fullBleed ? "overflow-hidden" : "overflow-y-auto")}>{pages[view]}</main>
      </div>
    </div>
  );
}
