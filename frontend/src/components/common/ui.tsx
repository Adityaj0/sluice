// The design system: a small set of components every page is built from,
// so spacing, type and color stay consistent.

import { Ban, CheckCircle2, LoaderCircle, XCircle } from "lucide-react";
import type { LucideIcon } from "lucide-react";
import type { ButtonHTMLAttributes, InputHTMLAttributes, ReactNode, SelectHTMLAttributes, TextareaHTMLAttributes } from "react";

import { useStore } from "../../store/pipelineStore.ts";
import type { JobStatus } from "../../types/pipeline.ts";

export function cx(...classes: (string | false | null | undefined)[]) {
  return classes.filter(Boolean).join(" ");
}

// ---- Layout ----------------------------------------------------------------

export function Page({ children, wide = false }: { children: ReactNode; wide?: boolean }) {
  return <div className={cx("mx-auto flex w-full flex-col gap-5 px-6 py-5", wide ? "max-w-none" : "max-w-[1400px]")}>{children}</div>;
}

export function PageHeader({
  title,
  description,
  actions,
  eyebrow,
}: {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  eyebrow?: ReactNode;
}) {
  return (
    <header className="flex flex-wrap items-start justify-between gap-4">
      <div className="min-w-0">
        {eyebrow && <div className="mb-1 text-[13px] text-slate-500">{eyebrow}</div>}
        <h1 className="text-[22px] leading-7 font-semibold text-slate-900">{title}</h1>
        {description && <p className="mt-1 max-w-3xl text-[13px] text-slate-500">{description}</p>}
      </div>
      {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
    </header>
  );
}

export function Card({
  title,
  description,
  actions,
  children,
  className = "",
  bodyClassName = "p-4",
}: {
  title?: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
}) {
  return (
    <section className={cx("flex min-h-0 flex-col rounded-md border border-slate-200 bg-white", className)}>
      {(title || actions) && (
        <header className="flex items-center justify-between gap-3 border-b border-slate-200 px-4 py-2.5">
          <div className="min-w-0">
            <h2 className="text-[14px] font-semibold text-slate-900">{title}</h2>
            {description && <p className="mt-0.5 text-xs text-slate-500">{description}</p>}
          </div>
          {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
        </header>
      )}
      <div className={cx("min-h-0 flex-auto", bodyClassName)}>{children}</div>
    </section>
  );
}

// ---- Controls --------------------------------------------------------------

type Variant = "primary" | "secondary" | "ghost" | "danger";
const variants: Record<Variant, string> = {
  primary: "border border-indigo-600 bg-indigo-600 text-white hover:border-indigo-700 hover:bg-indigo-700 disabled:border-indigo-300 disabled:bg-indigo-300",
  secondary: "border border-slate-300 bg-white text-slate-800 hover:border-slate-400 hover:bg-slate-50 disabled:text-slate-400",
  ghost: "border border-transparent text-indigo-700 hover:bg-indigo-50 disabled:text-slate-400",
  danger: "border border-rose-500 bg-white text-rose-600 hover:bg-rose-50 disabled:border-rose-200 disabled:text-rose-300",
};

export function Button({
  variant = "secondary",
  size = "md",
  icon: Icon,
  className = "",
  children,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: Variant; size?: "sm" | "md"; icon?: LucideIcon }) {
  return (
    <button
      type="button"
      className={cx(
        "inline-flex items-center justify-center gap-1.5 rounded-md font-medium whitespace-nowrap transition-colors outline-none focus-visible:ring-2 focus-visible:ring-indigo-500/40 disabled:cursor-not-allowed",
        size === "sm" ? "h-7 px-2.5 text-xs" : "h-8 px-3 text-[13px]",
        variants[variant],
        className,
      )}
      {...props}
    >
      {Icon && <Icon className={size === "sm" ? "size-3.5" : "size-4"} strokeWidth={1.75} />}
      {children}
    </button>
  );
}

export const controlClass =
  "h-8 w-full rounded-md border border-slate-300 bg-white px-2.5 text-[13px] text-slate-900 outline-none transition placeholder:text-slate-400 hover:border-slate-400 focus:border-indigo-600 focus:ring-1 focus:ring-indigo-600 disabled:bg-slate-50 disabled:text-slate-500";

export function Input(props: InputHTMLAttributes<HTMLInputElement>) {
  return <input {...props} className={cx(controlClass, props.className)} />;
}

export function Select({ children, ...props }: SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <select {...props} className={cx(controlClass, "pr-8", props.className)}>
      {children}
    </select>
  );
}

export function Textarea(props: TextareaHTMLAttributes<HTMLTextAreaElement>) {
  return (
    <textarea
      {...props}
      className={cx(
        "w-full rounded-md border border-slate-300 bg-white px-2.5 py-2 font-mono text-xs leading-5 text-slate-900 outline-none hover:border-slate-400 focus:border-indigo-600 focus:ring-1 focus:ring-indigo-600",
        props.className,
      )}
    />
  );
}

export function Field({ label, hint, children, className = "" }: { label: string; hint?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <label className={cx("block", className)}>
      <span className="mb-1 block text-[13px] font-semibold text-slate-800">{label}</span>
      {children}
      {hint && <span className="mt-1 block text-xs leading-relaxed text-slate-500">{hint}</span>}
    </label>
  );
}

export function Toggle({ checked, onChange, label, description }: { checked: boolean; onChange: (v: boolean) => void; label: string; description?: string }) {
  return (
    <button type="button" onClick={() => onChange(!checked)} className="flex w-full items-start gap-3 text-left">
      <span className={cx("relative mt-0.5 inline-flex h-4 w-7 shrink-0 rounded-full transition-colors", checked ? "bg-indigo-600" : "bg-slate-200")}>
        <span className={cx("absolute top-0.5 size-3 rounded-full bg-white transition-transform", checked ? "translate-x-3.5" : "translate-x-0.5")} />
      </span>
      <span>
        <span className="block text-[13px] font-semibold text-slate-800">{label}</span>
        {description && <span className="block text-xs text-slate-500">{description}</span>}
      </span>
    </button>
  );
}

export function Tabs<T extends string>({ value, onChange, tabs }: { value: T; onChange: (v: T) => void; tabs: { id: T; label: ReactNode }[] }) {
  return (
    <div className="flex gap-5 border-b border-slate-200">
      {tabs.map((tab) => (
        <button
          key={tab.id}
          type="button"
          onClick={() => onChange(tab.id)}
          className={cx(
            "-mb-px border-b-[3px] pb-2 text-[13px] font-semibold transition-colors",
            value === tab.id ? "border-indigo-600 text-slate-900" : "border-transparent text-slate-500 hover:text-indigo-700",
          )}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}

export function Segmented<T extends string>({ value, onChange, options }: { value: T; onChange: (v: T) => void; options: { id: T; label: string }[] }) {
  return (
    <div className="inline-flex overflow-hidden rounded-md border border-slate-300">
      {options.map((o) => (
        <button
          key={o.id}
          type="button"
          onClick={() => onChange(o.id)}
          className={cx(
            "h-7 border-r border-slate-300 px-3 text-xs font-medium transition last:border-r-0",
            value === o.id ? "bg-indigo-50 text-indigo-700" : "bg-white text-slate-600 hover:bg-slate-50",
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

// ---- Display ---------------------------------------------------------------

type Tone = "slate" | "indigo" | "emerald" | "amber" | "rose" | "sky" | "violet";
const tones: Record<Tone, string> = {
  slate: "bg-slate-100 text-slate-700",
  indigo: "bg-indigo-50 text-indigo-700",
  emerald: "bg-emerald-50 text-emerald-700",
  amber: "bg-amber-50 text-amber-800",
  rose: "bg-rose-50 text-rose-700",
  sky: "bg-slate-100 text-slate-700",
  violet: "bg-slate-100 text-slate-700",
};

export function Badge({ tone = "slate", children, dot = false }: { tone?: Tone; children: ReactNode; dot?: boolean }) {
  return (
    <span className={cx("inline-flex h-5 items-center gap-1.5 rounded-sm px-1.5 text-xs font-medium whitespace-nowrap", tones[tone])}>
      {dot && <span className="size-1.5 rounded-full bg-current" />}
      {children}
    </span>
  );
}

const STATUS: Record<string, { label: string; icon: LucideIcon; color: string; spin?: boolean }> = {
  running: { label: "Running", icon: LoaderCircle, color: "text-indigo-600", spin: true },
  finished: { label: "Succeeded", icon: CheckCircle2, color: "text-emerald-600" },
  failed: { label: "Failed", icon: XCircle, color: "text-rose-600" },
  cancelled: { label: "Cancelled", icon: Ban, color: "text-slate-500" },
};

export function StatusBadge({ status }: { status: JobStatus }) {
  const s = STATUS[status.state];
  const Icon = s.icon;
  return (
    <span title={status.error} className="inline-flex items-center gap-1.5 text-[13px] whitespace-nowrap text-slate-800">
      <Icon className={cx("size-4", s.color, s.spin && "animate-[spin_2.5s_linear_infinite]")} strokeWidth={2} />
      {s.label}
    </span>
  );
}

export function KindBadge({ kind }: { kind: "sql" | "pipeline" }) {
  return <Badge tone="slate">{kind === "sql" ? "SQL query" : "Pipeline"}</Badge>;
}

export function Metric({
  label,
  value,
  unit,
  hint,
  tone = "slate",
  trend,
}: {
  label: string;
  value: ReactNode;
  unit?: string;
  hint?: ReactNode;
  /** Accepted for call-site compatibility; the compact tile shows no icon. */
  icon?: LucideIcon;
  tone?: "slate" | "amber" | "rose" | "emerald";
  trend?: ReactNode;
}) {
  const valueTone = { slate: "text-slate-900", amber: "text-amber-700", rose: "text-rose-700", emerald: "text-emerald-700" }[tone];
  return (
    <div className="flex min-w-0 flex-col rounded-md border border-slate-200 bg-white px-4 py-3">
      <div className="text-xs font-semibold text-slate-500">{label}</div>
      <div className="mt-1.5 flex items-end justify-between gap-3">
        <div className={cx("shrink-0 text-[22px] leading-7 font-semibold whitespace-nowrap tabular-nums", valueTone)}>
          {value}
          {unit && <span className="ml-1 text-[13px] font-normal text-slate-500">{unit}</span>}
        </div>
        {trend && <div className="flex min-w-0 justify-end overflow-hidden">{trend}</div>}
      </div>
      {hint && <div className="mt-1 truncate text-xs text-slate-500">{hint}</div>}
    </div>
  );
}

export function Sparkline({ values, width = 96, height = 28, color = "#2272b4" }: { values: number[]; width?: number; height?: number; color?: string }) {
  if (values.length < 2) return <svg width={width} height={height} />;
  const max = Math.max(...values, 1);
  const step = width / (values.length - 1);
  const points = values.map((v, i) => `${(i * step).toFixed(1)},${(height - 2 - (v / max) * (height - 4)).toFixed(1)}`);
  const area = `0,${height} ${points.join(" ")} ${width},${height}`;
  const gradient = `spark-${color.slice(1)}`;
  return (
    <svg width={width} height={height} className="shrink-0 overflow-visible">
      <defs>
        <linearGradient id={gradient} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.18" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <polygon points={area} fill={`url(#${gradient})`} />
      <polyline points={points.join(" ")} fill="none" stroke={color} strokeWidth="1.5" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

export function EmptyState({
  icon: Icon,
  title,
  children,
  action,
}: {
  icon?: LucideIcon;
  title: string;
  children?: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="flex h-full flex-col items-center justify-center px-6 py-12 text-center">
      {Icon && (
        <div className="mb-3 text-slate-400">
          <Icon className="size-7" strokeWidth={1.5} />
        </div>
      )}
      <div className="text-[14px] font-semibold text-slate-900">{title}</div>
      {children && <div className="mt-1 max-w-sm text-[13px] leading-relaxed text-slate-500">{children}</div>}
      {action && <div className="mt-5 flex gap-2">{action}</div>}
    </div>
  );
}

export function Callout({ tone = "rose", title, children }: { tone?: "rose" | "amber" | "indigo" | "emerald"; title?: ReactNode; children: ReactNode }) {
  const styles = {
    rose: "border-rose-200 bg-rose-50 text-rose-800",
    amber: "border-amber-200 bg-amber-50 text-amber-900",
    indigo: "border-indigo-200 bg-indigo-50 text-indigo-900",
    emerald: "border-emerald-200 bg-emerald-50 text-emerald-900",
  }[tone];
  return (
    <div className={cx("rounded-md border px-3 py-2.5 text-[13px]", styles)}>
      {title && <div className="mb-0.5 font-semibold">{title}</div>}
      <div className="leading-relaxed">{children}</div>
    </div>
  );
}

export const tableClass = "w-full text-left text-[13px]";
export const thClass = "h-9 border-b border-slate-200 bg-white px-3 text-xs font-semibold whitespace-nowrap text-slate-600";
export const tdClass = "h-10 border-b border-slate-200 px-3 py-1.5 text-slate-800";

/** Picks the job a view shows; keeps the URL in sync. */
export function JobPicker() {
  const { jobs, jobId, view, navigate } = useStore();
  if (!jobs.length) return null;
  return (
    <Select className="w-72" value={jobId ?? ""} onChange={(e) => navigate(view, Number(e.target.value))}>
      {[...jobs].reverse().map((job) => (
        <option key={job.id} value={job.id}>
          #{job.id} · {job.name} ({job.status.state})
        </option>
      ))}
    </Select>
  );
}
