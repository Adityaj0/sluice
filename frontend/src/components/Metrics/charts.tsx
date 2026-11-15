// Shared chart pieces. Charts stay quiet: one or two series, blue then gray,
// recessive grid and axes, and text always in neutral ink (never the series color).

import { Area, Bar, BarChart, CartesianGrid, ComposedChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { clock, compact } from "../../lib/format.ts";

export const SERIES = { primary: "#2272b4", secondary: "#8f9ca9" } as const;
const GRID = "#eff1f4";
const AXIS = "#8f9ca9";

export interface Series {
  key: string;
  name: string;
  color: string;
  /** Fill under the line (single-series charts only). */
  area?: boolean;
  dashed?: boolean;
}

type TooltipProps = {
  active?: boolean;
  payload?: readonly { dataKey?: unknown; value?: unknown }[];
  label?: unknown;
};

function ChartTooltip({
  active,
  payload,
  label,
  series,
  format,
  formatLabel,
}: TooltipProps & { series: Series[]; format: (v: number) => string; formatLabel: (label: unknown) => string }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="min-w-44 rounded-md border border-slate-200 bg-white px-3 py-2 text-xs shadow-md">
      <div className="mb-1.5 text-slate-500">{formatLabel(label)}</div>
      {series.map((s) => {
        const value = payload.find((p) => p.dataKey === s.key)?.value;
        return (
          <div key={s.key} className="flex items-center gap-2 py-0.5">
            <span className="h-0.5 w-3 shrink-0 rounded-full" style={{ background: s.color }} />
            <span className="text-slate-700">{s.name}</span>
            <span className="ml-auto pl-4 font-semibold text-slate-900 tabular-nums">{typeof value === "number" ? format(value) : "–"}</span>
          </div>
        );
      })}
    </div>
  );
}

/** A legend for card headers: swatch plus neutral text. */
export function SeriesLegend({ series }: { series: Series[] }) {
  return (
    <div className="flex items-center gap-4 text-xs text-slate-600">
      {series.map((s) => (
        <span key={s.key} className="flex items-center gap-1.5">
          <span className="h-0.5 w-3.5 rounded-full" style={{ background: s.color }} />
          {s.name}
        </span>
      ))}
    </div>
  );
}

export function TimeSeriesChart({
  rows,
  series,
  format = compact,
  domain,
}: {
  rows: Record<string, number | null>[];
  series: Series[];
  format?: (v: number) => string;
  domain?: [number, number];
}) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={rows} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
        <defs>
          {series.map((s) => (
            <linearGradient key={s.key} id={`fill-${s.key}`} x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor={s.color} stopOpacity={0.14} />
              <stop offset="100%" stopColor={s.color} stopOpacity={0} />
            </linearGradient>
          ))}
        </defs>
        <CartesianGrid stroke={GRID} vertical={false} />
        <XAxis dataKey="ts" type="number" domain={["dataMin", "dataMax"]} tickFormatter={(t) => clock(t)} stroke={AXIS} fontSize={11} tickLine={false} axisLine={false} minTickGap={60} />
        <YAxis stroke={AXIS} fontSize={11} width={52} tickFormatter={format} domain={domain} tickLine={false} axisLine={false} />
        <Tooltip
          cursor={{ stroke: "#cbd2d9" }}
          content={(p) => <ChartTooltip {...p} series={series} format={format} formatLabel={(t) => clock(Number(t), true)} />}
        />
        {series.map((s) =>
          s.area ? (
            <Area key={s.key} dataKey={s.key} type="monotone" stroke={s.color} strokeWidth={2} fill={`url(#fill-${s.key})`} isAnimationActive={false} connectNulls />
          ) : (
            <Line key={s.key} dataKey={s.key} type="monotone" stroke={s.color} strokeWidth={2} strokeDasharray={s.dashed ? "4 3" : undefined} dot={false} isAnimationActive={false} connectNulls />
          ),
        )}
      </ComposedChart>
    </ResponsiveContainer>
  );
}

export function BarSeriesChart({
  rows,
  xKey,
  series,
  format = compact,
  formatLabel,
}: {
  rows: Record<string, number>[];
  xKey: string;
  series: Series;
  format?: (v: number) => string;
  formatLabel: (label: unknown) => string;
}) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={rows} margin={{ top: 8, right: 12, bottom: 0, left: 0 }} barCategoryGap={2}>
        <CartesianGrid stroke={GRID} vertical={false} />
        <XAxis dataKey={xKey} stroke={AXIS} fontSize={11} tickLine={false} axisLine={false} minTickGap={24} />
        <YAxis stroke={AXIS} fontSize={11} width={52} tickFormatter={format} tickLine={false} axisLine={false} />
        <Tooltip cursor={{ fill: "#f7f8fa" }} content={(p) => <ChartTooltip {...p} series={[series]} format={format} formatLabel={formatLabel} />} />
        <Bar dataKey={series.key} fill={series.color} radius={[2, 2, 0, 0]} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  );
}
