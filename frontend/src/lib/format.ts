export function compact(n: number): string {
  if (!Number.isFinite(n)) return "–";
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (abs >= 1e4) return `${(n / 1e3).toFixed(1)}k`;
  if (abs >= 100 || Number.isInteger(n)) return Math.round(n).toLocaleString();
  return n.toFixed(1);
}

export function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}

export function duration(ms: number | null | undefined): string {
  if (ms == null) return "–";
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
  return `${(ms / 3_600_000).toFixed(1)} h`;
}

/** Wall-clock or event time as HH:MM:SS.mmm (local time). */
export function clock(ms: number | null | undefined, millis = false): string {
  if (ms == null) return "–";
  if (ms >= Number.MAX_SAFE_INTEGER) return "end of stream";
  const d = new Date(ms);
  const base = d.toLocaleTimeString([], { hour12: false });
  return millis ? `${base}.${String(d.getMilliseconds()).padStart(3, "0")}` : base;
}

export function percent(ratio: number): string {
  return `${Math.round(ratio * 100)}%`;
}

export function json(value: unknown): string {
  return typeof value === "string" ? value : JSON.stringify(value);
}

/** "12s ago", "5m ago", "3h ago". */
export function ago(ms: number, now = Date.now()): string {
  const seconds = Math.max(0, Math.round((now - ms) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86_400)}d ago`;
}

/** Events per second, compactly: 2.1k/s. */
export function rate(perSec: number): string {
  return `${compact(perSec)}/s`;
}
