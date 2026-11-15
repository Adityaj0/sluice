// Turns engine stage names such as `window(tumbling 60000ms, sum(amount))`
// into a kind, a readable title and a short detail line.

export type StageKind =
  | "source"
  | "decode"
  | "scan"
  | "filter"
  | "map"
  | "key_by"
  | "window"
  | "project"
  | "sink"
  | "late"
  | "union"
  | "split"
  | "join"
  | "other";

export interface StageLabel {
  kind: StageKind;
  title: string;
  detail: string;
}

const TITLES: Record<string, [StageKind, string]> = {
  source: ["source", "Source"],
  decode: ["decode", "Decode JSON"],
  scan: ["scan", "Scan"],
  filter: ["filter", "Filter"],
  filter_map: ["filter", "Filter"],
  map: ["map", "Map"],
  key_by: ["key_by", "Key by"],
  window: ["window", "Window"],
  project: ["project", "Project"],
  sink: ["sink", "Sink"],
  collect: ["sink", "Sink"],
  print: ["sink", "Sink"],
  "late-events": ["late", "Late events"],
  "late-split": ["split", "Split late events"],
  union: ["union", "Union"],
  split: ["split", "Split"],
  join: ["join", "Join"],
};

/** `60000ms` → `1m`, `1500ms` → `1.5s`. */
export function humanizeMs(ms: number): string {
  if (ms >= 3_600_000 && ms % 3_600_000 === 0) return `${ms / 3_600_000}h`;
  if (ms >= 60_000 && ms % 60_000 === 0) return `${ms / 60_000}m`;
  if (ms >= 1000) return `${+(ms / 1000).toFixed(2)}s`;
  return `${ms}ms`;
}

export function parseStage(name: string): StageLabel {
  const match = name.match(/^([\w-]+)(?:\(([\s\S]*)\))?$/);
  const head = match?.[1] ?? name;
  let detail = (match?.[2] ?? "").trim();
  const [kind, title] = TITLES[head] ?? ["other", head];

  if (kind === "source") detail = detail.replace(/^grpc:/, "stream ");
  if (kind === "decode") detail = "";
  if (kind === "sink" && detail === "buffer") detail = "result buffer";
  detail = detail.replace(/(\d+)ms\b/g, (_, ms: string) => humanizeMs(Number(ms)));
  detail = detail.replace(/^(tumbling|sliding|session gap) ([^,]+), /, "$1 $2 · ");
  return { kind, title, detail };
}
