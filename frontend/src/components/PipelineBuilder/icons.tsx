import { ArrowDownToLine, Columns3, Combine, Filter, Inbox, KeyRound, Timer } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import type { NodeKind } from "../../types/pipeline.ts";
import { cx } from "../common/ui.tsx";

export const OPERATOR_ICONS: Record<NodeKind, LucideIcon> = {
  source: ArrowDownToLine,
  filter: Filter,
  map: Columns3,
  key_by: KeyRound,
  window: Timer,
  sink: Inbox,
  join: Combine,
};

export const accents: Record<NodeKind, string> = {
  source: "text-slate-600",
  filter: "text-slate-600",
  map: "text-slate-600",
  key_by: "text-slate-600",
  window: "text-slate-600",
  sink: "text-slate-600",
  join: "text-slate-400",
};

export function OperatorIcon({ kind, size = "md" }: { kind: NodeKind; size?: "md" | "lg" }) {
  const Icon = OPERATOR_ICONS[kind];
  return (
    <span className={cx("grid shrink-0 place-items-center rounded-md border border-slate-200 bg-slate-50", size === "lg" ? "size-8" : "size-7", accents[kind])}>
      <Icon className="size-4" strokeWidth={1.75} />
    </span>
  );
}
