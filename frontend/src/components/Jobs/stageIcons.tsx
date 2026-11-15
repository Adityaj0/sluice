import {
  AlarmClock,
  ArrowDownToLine,
  Box,
  Braces,
  Columns3,
  Combine,
  Filter,
  Inbox,
  KeyRound,
  Merge,
  ScanSearch,
  Split,
  Timer,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

import type { StageKind } from "../../lib/stages.ts";

export const STAGE_ICONS: Record<StageKind, LucideIcon> = {
  source: ArrowDownToLine,
  decode: Braces,
  scan: ScanSearch,
  filter: Filter,
  map: Columns3,
  key_by: KeyRound,
  window: Timer,
  project: Columns3,
  sink: Inbox,
  late: AlarmClock,
  union: Merge,
  split: Split,
  join: Combine,
  other: Box,
};
