import { ChevronRight } from "lucide-react";

import { ago } from "../../lib/format.ts";
import { useStore } from "../../store/pipelineStore.ts";
import type { JobSummary } from "../../types/pipeline.ts";
import { KindBadge, StatusBadge, tableClass, tdClass, thClass } from "../common/ui.tsx";

export function JobsTable({ jobs }: { jobs: JobSummary[] }) {
  const navigate = useStore((s) => s.navigate);
  return (
    <table className={tableClass}>
      <thead>
        <tr>
          <th className={thClass}>Job</th>
          <th className={thClass}>Status</th>
          <th className={thClass}>Reads</th>
          <th className={`${thClass} text-right`}>Checkpoints</th>
          <th className={`${thClass} text-right`}>Restarts</th>
          <th className={thClass}>Started</th>
          <th className={thClass} />
        </tr>
      </thead>
      <tbody>
        {[...jobs].reverse().map((job) => (
          <tr key={job.id} onClick={() => navigate("job", job.id)} className="group cursor-pointer transition-colors hover:bg-slate-50">
            <td className={tdClass}>
              <div className="font-medium whitespace-nowrap text-slate-900">{job.name}</div>
              <div className="mt-1 flex items-center gap-2 text-xs text-slate-400">
                <KindBadge kind={job.kind} />
                Job #{job.id}
              </div>
            </td>
            <td className={tdClass}>
              <StatusBadge status={job.status} />
            </td>
            <td className={`${tdClass} font-mono text-xs text-slate-600`}>{job.stream}</td>
            <td className={`${tdClass} text-right tabular-nums`}>{job.checkpoints_completed.toLocaleString()}</td>
            <td className={`${tdClass} text-right tabular-nums ${job.restarts ? "font-medium text-amber-700" : ""}`}>{job.restarts}</td>
            <td className={`${tdClass} text-slate-500`}>{ago(job.submitted_at)}</td>
            <td className={`${tdClass} w-8 text-slate-300 group-hover:text-slate-500`}>
              <ChevronRight className="size-4" />
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
