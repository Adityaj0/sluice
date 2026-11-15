// REST client for the Sluice engine. In development Vite proxies these
// paths to the engine; in production the engine serves the UI itself.

import type {
  CheckpointsView,
  GeneratorInfo,
  GeneratorSpec,
  StreamInfo,
  ExplainResult,
  InspectedCheckpoint,
  JobDefinition,
  JobDetails,
  JobSummary,
  OutputRecord,
  SqlError,
} from "./types/pipeline.ts";

export class ApiError extends Error {
  readonly status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      method,
      headers: body === undefined ? undefined : { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch {
    throw new ApiError("Cannot reach the Sluice engine. Is it running on :8080?", 0);
  }
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) throw new ApiError(data?.error ?? `${method} ${path} failed (${response.status})`, response.status);
  return data as T;
}

export const api = {
  jobs: () => request<JobSummary[]>("GET", "/jobs"),
  job: (id: number) => request<JobDetails>("GET", `/jobs/${id}`),
  submit: (definition: JobDefinition) => request<JobSummary>("POST", "/jobs", definition),
  cancel: (id: number) => request<JobSummary>("DELETE", `/jobs/${id}`),
  killStage: (id: number, stage: number) => request<JobSummary>("POST", `/jobs/${id}/stages/${stage}/kill`),
  output: (id: number, limit = 50) => request<OutputRecord[]>("GET", `/jobs/${id}/output?limit=${limit}`),
  checkpoints: (id: number) => request<CheckpointsView>("GET", `/jobs/${id}/checkpoints`),
  streams: () => request<StreamInfo[]>("GET", "/streams"),
  sendEvents: (stream: string, records: unknown) =>
    request<{ stream: string; first_offset: number; count: number }>("POST", `/streams/${encodeURIComponent(stream)}/events`, records),
  generators: () => request<GeneratorInfo[]>("GET", "/generators"),
  startGenerator: (spec: GeneratorSpec) => request<GeneratorInfo>("POST", "/generators", spec),
  stopGenerator: (id: number) => request<GeneratorInfo>("DELETE", `/generators/${id}`),
  checkpointState: (id: number, checkpoint: number, limit = 500) =>
    request<InspectedCheckpoint>("GET", `/jobs/${id}/checkpoints/${checkpoint}/state?limit=${limit}`),

  /** Compiles a query; errors come back as values (with positions). */
  async explain(query: string): Promise<{ ok: true; result: ExplainResult } | { ok: false; error: SqlError }> {
    try {
      const response = await fetch("/sql/explain", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query }),
      });
      const data = await response.json();
      if (response.ok) return { ok: true, result: data as ExplainResult };
      return { ok: false, error: (data.details as SqlError) ?? { kind: "plan", message: data.error } };
    } catch {
      return { ok: false, error: { kind: "plan", message: "Cannot reach the Sluice engine." } };
    }
  },
};

export function liveUrl(jobId: number): string {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${location.host}/jobs/${jobId}/live`;
}
