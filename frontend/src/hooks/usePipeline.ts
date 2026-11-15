// Polling of engine state, and job submission.

import { useCallback, useEffect, useState } from "react";

import { api } from "../api.ts";
import { useStore } from "../store/pipelineStore.ts";
import type { JobDefinition } from "../types/pipeline.ts";

/** Keeps jobs, streams and generators fresh while mounted. */
export function usePolling(intervalMs = 2000) {
  const refresh = useStore((s) => s.refresh);
  useEffect(() => {
    void refresh();
    const timer = setInterval(() => void refresh(), intervalMs);
    return () => clearInterval(timer);
  }, [refresh, intervalMs]);
}

/** Submits a job definition, then opens its live view. */
export function useSubmitJob() {
  const navigate = useStore((s) => s.navigate);
  const refresh = useStore((s) => s.refresh);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = useCallback(
    async (definition: JobDefinition) => {
      setSubmitting(true);
      setError(null);
      try {
        const job = await api.submit(definition);
        await refresh();
        navigate("job", job.id);
      } catch (e) {
        setError((e as Error).message);
      } finally {
        setSubmitting(false);
      }
    },
    [navigate, refresh],
  );
  return { submit, submitting, error };
}
