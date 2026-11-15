// Live job metrics over WebSocket. One socket per job, shared by every
// component that shows it (reference counted), reconnecting with backoff.

import { useEffect } from "react";

import { liveUrl } from "../api.ts";
import { useStore } from "../store/pipelineStore.ts";
import type { LiveJob } from "../store/pipelineStore.ts";
import type { LiveSnapshot } from "../types/pipeline.ts";

interface Connection {
  socket: WebSocket | null;
  refs: number;
  retry: ReturnType<typeof setTimeout> | null;
  delay: number;
}

const connections = new Map<number, Connection>();

function connect(jobId: number, connection: Connection) {
  const socket = new WebSocket(liveUrl(jobId));
  connection.socket = socket;
  socket.onopen = () => {
    connection.delay = 500;
    useStore.getState().setConnected(jobId, true);
  };
  socket.onmessage = (message) => {
    useStore.getState().pushSnapshot(jobId, JSON.parse(message.data as string) as LiveSnapshot);
  };
  socket.onclose = () => {
    useStore.getState().setConnected(jobId, false);
    if (connection.refs > 0) {
      connection.retry = setTimeout(() => connect(jobId, connection), connection.delay);
      connection.delay = Math.min(connection.delay * 2, 5000);
    }
  };
}

/** Subscribes to a job's live snapshots while the component is mounted. */
export function useLiveJob(jobId: number | null): LiveJob | undefined {
  useEffect(() => {
    if (jobId == null) return;
    let connection = connections.get(jobId);
    if (!connection) {
      connection = { socket: null, refs: 0, retry: null, delay: 500 };
      connections.set(jobId, connection);
    }
    connection.refs += 1;
    if (!connection.socket) connect(jobId, connection);
    const current = connection;
    return () => {
      current.refs -= 1;
      if (current.refs === 0) {
        if (current.retry) clearTimeout(current.retry);
        current.socket?.close();
        current.socket = null;
        connections.delete(jobId);
      }
    };
  }, [jobId]);
  return useStore((state) => (jobId == null ? undefined : state.live[jobId]));
}
