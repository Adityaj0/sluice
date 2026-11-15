#!/usr/bin/env bash
# One-command local demo (no Docker): builds the UI and engine, starts the
# engine, deploys three example jobs and streams synthetic purchases into
# them. Open http://localhost:8080. Ctrl-C stops everything.
set -euo pipefail
cd "$(dirname "$0")/.."

RATE="${RATE:-2000}"
export SLUICE_DATA_DIR="${SLUICE_DATA_DIR:-$(mktemp -d -t sluice-demo)}"

if [ ! -f frontend/dist/index.html ]; then
  echo "==> building the web UI"
  (cd frontend && npm install --no-audit --no-fund && npm run build)
fi
echo "==> building the engine (release)"
cargo build --release --bin sluice --bin sluice-loadgen

echo "==> starting the engine (checkpoints in $SLUICE_DATA_DIR)"
./target/release/sluice &
ENGINE=$!
trap 'kill $ENGINE 2>/dev/null; wait 2>/dev/null' EXIT INT TERM

echo "==> waiting for the engine"
for _ in $(seq 1 50); do curl -sf localhost:8080/jobs >/dev/null && break; sleep 0.2; done
echo "==> deploying example jobs and a $RATE events/s generator"
for job in examples/purchases_job.json examples/sql_job.json examples/sessions_job.json; do
  curl -sf -X POST localhost:8080/jobs -H 'content-type: application/json' --data @"$job" >/dev/null
done
curl -sf -X POST localhost:8080/generators -H 'content-type: application/json' \
  --data "{\"stream\":\"purchases\",\"rate\":$RATE,\"users\":40}" >/dev/null

cat <<MSG

  Sluice is running:  http://localhost:8080
    Data sources   start/stop generators or send your own JSON events
    Builder / SQL  design a pipeline or write a query, then deploy it
    Jobs           watch the dataflow; kill a stage to test recovery
    Debugger       step through checkpoints and inspect window state

  Ctrl-C to stop.
MSG
wait $ENGINE
