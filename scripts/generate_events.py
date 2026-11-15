#!/usr/bin/env python3
"""Synthetic purchase-event generator for Sluice.

Sends JSON purchase events ({"user_id", "amount", "item"}) to the gRPC
ingest service. Event times walk forward from `--span-minutes` ago to now,
with up to `--max-disorder-ms` of out-of-order jitter, so tumbling windows
fill and fire as the watermark advances.

    pip install grpcio grpcio-tools
    python scripts/generate_events.py --submit-job examples/purchases_job.json

Chaos demo: kill the window stage halfway through, then check that the
output is still exactly-once (no window missing, duplicated or wrong):

    python scripts/generate_events.py --submit-job examples/purchases_job.json \\
        --rate 3000 --kill-stage window --kill-at 5000 --verify
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path

PROTO_DIR = Path(__file__).resolve().parent.parent / "proto"
ITEMS = ["coffee", "book", "headphones", "groceries", "shoes", "game", "plant"]


def load_grpc():
    try:
        import grpc
    except ImportError:
        sys.exit("grpcio is not installed: pip install grpcio grpcio-tools")
    # protos_and_services compiles the .proto at import time (needs grpcio-tools)
    # and resolves it against sys.path.
    sys.path.insert(0, str(PROTO_DIR))
    try:
        protos, services = grpc.protos_and_services("ingest.proto")
    except Exception as exc:  # grpc raises plain exceptions here
        sys.exit(f"could not load proto/ingest.proto ({exc}); is grpcio-tools installed?")
    return grpc, protos, services


def http_json(method: str, url: str, body: dict | None = None) -> dict | list:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.HTTPError as err:
        sys.exit(f"{method} {url} failed with {err.code}: {err.read().decode()}")
    except urllib.error.URLError as err:
        sys.exit(f"{method} {url} failed: {err.reason}; is the engine running?")


def window_spec(job: dict) -> tuple[int, str, int] | None:
    """(window size, aggregate fn, allowed disorder) for jobs --verify understands."""
    windows = [op for op in job.get("operators", []) if op.get("type") == "window"]
    if len(windows) != 1:
        return None
    window, aggregate = windows[0]["window"], windows[0]["aggregate"]
    if window.get("kind") != "tumbling" or aggregate.get("fn") not in ("count", "sum"):
        return None
    if aggregate["fn"] == "sum" and aggregate.get("field") != "amount":
        return None
    keys = [op["field"] for op in job["operators"] if op.get("type") == "key_by"]
    if keys != ["user_id"]:
        return None
    return window["size_ms"], aggregate["fn"], job["source"].get("max_out_of_orderness_ms", 0)


def kill_stage(api: str, job_id: int, name: str) -> None:
    details = http_json("GET", f"{api}/jobs/{job_id}")
    for index, stage in enumerate(details["stages"]):
        if name in stage["uid"]:
            http_json("POST", f"{api}/jobs/{job_id}/stages/{index}/kill")
            print(f"\n  ** killed stage {stage['uid']} **")
            return
    sys.exit(f"no stage matching '{name}' in job {job_id}")


def verify(api: str, job_id: int, spec: tuple[int, str, int], sent: list[tuple[int, str, float]]) -> bool:
    size, fn, disorder = spec
    max_time = max(t for t, _, _ in sent)
    watermark = max_time - disorder - 1
    expected: dict[tuple[int, str], float] = defaultdict(float)
    for t, user, amount in sent:
        start = t - t % size
        if start + size - 1 <= watermark and amount > 0:  # windows the watermark has closed
            expected[(start, user)] += amount if fn == "sum" else 1

    # Checkpointed jobs publish output when the covering checkpoint completes.
    deadline = time.time() + 20
    while True:
        output = http_json("GET", f"{api}/jobs/{job_id}/output?limit=1000000")
        if len(output) >= len(expected) or time.time() > deadline:
            break
        time.sleep(0.25)

    got: dict[tuple[int, str], float] = {}
    duplicates = 0
    for record in output:
        value = record["value"]
        key = (value["window_start"], value["key"])
        if key in got:
            duplicates += 1
        got[key] = value[fn]
    missing = [k for k in expected if k not in got]
    wrong = [k for k in expected if k in got and abs(got[k] - expected[k]) > 1e-6]
    unexpected = [k for k in got if k not in expected]

    ok = not (missing or wrong or duplicates or unexpected)
    print(f"\nexactly-once check over {len(expected)} closed windows: {'PASS' if ok else 'FAIL'}")
    print(f"  missing={len(missing)} duplicates={duplicates} wrong={len(wrong)} unexpected={len(unexpected)}")
    for label, keys in (("missing", missing), ("wrong", wrong), ("unexpected", unexpected)):
        for k in keys[:3]:
            print(f"  {label}: window {k[0]} key {k[1]}: expected {expected.get(k)} got {got.get(k)}")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--grpc", default="localhost:50051", help="gRPC ingest address")
    parser.add_argument("--api", default="http://localhost:8080", help="REST API base URL")
    parser.add_argument("--stream", default="purchases")
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--users", type=int, default=25)
    parser.add_argument("--span-minutes", type=float, default=30, help="event-time range covered by the run")
    parser.add_argument("--max-disorder-ms", type=int, default=1_500, help="max out-of-order jitter")
    parser.add_argument("--rate", type=float, default=0, help="events/sec (0 = as fast as possible)")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--submit-job", type=Path, metavar="JOB.json", help="submit this job definition first")
    parser.add_argument("--kill-stage", metavar="NAME", help="kill the stage whose uid contains NAME (chaos test)")
    parser.add_argument("--kill-at", type=int, default=None, metavar="N", help="kill after N events (default: half)")
    parser.add_argument("--verify", action="store_true", help="check the job's output is exactly-once")
    args = parser.parse_args()

    if (args.kill_stage or args.verify) and not args.submit_job:
        sys.exit("--kill-stage and --verify need --submit-job")

    rng = random.Random(args.seed)
    grpc, protos, services = load_grpc()

    job_id, spec = None, None
    if args.submit_job:
        definition = json.loads(args.submit_job.read_text())
        definition["source"]["stream"] = args.stream
        spec = window_spec(definition)
        if args.verify and spec is None:
            sys.exit("--verify supports jobs with one tumbling count/sum(amount) window keyed by user_id")
        if spec and args.max_disorder_ms > spec[2]:
            print(f"note: --max-disorder-ms lowered to the job's {spec[2]} ms so no event arrives late")
            args.max_disorder_ms = spec[2]
        job = http_json("POST", f"{args.api}/jobs", definition)
        job_id = job["id"]
        print(f"submitted job {job_id} ({job['name']})")

    channel = grpc.insecure_channel(args.grpc)
    stub = services.IngestStub(channel)

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - int(args.span_minutes * 60_000)
    step = (now_ms - start_ms) / max(args.events, 1)
    kill_at = args.kill_at if args.kill_at is not None else args.events // 2
    sent: list[tuple[int, str, float]] = []

    began = time.perf_counter()
    for i in range(args.events):
        if args.kill_stage and i == kill_at:
            kill_stage(args.api, job_id, args.kill_stage)
        user = f"user-{rng.randrange(args.users):03d}"
        amount = round(rng.uniform(1, 200), 2)
        event = {"user_id": user, "amount": amount, "item": rng.choice(ITEMS)}
        event_time = max(int(start_ms + i * step) - rng.randint(0, args.max_disorder_ms), 1)
        try:
            stub.IngestEvent(
                protos.EventRequest(
                    stream=args.stream, key=user.encode(), value=json.dumps(event).encode(), event_time=event_time
                )
            )
        except grpc.RpcError as err:
            sys.exit(f"ingest failed after {i} events: {err.code().name}: {err.details()}")
        sent.append((event_time, user, amount))
        if args.rate > 0:
            # Pace against the schedule rather than sleeping a fixed amount.
            delay = began + (i + 1) / args.rate - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
        if (i + 1) % 1_000 == 0:
            print(f"  sent {i + 1}/{args.events}", end="\r", flush=True)

    elapsed = time.perf_counter() - began
    print(f"sent {args.events} events to stream '{args.stream}' in {elapsed:.2f}s ({args.events / elapsed:,.0f}/s)")

    if job_id is None:
        return
    ok = verify(args.api, job_id, spec, sent) if args.verify else True
    if not args.verify:
        time.sleep(0.5)  # let the periodic watermark fire the completed windows

    details = http_json("GET", f"{args.api}/jobs/{job_id}")
    checkpoints = http_json("GET", f"{args.api}/jobs/{job_id}/checkpoints")
    print(f"\njob {job_id}: {details['status']['state']}, restarts={details['restarts']}, "
          f"checkpoints completed={checkpoints['completed']} failed={checkpoints['failed']}, "
          f"restored_from={checkpoints['restored_from']}")
    for failure in details["failures"]:
        print(f"  failure: {failure}")
    print("operators (current attempt):")
    for op in details["operators"]:
        print(f"  {op['name']:<40} in={op['records_in']:<7} out={op['records_out']:<7} late={op['late_records']}")
    output = http_json("GET", f"{args.api}/jobs/{job_id}/output?limit=3")
    print("latest window results:")
    for record in output:
        print(f"  {json.dumps(record['value'])}")
    print(f"\nmore: curl '{args.api}/jobs/{job_id}/output?limit=100'   curl '{args.api}/jobs/{job_id}/checkpoints'")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
