import assert from "node:assert/strict";
import { test } from "node:test";

import { humanizeMs, parseStage } from "./stages.ts";

test("stage names become readable labels", () => {
  assert.deepEqual(parseStage("source(grpc:purchases)"), { kind: "source", title: "Source", detail: "stream purchases" });
  assert.deepEqual(parseStage("decode(json)"), { kind: "decode", title: "Decode JSON", detail: "" });
  assert.deepEqual(parseStage("window(tumbling 60000ms, sum(amount))"), {
    kind: "window",
    title: "Window",
    detail: "tumbling 1m · sum(amount)",
  });
  assert.deepEqual(parseStage("window(sliding 1000ms every 500ms, count(*), lateness 2500ms)").detail, "sliding 1s every 500ms · count(*), lateness 2.5s");
  assert.equal(parseStage("scan(purchases where amount > 0 fields [amount, user_id])").detail, "purchases where amount > 0 fields [amount, user_id]");
  assert.deepEqual(parseStage("late-events(json)").kind, "late");
  assert.deepEqual(parseStage("union"), { kind: "union", title: "Union", detail: "" });
  assert.deepEqual(parseStage("sink(buffer)"), { kind: "sink", title: "Sink", detail: "result buffer" });
  assert.equal(parseStage("mystery").kind, "other");
});

test("durations", () => {
  assert.equal(humanizeMs(60_000), "1m");
  assert.equal(humanizeMs(1_500), "1.5s");
  assert.equal(humanizeMs(250), "250ms");
  assert.equal(humanizeMs(7_200_000), "2h");
});
